"""
common.py — Outils partagés : config, RNG reproductible, écriture atomique/idempotente (CSV/JSONL/XML/PDF),
formats tunisiens (téléphone, CIN, RIB, matricule fiscal, EAN-13), injection de défauts, journal des anomalies,
exécution parallèle (contexte spawn : jamais fork, Polars se bloque).
"""
import datetime as dt
import gzip
import json
import multiprocessing as mp
import os
import time
import unicodedata
import zlib
from pathlib import Path

import numpy as np
import polars as pl
import yaml

import referentiels as R

AUJOURDHUI_FMT = "%Y-%m-%d"


# ────────────────────────── Configuration / portée ──────────────────────────
def load_config(path="config.yaml", mode=None, sortie=None, workers=None):
    p = Path(path).resolve()
    with open(p, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    cfg["_dir"] = str(p.parent)
    if mode:
        cfg["mode"] = mode
    if workers is not None:
        cfg["max_workers"] = workers
    base = Path(sortie) if sortie else (p.parent / cfg["sortie"])
    cfg["_raw"] = str((base / ("raw" if cfg["mode"] == "full" else "raw_echantillon")).resolve())
    if cfg["mode"] == "sample" and cfg.get("echantillon", {}).get("evolutions"):
        cfg["evolutions"] = dict(cfg["echantillon"]["evolutions"])
    return cfg


def to_date(x):
    return x if isinstance(x, dt.date) else dt.date.fromisoformat(str(x))


def scope(cfg):
    """Dates et magasins effectivement générés selon le mode."""
    if cfg["mode"] == "sample":
        e = cfg["echantillon"]
        return dict(debut=to_date(e["debut"]), fin=to_date(e["fin"]), magasins=list(e["magasins"]), sample=True,
                    facteur=float(e.get("facteur_clients", 0.1)))
    return dict(debut=to_date(cfg["periode"]["debut"]), fin=to_date(cfg["periode"]["fin"]),
                magasins=[f"magasin_{n:02d}" for n in range(1, 21)], sample=False, facteur=1.0)


def periode_complete(cfg):
    return to_date(cfg["periode"]["debut"]), to_date(cfg["periode"]["fin"])


def jours(d0, d1):
    d = d0
    while d <= d1:
        yield d
        d += dt.timedelta(days=1)


def mois_de(d0, d1):
    """Liste des (année, mois) couvrant [d0, d1]."""
    out, y, m = [], d0.year, d0.month
    while (y, m) <= (d1.year, d1.month):
        out.append((y, m))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def raw(cfg, *parts):
    return os.path.join(cfg["_raw"], *parts)


def meta(cfg, *parts):
    return os.path.join(cfg["_raw"], "_meta", *parts)


def magasins_cfg(cfg):
    """[{id, num, format, gouv, deleg, ...}] avec paramètres du format."""
    out = []
    for num, fmt, gv, dl in R.MAGASINS:
        c = dict(cfg["magasins"][fmt])
        c.update(id=f"magasin_{num:02d}", num=num, format=fmt, gouv=gv, deleg=dl, tourisme=gv in R.GOUV_TOURISTIQUES)
        out.append(c)
    return out


# ────────────────────────── RNG ──────────────────────────
def rng_for(cfg, *tags):
    """RNG déterministe par tâche : indépendant du nombre de workers (reproductible)."""
    ints = [int(cfg["graine"])] + [zlib.crc32(str(t).encode()) for t in tags]
    return np.random.default_rng(np.random.SeedSequence(ints))


# ────────────────────────── Écriture ──────────────────────────
def _final(path, cfg, compressible=True):
    return path + ".gz" if (cfg.get("compression") == "gz" and compressible) else path


def deja_fait(path, cfg, compressible=True):
    """Idempotence : un fichier déjà généré est ignoré."""
    return os.path.exists(_final(path, cfg, compressible))


def ecrire_octets(path, data, cfg, compressible=True):
    """Écriture atomique (tmp -> rename). Retourne la taille écrite sur disque."""
    fp = _final(path, cfg, compressible)
    os.makedirs(os.path.dirname(fp), exist_ok=True)
    tmp = fp + ".tmp"
    if fp.endswith(".gz"):
        with gzip.open(tmp, "wb", compresslevel=4) as f:
            f.write(data)
    else:
        with open(tmp, "wb") as f:
            f.write(data)
    os.replace(tmp, fp)
    return os.path.getsize(fp)


# ────────────────────────── Journal / statistiques par tâche ──────────────────────────
class Sink:
    """Collecte stats + anomalies d'une tâche ; persistée dans _meta/_parts (reprise sans perte du journal)."""

    def __init__(self, cfg, source, taskid):
        self.cfg, self.source = cfg, source
        self.path = meta(cfg, "_parts", source, f"{taskid}.json")
        self.d = {}
        if os.path.exists(self.path):
            with open(self.path, encoding="utf-8") as f:
                self.d = json.load(f)

    def put(self, rel, groupe, rows, nbytes, anomalies=()):
        self.d[rel] = {"g": groupe, "rows": int(rows), "bytes": int(nbytes), "anom": [list(a) for a in anomalies]}

    def close(self):
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.d, f, ensure_ascii=False)
        os.replace(tmp, self.path)


def sortie_csv(cfg, sink, rel, groupe, df, spec=None, rng=None, post=None, an_extra=()):
    """Injecte les défauts (si spec), applique le formatage final (post) puis écrit le CSV. Retourne la taille."""
    path = raw(cfg, rel)
    an = []
    if spec is not None:
        df, an = injecter_defauts(df, spec, rng, cfg["defauts"], rel)
    an = list(an) + [(rel,) + tuple(a) for a in an_extra]
    if post is not None:
        df = post(df)
    data = df.write_csv().encode("utf-8")
    n = ecrire_octets(path, data, cfg)
    sink.put(rel, groupe, df.height, n, an)
    return n


def sortie_texte(cfg, sink, rel, groupe, texte, rows, compressible=True):
    data = texte if isinstance(texte, bytes) else texte.encode("utf-8")
    n = ecrire_octets(raw(cfg, rel), data, cfg, compressible)
    sink.put(rel, groupe, rows, n)
    return n


# ────────────────────────── Formats tunisiens ──────────────────────────
def _zf(a, n):
    if np.asarray(a).size == 0:
        return np.array([], dtype="<U%d" % n)
    return np.char.zfill(np.asarray(a).astype(str), n)


def tel_chiffres(rng, n):
    """8 chiffres : mobiles 2/4/5/9, fixes 7."""
    p = rng.choice(np.array(["2", "4", "5", "9", "7"]), size=n, p=[0.30, 0.18, 0.22, 0.22, 0.08])
    return np.char.add(p, _zf(rng.integers(0, 10 ** 7, n), 7))


def tel_formats(d8, rng, taux_varie):
    """Format canonique +216XXXXXXXX ; (taux_varie) autres variantes injectées."""
    d8 = np.asarray(d8).astype(str)
    a, b, c = (np.char.add("", np.array([s[:2] for s in d8])), np.array([s[2:5] for s in d8]), np.array([s[5:] for s in d8]))
    v = {
        0: np.char.add("+216", d8),
        1: np.char.add(np.char.add(np.char.add(np.char.add("+216 ", a), " "), np.char.add(b, " ")), c),
        2: np.char.add("00216 ", d8),
        3: np.char.add(np.char.add(np.char.add(a, " "), np.char.add(b, " ")), c),
        4: np.char.add(np.char.add(np.char.add(a, "."), np.char.add(b, ".")), c),
        5: d8,
    }
    ch = np.zeros(len(d8), dtype=int)
    var = rng.random(len(d8)) < taux_varie
    ch[var] = rng.integers(1, 6, var.sum())
    out = v[0].copy()
    for k in range(1, 6):
        out = np.where(ch == k, v[k], out)
    return out, int(var.sum())


def cin_8(rng, n):
    return _zf(rng.integers(0, 2, n) * 10 ** 7 + rng.integers(0, 10 ** 7, n), 8)


def rib_20(rng, n):
    """RIB : banque(2)+agence(3)+compte(13)+clé(2) ; clé telle que int(RIB) % 97 == 0."""
    out = []
    for _ in range(n):
        base = f"{R.BANQUES[rng.integers(0, len(R.BANQUES))]}{rng.integers(0, 1000):03d}{rng.integers(0, 10 ** 13):013d}"
        cle = 97 - (int(base) * 100 % 97)
        out.append(f"{base}{cle:02d}")
    return np.array(out)


def matricule_fiscal(rng, n):
    L = np.array(list("ABCDEFGHJKLMNPQRSTVWXYZ"))
    c2, c3 = np.array(list("ABPN")), np.array(list("MPCE"))
    sfx = np.array(["000", "000", "000", "001"])
    nums = _zf(rng.integers(0, 10 ** 7, n), 7)
    return np.char.add(np.char.add(np.char.add(np.char.add(nums, rng.choice(L, n)), np.char.add("/", rng.choice(c2, n))),
                                   np.char.add("/", rng.choice(c3, n))), np.char.add("/", rng.choice(sfx, n)))


def ean13_619(rng, n):
    out = []
    for _ in range(n):
        base = "619" + f"{rng.integers(0, 10 ** 9):09d}"
        s = sum(int(c) * (3 if i % 2 else 1) for i, c in enumerate(base))
        out.append(base + str((10 - s % 10) % 10))
    return np.array(out)


def ean_valide(e):
    e = str(e)
    if len(e) != 13 or not e.isdigit() or not e.startswith("619"):
        return False
    s = sum(int(c) * (3 if i % 2 else 1) for i, c in enumerate(e[:12]))
    return (10 - s % 10) % 10 == int(e[12])


def rib_valide(r):
    r = str(r)
    return len(r) == 20 and r.isdigit() and int(r) % 97 == 0


def sans_accents(s):
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")


def tnd(col):
    """Millimes (Int64) -> chaîne 'X.YYY' (3 décimales, TND)."""
    c = pl.col(col)
    return (c // 1000).cast(pl.Utf8) + pl.lit(".") + (c % 1000).cast(pl.Utf8).str.zfill(3)


def tnd_v(a):
    """numpy int millimes -> numpy str 'X.YYY'."""
    a = np.asarray(a, dtype=np.int64)
    return np.char.add(np.char.add((a // 1000).astype(str), "."), _zf(a % 1000, 3))


def arrondi5(x):
    return (np.rint(np.asarray(x, dtype=np.float64) / 5.0) * 5).astype(np.int64)


# ────────────────────────── Injection de défauts ──────────────────────────
def _degrade(s, op):
    if s is None:
        return s
    h = zlib.crc32(s.encode("utf-8"))          # déterministe (hash() est salé par processus)
    if op == 0:
        return sans_accents(s)
    if op == 1:
        return [s.lower(), s.upper(), s.title(), s.swapcase()][h % 4]
    return ("  " + s) if h % 3 == 0 else (s + "  " if h % 3 == 1 else s.replace(" ", "  ", 1))


def injecter_defauts(df, spec, rng, dcfg, fichier):
    """
    spec = {nulls:[col], dates:[col], texte:[col], aberr:{col:[facteurs]}, orphelins:{col: f(rng,n)},
            doublons:bool}.  Retourne (df, [(fichier, type, colonne, nombre)]).
    Les valeurs aberrantes portent sur des colonnes numériques *avant* formatage (millimes / quantités).
    """
    an, n = [], df.height
    if n == 0:
        return df, an
    # 1. aberrants : prix x100/x10, quantité x100 (une cellule sur ~aberrants)
    nab = max(1, len(spec.get("aberr") or {}))          # le taux global est réparti entre les colonnes ciblées
    for col, facs in (spec.get("aberr") or {}).items():
        mk = rng.random(n) < dcfg["aberrants"] / nab
        k = int(mk.sum())
        if k:
            f = rng.choice(np.array(facs), size=k)
            v = df[col].to_numpy().astype(np.int64).copy()
            v[mk] = v[mk] * f
            df = df.with_columns(pl.Series(col, v))
            an.append((fichier, "aberrant", col, k))
    # 2. orphelins
    for col, gen in (spec.get("orphelins") or {}).items():
        mk = (rng.random(n) < dcfg["orphelins"]) & df[col].is_not_null().to_numpy()     # une clé absente (ex. ticket sans carte) n'est pas "orpheline"
        k = int(mk.sum())
        if k:
            vals = gen(rng, k)
            s = df[col].to_list() if df[col].dtype == pl.Utf8 else df[col].to_numpy().copy()
            idx = np.flatnonzero(mk)
            for i, v in zip(idx, vals):
                s[i] = v
            df = df.with_columns(pl.Series(col, s, dtype=df[col].dtype))
            an.append((fichier, "orphelin", col, k))
    # 3. texte : accents / casse / espaces
    for col in spec.get("texte", []):
        mk = rng.random(n) < dcfg["texte"]
        idx = np.flatnonzero(mk & df[col].is_not_null().to_numpy())
        if len(idx):
            s = df[col].to_list()
            ops = rng.integers(0, 3, len(idx))
            for i, o in zip(idx, ops):
                s[i] = _degrade(s[i], int(o))
            df = df.with_columns(pl.Series(col, s, dtype=pl.Utf8))
            an.append((fichier, "texte_degrade", col, len(idx)))
    # 4. formats de dates mixtes : AAAA-MM-JJ[...] -> JJ/MM/AAAA[...]
    for col in spec.get("dates", []):
        mk = rng.random(n) < dcfg["dates_mixtes"]
        k = int(mk.sum())
        if k:
            c = pl.col(col)
            alt = c.str.slice(8, 2) + "/" + c.str.slice(5, 2) + "/" + c.str.slice(0, 4) + c.str.slice(10)
            df = df.with_columns(pl.when(pl.Series(mk)).then(alt).otherwise(c).alias(col))
            an.append((fichier, "date_format_mixte", col, k))
    # 5. nulls
    for col in spec.get("nulls", []):
        mk = rng.random(n) < dcfg["nulls"]
        k = int(mk.sum())
        if k:
            df = df.with_columns(pl.when(pl.Series(mk)).then(None).otherwise(pl.col(col)).alias(col))
            an.append((fichier, "null", col, k))
    # 6. doublons exacts (insérés juste après l'original)
    if spec.get("doublons", True):
        mk = rng.random(n) < dcfg["doublons_exacts"]
        idx = np.flatnonzero(mk)
        if len(idx):
            df = pl.concat([df.with_row_index("_i"), df[idx].with_row_index("_i").with_columns(pl.Series("_i", idx, dtype=pl.UInt32))]) \
                .sort("_i", maintain_order=True).drop("_i")
            an.append((fichier, "doublon_exact", "*", len(idx)))
    return df, an


# ────────────────────────── PDF minimal (sans dépendance) ──────────────────────────
def pdf_texte(lignes, titre="Document", lignes_par_page=48):
    """PDF valide (Helvetica, WinAnsi) à partir d'une liste de lignes de texte."""
    def esc(s):
        s = s.encode("cp1252", "replace").decode("cp1252")
        return s.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")
    pages = [lignes[i:i + lignes_par_page] for i in range(0, max(len(lignes), 1), lignes_par_page)] or [[]]
    objs = []                                    # corps des objets 1..n
    n_pages = len(pages)
    kids = " ".join(f"{3 + 2 * i} 0 R" for i in range(n_pages))
    objs.append(b"<< /Type /Catalog /Pages 2 0 R >>")
    objs.append(f"<< /Type /Pages /Kids [{kids}] /Count {n_pages} >>".encode())
    fonte = 3 + 2 * n_pages
    for i, pg in enumerate(pages):
        flux = ["BT /F1 10 Tf 50 800 Td 13 TL"] + [f"({esc(l)}) Tj T*" for l in pg] + ["ET"]
        flux = "\n".join(flux).encode("cp1252", "replace")
        objs.append(f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 595 842] /Contents {4 + 2 * i} 0 R "
                    f"/Resources << /Font << /F1 {fonte} 0 R >> >> >>".encode())
        objs.append(b"<< /Length %d >>\nstream\n" % len(flux) + flux + b"\nendstream")
    objs.append(b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica /Encoding /WinAnsiEncoding >>")
    out, offs = bytearray(b"%PDF-1.4\n"), []
    for i, o in enumerate(objs, 1):
        offs.append(len(out))
        out += f"{i} 0 obj\n".encode() + o + b"\nendobj\n"
    x = len(out)
    out += f"xref\n0 {len(objs) + 1}\n0000000000 65535 f \n".encode()
    for o in offs:
        out += f"{o:010d} 00000 n \n".encode()
    out += f"trailer\n<< /Size {len(objs) + 1} /Root 1 0 R /Info << /Title ({esc(titre)}) >> >>\nstartxref\n{x}\n%%EOF\n".encode()
    return bytes(out)


# ────────────────────────── Parallélisme (spawn) ──────────────────────────
def run_pool(func, taches, workers, libelle="", verbeux=True):
    """Exécute func(tache) pour chaque tâche. workers<=1 : séquentiel. Contexte spawn (jamais fork)."""
    taches = list(taches)
    if not taches:
        return []
    t0, res = time.time(), []
    if workers <= 1:
        for i, t in enumerate(taches, 1):
            res.append(func(t))
            if verbeux and (i % max(1, len(taches) // 10) == 0 or i == len(taches)):
                print(f"   {libelle}: {i}/{len(taches)} ({time.time() - t0:.0f}s)", flush=True)
        return res
    ctx = mp.get_context("spawn")
    with ctx.Pool(min(workers, len(taches))) as pool:
        for i, r in enumerate(pool.imap_unordered(func, taches, chunksize=1), 1):
            res.append(r)
            if verbeux and (i % max(1, len(taches) // 10) == 0 or i == len(taches)):
                print(f"   {libelle}: {i}/{len(taches)} ({time.time() - t0:.0f}s)", flush=True)
    return res


# ────────────────────────── État partagé (référentiels persistés) ──────────────────────────
_CACHE = {}


def etat_path(cfg, nom):
    return meta(cfg, "_etat", nom)


def charger_etat(cfg, nom):
    """Charge (et met en cache par processus) un fichier d'état .parquet / .npz."""
    p = etat_path(cfg, nom)
    k = os.path.realpath(p)
    if k not in _CACHE:
        _CACHE[k] = pl.read_parquet(p) if nom.endswith(".parquet") else dict(np.load(p))   # dict : un NpzFile relit/décompresse à chaque accès
    return _CACHE[k]
