"""src_clients.py — Clients : 3 sources (CRM CSV, carte de fidélité CSV, comptes web JSONL) avec doublons inter-sources
(même personne, saisies différentes), doublons intra-source (CRM), fichier initial + fichiers mensuels (créations + ~3 % de mises à jour)."""
import datetime as dt
import json
import os

import numpy as np
import polars as pl

import common as C
import referentiels as R

_AB = {"Avenue": "Av.", "Rue": "R.", "Boulevard": "Bd"}


def _poids_gouv():
    g = list(R.POIDS_GOUV)
    p = np.array([R.POIDS_GOUV[x] for x in g], float)
    return g, p / p.sum()


def construire_personnes(cfg, n):
    rng = C.rng_for(cfg, "personnes", n)
    d0, d1 = C.periode_complete(cfg)
    etr = rng.random(n) < 0.05
    fem = rng.random(n) < 0.5
    pn = np.where(etr, rng.choice(R.PRENOMS_ETR, n), np.where(fem, rng.choice(R.PRENOMS_F, n), rng.choice(R.PRENOMS_H, n)))
    nm = np.where(etr, rng.choice(R.NOMS_ETR, n), np.array([x.replace("_", " ") for x in rng.choice(R.NOMS, n)]))
    g, p = _poids_gouv()
    gv = rng.choice(np.array(g), n, p=p)
    dl, cp = [], []
    for x in gv:
        lst = R.GEO[x]
        d, c = lst[int(rng.integers(0, len(lst)))]
        dl.append(d); cp.append(c)
    rue = np.array([f"{int(rng.integers(1, 250))} {R.RUES[int(rng.integers(0, len(R.RUES)))]}" for _ in range(n)])
    em = np.array([f"{C.sans_accents(a).lower().replace(' ', '').replace('-', '')}.{C.sans_accents(b).lower().replace(' ', '').replace(chr(39), '')}"
                   f"{'' if rng.random() < 0.5 else int(rng.integers(1, 99))}@{R.DOMAINES_EMAIL[int(rng.integers(0, len(R.DOMAINES_EMAIL)))]}"
                   for a, b in zip(pn, nm)])
    dn = np.array([(dt.date(1945, 1, 1) + dt.timedelta(days=int(x))).isoformat() for x in rng.integers(0, 23000, n)])
    # date de création : 60 % avant la période, 40 % pendant
    avant = rng.random(n) < 0.6
    off = np.where(avant, -rng.integers(1, 2400, n), rng.integers(0, (d1 - d0).days + 1, n))
    crea = np.array([d0 + dt.timedelta(days=int(o)) for o in off], dtype=object)
    in_crm = rng.random(n) < cfg["volumes"]["part_crm"]
    in_fid = rng.random(n) < cfg["volumes"]["part_fidelite"]
    in_web = rng.random(n) < cfg["volumes"]["part_web"]
    in_crm |= ~(in_crm | in_fid | in_web)
    return dict(n=n, nm=nm, pn=pn, sx=np.where(fem, "F", "M"), gv=gv, dl=np.array(dl), cp=np.array(cp), rue=rue, em=em, dn=dn,
                tel8=C.tel_chiffres(rng, n), cin=C.cin_8(rng, n), crea=crea, in_crm=in_crm, in_fid=in_fid, in_web=in_web)


def _u(a):
    return pl.Series([None if x is None else str(x) for x in a], dtype=pl.Utf8)


def _var_nom(s, rng, p=0.08):
    """Variation de saisie d'un nom : accents perdus / casse."""
    r = rng.random(len(s))
    out = np.array(s, dtype=object)
    for i in np.flatnonzero(r < p):
        out[i] = C.sans_accents(out[i])
    for i in np.flatnonzero((r >= p) & (r < 2 * p)):
        out[i] = out[i].upper()
    return out


def _tirer_maj(rng, P, cle, idx, a, part):
    """Mises à jour : ~part des lignes du fichier = personnes déjà existantes (même identifiant, tel/adresse modifiés)."""
    ex = np.flatnonzero(P[cle] & np.array([c < a for c in P["crea"]]))
    k = int(round(len(idx) * part / (1 - part)))
    return rng.choice(ex, min(k, len(ex)), replace=False) if len(ex) and k else np.array([], dtype=int)


def run(cfg):
    sc = C.scope(cfg)
    n = max(1000, int(cfg["volumes"]["personnes"] * sc["facteur"]))
    P = construire_personnes(cfg, n)
    d0 = sc["debut"].replace(day=1)
    sink = C.Sink(cfg, "clients", "clients")
    crea = P["crea"]
    ordre = np.argsort(np.array([c.toordinal() for c in crea]), kind="stable")      # ordre chronologique
    rngm = C.rng_for(cfg, "ids_clients")
    # identifiants par source (attribués dans l'ordre de création)
    cli_id, fid_id, uid = np.full(n, "", dtype=object), np.full(n, "", dtype=object), np.full(n, "", dtype=object)
    k1 = k2 = 0
    for i in ordre:
        if P["in_crm"][i]: k1 += 1; cli_id[i] = f"C{k1:07d}"
        if P["in_fid"][i]: k2 += 1; fid_id[i] = f"FID{k2:07d}"
        if P["in_web"][i]: uid[i] = "u_" + "".join(rngm.choice(list("0123456789abcdef"), 10))
    mg_adh = rngm.integers(1, 21, n)
    # état pour les ventes : cartes triées par date d'adhésion
    fi = np.flatnonzero(P["in_fid"])
    fi = fi[np.argsort(fid_id[fi])]
    os.makedirs(C.etat_path(cfg, ""), exist_ok=True)
    pl.DataFrame({"fid_id": fid_id[fi].astype(str), "ordre": np.array([crea[i].toordinal() for i in fi])}).write_parquet(C.etat_path(cfg, "fid.parquet"))
    C._CACHE.clear()
    wi = np.flatnonzero(P["in_web"])
    pl.DataFrame({"uid": uid[wi].astype(str), "ordre": np.array([crea[i].toordinal() for i in wi])}).sort("ordre").write_parquet(C.etat_path(cfg, "web.parquet"))
    tvar = cfg["defauts"]["telephones_varies"]
    verite = []

    def fenetre(i_mask, a, b):  # personnes créées dans [a, b[
        return np.flatnonzero(i_mask & np.array([(a <= c < b) for c in crea]))

    # périodes : "initial" (avant le 1er mois de la portée) puis un fichier par mois
    mois = C.mois_de(sc["debut"], sc["fin"])
    periodes = [("initial", dt.date(1900, 1, 1), d0)]
    for (y, m) in mois:
        a = dt.date(y, m, 1)
        b = dt.date(y + (m == 12), 1 if m == 12 else m + 1, 1)
        periodes.append((f"{y}-{m:02d}", a, b))
    for nom, a, b in periodes:
        rng = C.rng_for(cfg, "clients_p", nom)
        cles = f"{'initial_' + d0.isoformat() if nom == 'initial' else nom}"
        # --- CRM ---
        rel = f"clients/crm/crm_{cles}.csv"
        if not C.deja_fait(C.raw(cfg, rel), cfg):
            idx = fenetre(P["in_crm"], a, b)
            upd = _tirer_maj(rng, P, "in_crm", idx, a, cfg["volumes"]["part_maj_clients"]) if nom != "initial" else np.array([], dtype=int)
            ids = np.concatenate([idx, upd]).astype(int)
            est_maj = np.concatenate([np.zeros(len(idx), bool), np.ones(len(upd), bool)])
            # doublons intra-source (saisie différente) : nouveau cli_id, même personne
            nd = int(len(idx) * cfg["defauts"]["doublons_clients"])
            dup = rng.choice(idx, nd, replace=False) if nd and len(idx) else np.array([], dtype=int)
            tel8 = P["tel8"][ids].copy()
            rue = P["rue"][ids].copy()
            tel8[est_maj] = C.tel_chiffres(rng, int(est_maj.sum()))
            rue[est_maj] = np.array([f"{int(rng.integers(1, 250))} {R.RUES[int(rng.integers(0, len(R.RUES)))]}" for _ in range(int(est_maj.sum()))])
            dtm = np.array([(a + dt.timedelta(days=int(x))).isoformat() for x in rng.integers(0, 27, len(ids))], dtype=object)
            dtc = np.array([P["crea"][i].isoformat() for i in ids])
            dtm = np.where(est_maj, dtm, dtc)
            rows = dict(cli_id=cli_id[ids], c_nm=P["nm"][ids], c_pn=P["pn"][ids], dn=P["dn"][ids], sx=P["sx"][ids], tel8=tel8, em=P["em"][ids],
                        ad1=rue, vl=P["dl"][ids], cp=P["cp"][ids].astype(str), gv=P["gv"][ids],
                        n_ci=_u(np.where(rng.random(len(ids)) < 0.25, P["cin"][ids], None)), dt_crea=dtc, dt_maj=dtm)
            df = pl.DataFrame(rows, strict=False)
            if len(dup):                       # lignes dupliquées avec variations
                d = P
                extra = pl.DataFrame(dict(
                    cli_id=[f"C9{int(x) :06d}" for x in dup], c_nm=_var_nom(d["nm"][dup], rng, 0.3), c_pn=_var_nom(d["pn"][dup], rng, 0.3),
                    dn=d["dn"][dup], sx=d["sx"][dup], tel8=np.where(rng.random(len(dup)) < 0.5, d["tel8"][dup], C.tel_chiffres(rng, len(dup))),
                    em=_u(np.where(rng.random(len(dup)) < 0.6, d["em"][dup], None)), ad1=np.array([(x.replace("Avenue", "Av.").replace("Rue", "R.")) for x in d["rue"][dup]]),
                    vl=d["dl"][dup], cp=d["cp"][dup].astype(str), gv=d["gv"][dup], n_ci=_u(np.where(rng.random(len(dup)) < 0.25, d["cin"][dup], None)),
                    dt_crea=np.array([d["crea"][i].isoformat() for i in dup]), dt_maj=np.array([d["crea"][i].isoformat() for i in dup])), strict=False)
                df = pl.concat([df, extra])
                for i, x in zip(dup, extra["cli_id"]): verite.append(("crm", x, int(i)))
            for i in ids[~est_maj]: verite.append(("crm", cli_id[i], int(i)))
            tel, nv = C.tel_formats(df["tel8"].to_numpy().astype(str), rng, tvar)
            df = df.with_columns(pl.Series("nt", tel)).drop("tel8").select(["cli_id", "c_nm", "c_pn", "dn", "sx", "nt", "em", "ad1", "vl", "cp", "gv", "n_ci", "dt_crea", "dt_maj"])
            spec = {"nulls": ["dn", "em", "ad1", "cp"], "texte": ["c_nm", "c_pn", "ad1", "vl"], "dates": ["dn", "dt_crea", "dt_maj"], "doublons": True}
            C.sortie_csv(cfg, sink, rel, "clients/crm", df, spec, rng, an_extra=[("telephone_format_varie", "nt", nv)] + ([("doublon_intra_source", "cli_id", len(dup))] if len(dup) else []))
        # --- Carte de fidélité ---
        rel = f"clients/fidelite/fidelite_{cles}.csv"
        if not C.deja_fait(C.raw(cfg, rel), cfg):
            idx = fenetre(P["in_fid"], a, b)
            upd = _tirer_maj(rng, P, "in_fid", idx, a, cfg["volumes"]["part_maj_clients"]) if nom != "initial" else np.array([], dtype=int)
            ids = np.concatenate([idx, upd]).astype(int)
            fo = rng.integers(0, 3, len(ids))
            nomp = np.array([[f"{P['nm'][i].upper()} {P['pn'][i]}", f"{P['pn'][i]} {P['nm'][i].upper()}", f"{P['nm'][i]} {P['pn'][i]}"][f] for i, f in zip(ids, fo)], dtype=object)
            nomp = _var_nom(nomp, rng, 0.06)
            tel8 = P["tel8"][ids].copy()
            ch = rng.random(len(ids)) < 0.10
            tel8[ch] = C.tel_chiffres(rng, int(ch.sum()))                 # 2e numéro : fait diverger les sources
            tel8[len(idx):] = C.tel_chiffres(rng, len(upd))
            tel, nv = C.tel_formats(tel8, rng, tvar)
            pts = rng.integers(0, 6000, len(ids))
            df = pl.DataFrame(dict(fid_id=fid_id[ids].astype(str), nom_prenom=nomp.astype(str), tel_cli=tel,
                                   niv=np.where(pts > 3500, "OR", np.where(pts > 1200, "ARGENT", "STD")), pts=pts,
                                   dt_adh=np.array([P["crea"][i].isoformat() for i in ids]), mg_adh=np.array([f"magasin_{x:02d}" for x in mg_adh[ids]])))
            for i in ids[:len(idx)]: verite.append(("fidelite", fid_id[i], int(i)))
            spec = {"nulls": ["tel_cli", "niv"], "texte": ["nom_prenom"], "dates": ["dt_adh"], "doublons": True}
            C.sortie_csv(cfg, sink, rel, "clients/fidelite", df, spec, rng, an_extra=[("telephone_format_varie", "tel_cli", nv)])
        # --- Comptes web (JSONL) ---
        rel = f"clients/ecommerce/comptes_{cles}.jsonl"
        if not C.deja_fait(C.raw(cfg, rel), cfg):
            idx = fenetre(P["in_web"], a, b)
            upd = _tirer_maj(rng, P, "in_web", idx, a, cfg["volumes"]["part_maj_clients"]) if nom != "initial" else np.array([], dtype=int)
            ids = np.concatenate([idx, upd]).astype(int)
            tel8 = P["tel8"][ids].copy(); tel8[len(idx):] = C.tel_chiffres(rng, len(upd))
            mob, nv = C.tel_formats(tel8, rng, tvar)
            lines, nn = [], 0
            for j, i in enumerate(ids):
                em = P["em"][i] if rng.random() > 0.03 else None
                nn += em is None
                if em and rng.random() < 0.12:
                    em = em.replace("gmail.com", "gmail.tn") if rng.random() < 0.5 else em.replace(".", "_", 1)
                lines.append(json.dumps({"uid": uid[i], "profil": {"nom": P["nm"][i], "prenom": P["pn"][i], "email": em, "mobile": str(mob[j])},
                                         "adr": {"rue": P["rue"][i], "ville": P["dl"][i], "cp": str(P["cp"][i]), "gouv": P["gv"][i]},
                                         "cree_le": f"{P['crea'][i].isoformat()}T{int(rng.integers(0, 24)):02d}:{int(rng.integers(0, 60)):02d}:00",
                                         "consent_news": bool(rng.random() < 0.45), "device": ["android", "ios", "web", "web"][int(rng.integers(0, 4))]},
                                        ensure_ascii=False))
                if j < len(idx): verite.append(("web", uid[i], int(i)))
            data = ("\n".join(lines) + "\n") if lines else ""
            nb = C.ecrire_octets(C.raw(cfg, rel), data.encode("utf-8"), cfg)
            sink.put(rel, "clients/ecommerce", len(lines), nb, [(rel, "null", "profil.email", nn), (rel, "telephone_format_varie", "profil.mobile", nv)])
    sink.close()
    if verite:
        pl.DataFrame(verite, schema=["source", "id", "personne_id"], orient="row").write_csv(C.meta(cfg, "verite_clients.csv"))
    json.dump({"crm": {"cli_id": "identifiant_client_crm", "c_nm": "nom", "c_pn": "prenom", "dn": "date_naissance", "sx": "sexe", "nt": "telephone",
                       "em": "email", "ad1": "adresse", "vl": "ville", "cp": "code_postal", "gv": "gouvernorat", "n_ci": "cin"},
               "fidelite": {"fid_id": "identifiant_carte_fidelite", "nom_prenom": "nom_complet", "tel_cli": "telephone", "niv": "niveau_fidelite", "pts": "points", "mg_adh": "magasin_adhesion"}},
              open(C.meta(cfg, "_verite") + "/clients.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    return {"personnes": n, "cartes": int(P["in_fid"].sum())}
