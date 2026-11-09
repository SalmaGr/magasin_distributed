"""src_produits.py — Catalogue (~2000 produits), prix mensuels (inflation par paliers), promotions, exports ERP par table."""
import datetime as dt
import json
import os
import zlib

import numpy as np
import polars as pl

import common as C
import referentiels as R
import src_fournisseurs as F


def _mult_marque(b):
    return 0.90 + (zlib.crc32(b.encode()) % 30) / 100.0


def _combos(cat):
    """Toutes les combinaisons (base, marque, taille, pa, tva, encadré) d'une catégorie."""
    out = []
    if cat == "textile":
        for art, pa, genres in R.TEXTILE_ARTICLES:
            for g in genres:
                for t in R.TEXTILE_TAILLES[g]:
                    for b in R.TEXTILE_MARQUES:
                        out.append((f"{art} {g}", b, t, pa * (0.6 if g == "E" else 1.0) * _mult_marque(b), 19, False))
        return out
    for brands, tpls in R.CATALOGUE[cat]:
        for lib, sizes, tva, enc in tpls:
            for size, pa in sizes:
                for b in brands:
                    out.append((lib, b, size, pa * _mult_marque(b), tva, enc == "E"))
    return out


def construire_produits(cfg):
    rng = C.rng_for(cfg, "produits")
    n_tot = int(cfg["volumes"]["produits"])
    frn = F.construire_fournisseurs(cfg)
    frn_par_cat = {c: frn.filter(pl.col("dom") == c)["id_frn"].to_list() for c in R.CATEGORIES}
    rows = []
    for cat in R.CATEGORIES:
        cb = _combos(cat)
        cible = round(n_tot * R.PART_CAT[cat])
        sel = [cb[i] for i in rng.choice(len(cb), min(cible, len(cb)), replace=False)]
        k = 0
        while len(sel) < cible:                       # repli : formats « lot » si le gabarit est trop court
            b = cb[k % len(cb)]
            sel.append((b[0], b[1], "LOT X2" if k < len(cb) else "FAMILIAL", b[3] * 1.9, b[4], b[5])); k += 1
        for base, marque, taille, pa, tva, enc in sel:
            lo, hi = R.MARGE_ENCADREE if enc else R.MARGES[cat]
            fl = frn_par_cat[cat]
            rows.append(dict(cat=cat, base=base, marque=marque, taille=taille, pa0=round(pa, 3), tva=tva, enc=enc,
                             mrg=round(float(rng.uniform(lo, hi)), 3), frn=fl[zlib.crc32((marque + cat).encode()) % len(fl)]))
    rng.shuffle(rows)
    n = len(rows)
    df = pl.DataFrame(rows)
    mixte = rng.random(n) < cfg["volumes"]["part_mixte"]
    tbl = np.where(mixte, np.array(R.TABLES_MIXTES)[rng.integers(0, 2, n)], np.array([R.TABLE_CAT[c] for c in df["cat"]]))
    unt = np.array([("KG" if t.endswith("KG") else "L" if (t.endswith("L") and not t.endswith("ML") and not t.endswith("CL")) else R.CAT_UNITE[c])
                    for t, c in zip(df["taille"], df["cat"])])
    m_lanc = np.where(rng.random(n) < 0.08, rng.integers(1, 21, n), 0)
    # popularité Zipf (rang aléatoire), normalisée par catégorie -> part de lignes = PART_LIGNES
    rang = rng.permutation(n) + 1
    pop = 1.0 / rang ** 0.9
    cats = df["cat"].to_numpy()
    w = np.zeros(n)
    for c in R.CATEGORIES:
        m = cats == c
        w[m] = pop[m] / pop[m].sum() * R.PART_LIGNES[c]
    df = df.with_columns(pl.Series("idx", np.arange(n)), pl.Series("code", 1000001 + np.arange(n)), pl.Series("table", tbl),
                         pl.Series("unt", unt), pl.Series("m_lanc", m_lanc), pl.Series("poids", w), pl.Series("ean", C.ean13_619(rng, n)))
    return df


def _libelles(df, cfg):
    """Libellés abrégés ; style (marque avant/après) propre à chaque table, mixte par ligne dans les tables mixtes."""
    out = []
    for r in df.iter_rows(named=True):
        if r["cat"] == "textile":
            core = f"{r['base']} {r['marque']} {r['taille']}"
            avant = f"{r['marque']} {r['base']} {r['taille']}"
        else:
            core = f"{r['base']} {r['marque']} {r['taille']}"
            avant = f"{r['marque']} {r['base']} {r['taille']}"
        st = zlib.crc32((r["table"] + "style").encode()) % 2 if r["table"] not in R.TABLES_MIXTES else zlib.crc32(str(r["code"]).encode()) % 2
        out.append(avant if st else core)
    return df.with_columns(pl.Series("lib", out))


def construire_prix(cfg, P):
    """Matrices [mois, produit] en millimes : PA, PV_HT, PV_TTC + dernier mois de changement."""
    rng = C.rng_for(cfg, "prix")
    n, M = P.height, len(C.mois_de(*C.periode_complete(cfg)))
    enc = P["enc"].to_numpy()
    prob = np.where(enc, 0.04, 0.45)
    pas = np.where(enc[None, :], rng.uniform(0.03, 0.08, (M, n)), rng.uniform(0.004, 0.018, (M, n)))
    chg = rng.random((M, n)) < prob[None, :]
    chg[0] = False
    idx = np.cumprod(1 + pas * chg, axis=0)
    last = np.where(chg, np.arange(M)[:, None], -1)
    last = np.maximum.accumulate(last, axis=0)
    PA = np.maximum(C.arrondi5(P["pa0"].to_numpy()[None, :] * 1000 * idx), 100)
    mrg, tva = P["mrg"].to_numpy(), P["tva"].to_numpy()
    PV = C.arrondi5(PA * (1 + mrg[None, :]))
    TTC = C.arrondi5(PV * (1 + tva[None, :] / 100.0))
    return PA, PV, TTC, np.maximum(last, 0)


def construire_promos(cfg, P):
    rng = C.rng_for(cfg, "promos")
    d0, d1 = C.periode_complete(cfg)
    cats, codes, enc = P["cat"].to_numpy(), P["code"].to_numpy(), P["enc"].to_numpy()
    rows = []

    def add(mask, deb, fin, taux, typ, jitter=0):
        for i in np.flatnonzero(mask):
            a = deb + dt.timedelta(days=int(rng.integers(0, jitter + 1)))
            b = fin - dt.timedelta(days=int(rng.integers(0, jitter + 1)))
            if b >= d0 and a <= d1 and b >= a:
                rows.append((int(codes[i]), max(a, d0), min(b, d1), int(rng.choice(taux)), typ))
    for y in (2024, 2025, 2026):
        for (m1, j1), (m2, j2) in R.SOLDES:
            add((cats == "textile") & (rng.random(len(cats)) < 0.8), dt.date(y, m1, j1), dt.date(y, m2, j2), [20, 30, 40, 50], "SOLDES_TEXTILE", 5)
        add((cats == "scolaire") & (rng.random(len(cats)) < 0.45), dt.date(y, 9, 1), dt.date(y, 9, 15), [10, 15, 20], "RENTREE", 2)
    for a, b in R.RAMADAN:
        add((cats == "alimentaire") & ~enc & (rng.random(len(cats)) < 0.25), a, b, [5, 10, 15], "RAMADAN", 3)
        add((cats == "boissons") & (rng.random(len(cats)) < 0.15), a, b, [5, 10], "RAMADAN", 3)
    for (y, m) in C.mois_de(d0, d1):                      # promotions aléatoires datées
        mk = (rng.random(len(cats)) < 0.025) & ~enc
        for i in np.flatnonzero(mk):
            a = dt.date(y, m, int(rng.integers(1, 22)))
            rows.append((int(codes[i]), max(a, d0), min(a + dt.timedelta(days=6), d1), int(rng.choice([10, 15, 20, 25, 30])), "ALEATOIRE"))
    df = pl.DataFrame(rows, schema=["art", "dt_deb", "dt_fin", "tx_rem", "typ_pr"], orient="row")
    return df.sort(["dt_deb", "art"]).with_row_index("n", offset=1).with_columns(
        ("PR" + pl.col("n").cast(pl.Utf8).str.zfill(6)).alias("id_pr")).drop("n")


def colonnes_table(cfg, table):
    rng = C.rng_for(cfg, "colonnes", table)
    v = {k: opts[int(rng.integers(0, len(opts)))] for k, opts in R.VARIANTES_COLONNES.items()}
    inclus = {"id", "lib", "pa", "pv_ht", "tva", "frn", "unt"}
    if rng.random() < 0.80: inclus.add("mrg")
    if rng.random() < 0.75: inclus.add("pv_ttc")
    if rng.random() < 0.85: inclus.add("ean")
    return v, inclus


def build_all(cfg):
    P = _libelles(construire_produits(cfg), cfg)
    PA, PV, TTC, LAST = construire_prix(cfg, P)
    PR = construire_promos(cfg, P)
    os.makedirs(C.etat_path(cfg, ""), exist_ok=True)
    P.write_parquet(C.etat_path(cfg, "produits.parquet"))
    PR.write_parquet(C.etat_path(cfg, "promos.parquet"))
    np.savez_compressed(C.etat_path(cfg, "prix.npz"), PA=PA, PV=PV, TTC=TTC, LAST=LAST)
    C._CACHE.clear()
    return P, PA, PV, TTC, LAST, PR


def run(cfg):
    sc = C.scope(cfg)
    P, PA, PV, TTC, LAST, PR = build_all(cfg)
    d0 = C.periode_complete(cfg)[0]
    sink = C.Sink(cfg, "produits", "erp")
    evo = cfg["evolutions"]
    d_maj, d_unt = C.to_date(evo["produits_ajout_dt_maj"]), C.to_date(evo["produits_renomme_unite"])
    # vérité terrain produit
    os.makedirs(C.meta(cfg), exist_ok=True)
    P.select(["code", "table", "cat", "marque", "base", "taille", "mrg", "tva", "frn", "ean"]).rename({"cat": "categorie"}) \
        .write_csv(C.meta(cfg, "verite_produits.csv"))
    verite = {}
    for table in R.TABLES_MIXTES + [R.TABLE_CAT[c] for c in R.CATEGORIES]:
        v, inc = colonnes_table(cfg, table)
        cat = next((c for c, t in R.TABLE_CAT.items() if t == table), "mixte")
        verite[table] = {"categorie": cat, "colonnes": {v[k]: R.NATURE_COLONNES_PRODUIT[k] for k in R.NATURE_COLONNES_PRODUIT if k in inc}}
    with open(C.meta(cfg, "_verite", "produits.json"), "w", encoding="utf-8") if os.makedirs(C.meta(cfg, "_verite"), exist_ok=True) is None else None as f:
        json.dump({"tables_produits": verite}, f, ensure_ascii=False, indent=1)
    nfiles = 0
    for (y, m) in C.mois_de(sc["debut"], sc["fin"]):
        mi = (y - d0.year) * 12 + (m - d0.month)
        d = dt.date(y, m, 1)
        # export produits par table
        for table in R.TABLES_MIXTES + [R.TABLE_CAT[c] for c in R.CATEGORIES]:
            rel = f"erp/produits/{table}/export_{d.isoformat()}.csv"
            if C.deja_fait(C.raw(cfg, rel), cfg):
                continue
            sel = np.flatnonzero((P["table"].to_numpy() == table) & (P["m_lanc"].to_numpy() <= mi))
            v, inc = colonnes_table(cfg, table)
            rng = C.rng_for(cfg, "export", table, y, m)
            cols = {v["id"]: P["code"].to_numpy()[sel], v["lib"]: P["lib"].to_numpy()[sel]}
            if "ean" in inc: cols[v["ean"]] = P["ean"].to_numpy()[sel]
            cols[v["pa"]] = PA[mi][sel]; cols[v["pv_ht"]] = PV[mi][sel]
            if "pv_ttc" in inc: cols[v["pv_ttc"]] = TTC[mi][sel]
            if "mrg" in inc: cols[v["mrg"]] = np.array([f"{x:.3f}" for x in P["mrg"].to_numpy()[sel]])
            cols[v["tva"]] = P["tva"].to_numpy()[sel]; cols[v["frn"]] = P["frn"].to_numpy()[sel]
            unt_name = "cond_vte" if d >= d_unt else v["unt"]
            cols[unt_name] = P["unt"].to_numpy()[sel]
            if d >= d_maj:
                cols["dt_maj"] = np.array([(d0.replace(day=1) + dt.timedelta(days=31 * int(k))).replace(day=1).isoformat() for k in LAST[mi][sel]])
            df = pl.DataFrame(cols)
            fmt_cols = [c for c in (v["pa"], v["pv_ht"], v.get("pv_ttc")) if c in df.columns]
            orph = {v["frn"]: lambda r, k: np.array([f"F{9000 + int(x):04d}" for x in r.integers(0, 999, k)])}
            spec = {"nulls": [unt_name, v["frn"]] + ([v["ean"]] if "ean" in inc else []) + ([v["mrg"]] if "mrg" in inc else []),
                    "texte": [v["lib"]], "dates": ["dt_maj"] if "dt_maj" in df.columns else [],
                    "aberr": {v["pa"]: [10, 100], v["pv_ht"]: [10, 100]}, "orphelins": orph}
            C.sortie_csv(cfg, sink, rel, f"erp/produits/{table}", df, spec, rng,
                         post=lambda x, fc=fmt_cols: x.with_columns([C.tnd(c) for c in fc]))
            nfiles += 1
        # hist_prix
        rel = f"erp/hist_prix/hist_prix_{y}-{m:02d}.csv"
        if not C.deja_fait(C.raw(cfg, rel), cfg):
            ok = P["m_lanc"].to_numpy() <= mi
            h = pl.DataFrame({"cd_art": P["code"].to_numpy()[ok], "per": f"{y}-{m:02d}", "pa": PA[mi][ok], "pvh": PV[mi][ok],
                              "pvt": TTC[mi][ok], "tv": P["tva"].to_numpy()[ok], "tx_mg": P["mrg"].to_numpy()[ok]})
            C.sortie_csv(cfg, sink, rel, "erp/hist_prix", h, {"nulls": ["tx_mg"], "doublons": True}, C.rng_for(cfg, "hist", y, m),
                         post=lambda x: x.with_columns(C.tnd("pa"), C.tnd("pvh"), C.tnd("pvt")))
            nfiles += 1
        # promos débutant ce mois
        rel = f"erp/promos/promos_{y}-{m:02d}.csv"
        if not C.deja_fait(C.raw(cfg, rel), cfg):
            pm = PR.filter((pl.col("dt_deb").dt.year() == y) & (pl.col("dt_deb").dt.month() == m)) \
                .with_columns(pl.col("dt_deb").dt.strftime("%Y-%m-%d"), pl.col("dt_fin").dt.strftime("%Y-%m-%d"), pl.lit("TOUS").alias("mg"))
            C.sortie_csv(cfg, sink, rel, "erp/promos", pm, {"dates": ["dt_deb", "dt_fin"], "nulls": ["tx_rem"]}, C.rng_for(cfg, "promos", y, m))
            nfiles += 1
    sink.close()
    return {"produits": P.height, "promos": PR.height, "fichiers": nfiles}
