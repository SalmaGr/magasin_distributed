"""src_ref.py — Référentiels : magasins (2 snapshots) et employés RH (snapshots périodiques)."""
import datetime as dt
import json
import os

import numpy as np
import polars as pl

import common as C
import referentiels as R
import src_fournisseurs as F

POSTES = [  # (poste, part, salaire brut mensuel min-max en TND)
    ("caissier", 0.0, (780, 1050)), ("vendeur", 0.30, (800, 1150)), ("magasinier", 0.12, (820, 1100)),
    ("chef_rayon", 0.08, (1300, 1900)), ("boucher", 0.05, (900, 1300)), ("agent_securite", 0.04, (750, 950)),
    ("employe_libre_service", 0.25, (760, 1000)), ("caissier_principal", 0.03, (1100, 1500)), ("comptable", 0.02, (1600, 2600))]


def _noms(rng, n):
    etr = rng.random(n) < 0.05
    fem = rng.random(n) < 0.5
    pn = np.where(etr, rng.choice(R.PRENOMS_ETR, n), np.where(fem, rng.choice(R.PRENOMS_F, n), rng.choice(R.PRENOMS_H, n)))
    nm = np.where(etr, rng.choice(R.NOMS_ETR, n), np.array([x.replace("_", " ") for x in rng.choice(R.NOMS, n)]))
    return nm, pn


def construire_employes(cfg):
    rng = C.rng_for(cfg, "employes")
    d0, d1 = C.periode_complete(cfg)
    rows = []
    for mg in C.magasins_cfg(cfg):
        n = int(mg["effectif"])
        ncais = int(mg["caisses"] * 1.4) + 1
        postes = ["caissier"] * ncais
        reste = n - ncais - 1
        pp = np.array([p[1] for p in POSTES[1:]]); pp = pp / pp.sum()
        postes += list(rng.choice([p[0] for p in POSTES[1:]], reste, p=pp)) + ["responsable_magasin"]
        for p in postes:
            lo, hi = (2500, 4200) if p == "responsable_magasin" else next(x[2] for x in POSTES if x[0] == p)
            rows.append(dict(mg=mg["id"], poste=p, sal0=int(rng.integers(lo, hi)) , dt_emb=d0 - dt.timedelta(days=int(rng.integers(30, 2500)))))
    n = len(rows)
    df = pl.DataFrame(rows)
    nm, pn = _noms(rng, n)
    # ~8 % de départs et de nouvelles embauches pendant la période
    sortie = np.where(rng.random(n) < 0.08, rng.integers(60, 700, n), 0)
    emb_tardive = rng.random(n) < 0.05
    emb = df["dt_emb"].to_list()
    emb = [d0 + dt.timedelta(days=int(rng.integers(30, 600))) if t else e for t, e in zip(emb_tardive, emb)]
    dts = [(d0 + dt.timedelta(days=int(s))) if s else None for s in sortie]
    df = df.with_columns(pl.Series("mat", [f"E{i + 1:05d}" for i in range(n)]), pl.Series("nm", nm), pl.Series("pn", pn),
                         pl.Series("n_ci", C.cin_8(rng, n)), pl.Series("tel8", C.tel_chiffres(rng, n)), pl.Series("rib", C.rib_20(rng, n)),
                         pl.Series("dt_emb", emb), pl.Series("dt_sortie", dts, dtype=pl.Date))
    # les caissiers retenus pour les ventes sont présents sur toute la période
    perm = (df["poste"] == "caissier") & (df["dt_sortie"].is_null()) & (df["dt_emb"] <= d0)
    return df.with_columns(perm.alias("permanent"))


def run(cfg):
    sc = C.scope(cfg)
    sink = C.Sink(cfg, "referentiel", "ref")
    os.makedirs(C.etat_path(cfg, ""), exist_ok=True)
    E = construire_employes(cfg)
    E.write_parquet(C.etat_path(cfg, "employes.parquet"))
    M = pl.DataFrame([dict(id=m["id"], num=m["num"], format=m["format"], gouv=m["gouv"], deleg=m["deleg"], tourisme=m["tourisme"],
                           tickets_jour=m["tickets_jour"], caisses=m["caisses"], lignes_moy=m["lignes_moy"], coef_prix=m["coef_prix"],
                           h0=m["heures"][0], h1=m["heures"][1]) for m in C.magasins_cfg(cfg)])
    M.write_parquet(C.etat_path(cfg, "magasins.parquet"))
    C._CACHE.clear()
    # --- magasins : 2 snapshots (le 2e met à jour surface et téléphone de quelques points de vente)
    snaps = F.dates_snapshots(cfg, 12)
    rng0 = C.rng_for(cfg, "magasins")
    n = M.height
    adr, cp, tel8, surf, ouv, mf = [], [], C.tel_chiffres(rng0, n), [], [], C.matricule_fiscal(rng0, n)
    for m in C.magasins_cfg(cfg):
        dl = dict(R.GEO[m["gouv"]])
        adr.append(f"{int(rng0.integers(1, 200))} {R.RUES[int(rng0.integers(0, len(R.RUES)))]}")
        cp.append(dl[m["deleg"]])
        surf.append({"hypermarche": int(rng0.integers(6000, 12000)), "supermarche": int(rng0.integers(1200, 2800)), "proximite": int(rng0.integers(250, 600))}[m["format"]])
        ouv.append((dt.date(2005, 1, 1) + dt.timedelta(days=int(rng0.integers(0, 6500)))).isoformat())
    for k, d in enumerate(snaps):
        rel = f"referentiel/magasins/magasins_{d.isoformat()}.csv"
        if C.deja_fait(C.raw(cfg, rel), cfg):
            continue
        rng = C.rng_for(cfg, "magasins_snap", d.isoformat())
        s, t = np.array(surf), tel8.copy()
        if k > 0:
            u = rng.random(n) < 0.3
            s = np.where(u, (s * rng.uniform(1.02, 1.15, n)).astype(int), s)
            t = np.where(u, C.tel_chiffres(rng, n), t)
        tel, _ = C.tel_formats(t, rng, cfg["defauts"]["telephones_varies"])
        df = pl.DataFrame({"mg": M["id"], "lib_mg": [f"Marché {x['format'][:5].capitalize()} {x['deleg']}" for x in C.magasins_cfg(cfg)],
                           "fmt": ["HYP" if f == "hypermarche" else "SUP" if f == "supermarche" else "PRX" for f in M["format"]],
                           "gv": M["gouv"], "dlg": M["deleg"], "adr": adr, "cp": cp, "tel": tel, "dt_ouv": ouv, "surf": s, "mf": mf})
        C.sortie_csv(cfg, sink, rel, "referentiel/magasins", df, {"nulls": ["adr", "surf"], "texte": ["adr", "dlg"], "dates": ["dt_ouv"], "doublons": True}, rng)
    # --- employés : snapshots (trimestriels en full ; début + milieu en échantillon), augmentation de 5 % chaque janvier
    for d in F.dates_snapshots(cfg, 3):
        rel = f"rh/employes/employes_{d.isoformat()}.csv"
        if C.deja_fait(C.raw(cfg, rel), cfg):
            continue
        rng = C.rng_for(cfg, "emp_snap", d.isoformat())
        a = E.filter((pl.col("dt_emb") <= d) & (pl.col("dt_sortie").is_null() | (pl.col("dt_sortie") > d)))
        k = max(0, d.year - 2025 + (1 if d.month >= 1 and d.year >= 2025 else 0))
        sal = (a["sal0"].to_numpy() * (1.05 ** k)).astype(int)
        tel, _ = C.tel_formats(a["tel8"].to_numpy(), rng, cfg["defauts"]["telephones_varies"])
        df = pl.DataFrame({"mat": a["mat"], "nm": a["nm"], "pn": a["pn"], "n_ci": a["n_ci"], "tel": tel, "pst": a["poste"], "mg": a["mg"],
                           "sal_b": sal * 1000, "dt_emb": [x.isoformat() for x in a["dt_emb"]], "rib": a["rib"]})
        C.sortie_csv(cfg, sink, rel, "rh/employes", df, {"nulls": ["tel", "rib"], "texte": ["nm", "pn"], "dates": ["dt_emb"], "doublons": True}, rng,
                     post=lambda x: x.with_columns(C.tnd("sal_b")))
    sink.close()
    vt = {"magasins": {"mg": "identifiant_magasin", "fmt": "format_magasin", "gv": "gouvernorat", "dlg": "delegation", "adr": "adresse", "cp": "code_postal",
                       "tel": "telephone", "surf": "surface_m2", "mf": "matricule_fiscal"},
          "employes": {"mat": "matricule_employe", "nm": "nom", "pn": "prenom", "n_ci": "cin", "tel": "telephone", "pst": "poste", "sal_b": "salaire_brut", "rib": "rib"}}
    os.makedirs(C.meta(cfg, "_verite"), exist_ok=True)
    json.dump(vt, open(C.meta(cfg, "_verite", "referentiel.json"), "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    return {"magasins": n, "employes": E.height}
