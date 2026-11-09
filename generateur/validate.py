#!/usr/bin/env python3
"""
validate.py — Contrôle des critères d'acceptation sur les données générées.
    python validate.py                    # mode du config.yaml
    python validate.py --mode full --ratio 0.03    # en full : échantillonne 3 % des (magasin, jour) pour rester rapide
Code retour 1 si un contrôle échoue.
"""
import argparse
import glob
import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import numpy as np
import polars as pl

import common as C
import referentiels as R

RES = []


def check(nom, ok, detail=""):
    RES.append((nom, ok))
    print(f"  [{'OK ' if ok else 'ÉCHEC'}] {nom}" + (f" — {detail}" if detail else ""), flush=True)


def lire(fichiers):
    """Lit des CSV (éventuellement .gz) en texte, concatène (schémas différents autorisés), ajoute _f."""
    dfs = []
    for f in fichiers:
        d = pl.read_csv(f, infer_schema_length=0, truncate_ragged_lines=True)
        dfs.append(d.with_columns(pl.lit(f).alias("_f")))
    return pl.concat(dfs, how="diagonal_relaxed") if dfs else pl.DataFrame()


def fich(cfg, pat):
    return sorted(glob.glob(C.raw(cfg, pat), recursive=True))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.yaml"))
    ap.add_argument("--mode"); ap.add_argument("--out"); ap.add_argument("--ratio", type=float, default=1.0)
    a = ap.parse_args()
    cfg = C.load_config(a.config, a.mode, a.out)
    sc = C.scope(cfg)
    raw = cfg["_raw"]
    rap = json.load(open(C.meta(cfg, "rapport_generation.json"), encoding="utf-8"))
    vt = json.load(open(C.meta(cfg, "verite_terrain.json"), encoding="utf-8"))
    print(f"Validation de {raw}  (mode {cfg['mode']})\n")

    # ── 1. période / couverture ────────────────────────────────────────────────
    print("1. Période et couverture")
    ft = fich(cfg, "ventes/tickets/*/annee=*/mois=*/tickets_*.csv*")
    attendus = {(m, d.isoformat()) for m in sc["magasins"] for d in C.jours(sc["debut"], sc["fin"])}
    trouves = {(re.search(r"tickets_(magasin_\d+)_(\d{4}-\d{2}-\d{2})", f).group(1), re.search(r"_(\d{4}-\d{2}-\d{2})", f).group(1)) for f in ft}
    check("un fichier tickets par magasin et par jour sur toute la période", attendus <= trouves, f"{len(trouves)}/{len(attendus)} (magasin, jour)")
    for t in ("lignes", "paiements"):
        check(f"fichiers {t} présents pour chaque jour", len(fich(cfg, f"ventes/{t}/*/annee=*/mois=*/{t}_*.csv*")) >= len(ft))
    check("logs POS JSONL présents pour chaque jour", len(fich(cfg, "pos_logs/*/annee=*/mois=*/pos_*.jsonl*")) >= len(ft))
    if cfg["mode"] == "full":
        cible, go = cfg["volume_cible_go"], rap["taille_totale_go"]
        check(f"volume total ≈ {cible} Go (±10 %)", 0.9 * cible <= go <= 1.1 * cible, f"{go} Go")
    else:
        print(f"  (information) volume de l'échantillon : {rap['taille_totale_go'] * 1024:.0f} Mo — la cible de 40 Go ne s'applique qu'en mode full (voir --estimate)")

    # échantillonnage des (magasin, jour) en mode full
    rng = np.random.default_rng(1)
    ft_s = [f for f in ft if a.ratio >= 1 or rng.random() < a.ratio]
    jours_s = {re.search(r"_(magasin_\d+_\d{4}-\d{2}-\d{2})", f).group(1) for f in ft_s}
    sel = lambda t: [f for f in fich(cfg, f"ventes/{t}/*/annee=*/mois=*/{t}_*.csv*") if re.search(r"_(magasin_\d+_\d{4}-\d{2}-\d{2})", f).group(1) in jours_s]
    T, L, P = lire(sel("tickets")), lire(sel("lignes")), lire(sel("paiements"))
    nd = {n: d.height for n, d in (("tickets", T), ("lignes", L), ("paiements", P))}
    T, L, P = T.unique(maintain_order=True), L.unique(maintain_order=True), P.unique(maintain_order=True)      # retire les doublons exacts injectés
    print(f"  {nd['tickets']} tickets · {nd['lignes']} lignes · {nd['paiements']} paiements lus ({len(jours_s)} magasin-jours)")

    # ── 2. cohérence référentielle et totaux ──────────────────────────────────
    print("\n2. Cohérence référentielle, totaux, prix")
    prod = pl.read_csv(C.meta(cfg, "verite_produits.csv"), infer_schema_length=0)
    codes = set(prod["code"].to_list())
    fid = set(C.charger_etat(cfg, "fid.parquet")["fid_id"].to_list())
    orph_art = (~L["art"].is_in(list(codes))).sum() / L.height
    check("articles des lignes : orphelins ≈ taux configuré", abs(orph_art - cfg["defauts"]["orphelins"]) < 0.004, f"{orph_art:.4f} (cfg {cfg['defauts']['orphelins']})")
    cl = T.filter(pl.col("cli").is_not_null())
    orph_cli = (~cl["cli"].is_in(list(fid))).sum() / max(1, cl.height)
    check("cartes de fidélité des tickets : orphelins ≈ taux configuré", abs(orph_cli - cfg["defauts"]["orphelins"]) < 0.004, f"{orph_cli:.4f}")
    Li = L.with_columns(pl.col("mt_ht").str.replace(r"\.", "").cast(pl.Int64).alias("h"), pl.col("mt_ttc").str.replace(r"\.", "").cast(pl.Int64).alias("t"))
    s = Li.group_by("tk_id").agg(pl.col("h").sum().alias("sh"), pl.col("t").sum().alias("st"))
    Tj = T.with_columns(pl.col("mt_ht").str.replace(r"\.", "").cast(pl.Int64).alias("th"), pl.col("mt_ttc").str.replace(r"\.", "").cast(pl.Int64).alias("tt")).join(s, on="tk_id", how="left")
    bad = Tj.filter((pl.col("th") != pl.col("sh")) | (pl.col("tt") != pl.col("st"))).height
    check("total ticket = somme des lignes (HT et TTC)", bad == 0, f"{bad} écarts / {Tj.height}")
    ps = P.with_columns(pl.col("mt").str.replace(r"\.", "").cast(pl.Int64)).group_by("tk_id").agg(pl.col("mt").sum().alias("sp"))
    bad = Tj.join(ps, on="tk_id", how="left").filter(pl.col("tt") != pl.col("sp")).height
    check("somme des paiements = TTC du ticket", bad == 0, f"{bad} écarts")
    # ligne : mt_ht = qte x pu x (1-rem) ; écarts = valeurs aberrantes injectées
    Lc = L.filter(pl.col("rem").is_not_null() & pl.col("qte").is_not_null()).with_columns(
        pl.col("qte").cast(pl.Int64), pl.col("pu_ht").str.replace(r"\.", "").cast(pl.Int64), pl.col("rem").cast(pl.Int64), pl.col("mt_ht").str.replace(r"\.", "").cast(pl.Int64))
    Lc = Lc.with_columns(((pl.col("qte") * pl.col("pu_ht") * (100 - pl.col("rem")) / 100).round().cast(pl.Int64)).alias("att"))
    ecart = Lc.filter(pl.col("att") != pl.col("mt_ht")).height / Lc.height
    check("lignes : montant = qté × prix × (1 − remise), écarts = aberrants injectés", ecart <= cfg["defauts"]["aberrants"] * 1.6 + 0.001, f"{ecart:.4f} (aberrants cfg {cfg['defauts']['aberrants']})")
    check("promotions visibles (lignes avec remise)", (Lc.filter(pl.col("rem") > 0).height / Lc.height) > 0.01, f"{Lc.filter(pl.col('rem') > 0).height / Lc.height:.3f} des lignes")
    # prix cohérents sur les exports
    ok_tot = n_tot = n_marge = n_marge_ok = 0
    for table, v in vt["produits"]["tables_produits"].items():
        nat = {n: c for c, n in v["colonnes"].items()}
        for f in fich(cfg, f"erp/produits/{table}/export_*.csv*"):
            d = lire([f])
            if "prix_achat_ht" not in nat or d.is_empty():
                continue
            pa, pv, mr = nat["prix_achat_ht"], nat["prix_vente_ht"], nat.get("taux_marge")
            d = d.filter(pl.col(pa).is_not_null() & pl.col(pv).is_not_null())
            d = d.with_columns(pl.col(pa).cast(pl.Float64), pl.col(pv).cast(pl.Float64))
            if mr:
                d = d.filter(pl.col(mr).is_not_null()).with_columns(pl.col(mr).cast(pl.Float64))
                att = d[pa] * (1 + d[mr])
                good = ((d[pv] - att).abs() / att < 0.02)
                n_tot += d.height; ok_tot += int(good.sum())
                cat = v["categorie"]
                for row in d.iter_rows(named=True):
                    c = cat if cat != "mixte" else None
                    if c:
                        lo, hi = R.MARGES[c]
                        n_marge += 1; n_marge_ok += (lo - 0.002 <= row[mr] <= hi + 0.002) or (R.MARGE_ENCADREE[0] - 0.002 <= row[mr] <= R.MARGE_ENCADREE[1] + 0.002)
    if n_tot:
        check("exports produits : pv_ht = pa_ht × (1 + marge) (±2 %, arrondi 0,005)", ok_tot / n_tot > 1 - cfg["defauts"]["aberrants"] * 3 - 0.005, f"{ok_tot / n_tot:.4f}")
        check("marges dans les fourchettes par catégorie", n_marge_ok / max(1, n_marge) > 0.99, f"{n_marge_ok / max(1, n_marge):.4f}")

    # ── 3. saisonnalité ─────────────────────────────────────────────────────────
    print("\n3. Saisonnalité")
    Td = T.with_columns(pl.col("tk_id").str.split("-").list.get(2).str.strptime(pl.Date, "%Y%m%d").alias("d"))
    par_jour = Td.group_by("d").len().sort("d")
    pj = par_jour.with_columns(pl.col("d").dt.weekday().alias("w"))
    we = pj.filter(pl.col("w").is_in([6, 7]))["len"].mean(); sem = pj.filter(pl.col("w").is_in([1, 2, 3, 4]))["len"].mean()
    check("pic le week-end (week-end / lundi-jeudi > 1,2)", we / sem > 1.2, f"×{we / sem:.2f}")
    Ld = L.with_columns(pl.col("tk_id").str.split("-").list.get(2).str.strptime(pl.Date, "%Y%m%d").alias("d")).join(prod.select(["code", "categorie"]).with_columns(pl.col("code").alias("art")), on="art", how="left")
    def part(cat, cond):
        x = Ld.filter(cond)
        return x.filter(pl.col("categorie") == cat).height / max(1, x.height)
    import datetime as dt
    any_ram = any(R.en_ramadan(d) for d in C.jours(sc["debut"], sc["fin"])); any_non = any(not R.en_ramadan(d) for d in C.jours(sc["debut"], sc["fin"]))
    if any_ram and any_non:
        ram = pl.col("d").map_elements(R.en_ramadan, return_dtype=pl.Boolean)
        r1, r0 = part("alimentaire", ram), part("alimentaire", ~ram)
        check("Ramadan : part alimentaire en hausse", r1 > r0 * 1.08, f"{r0:.3f} → {r1:.3f}")
    sold = pl.col("d").map_elements(R.en_soldes, return_dtype=pl.Boolean)
    if Ld.filter(sold).height and Ld.filter(~sold).height:
        t1, t0 = part("textile", sold), part("textile", ~sold)
        check("soldes textile : part textile en hausse", t1 > t0 * 1.3, f"{t0:.3f} → {t1:.3f}")
    if any(d.month == 9 and d.day <= 20 for d in C.jours(sc["debut"], sc["fin"])):
        ren = pl.col("d").map_elements(lambda d: d.month == 9 and d.day <= 20, return_dtype=pl.Boolean)
        check("rentrée : part fournitures scolaires en hausse", part("scolaire", ren) > part("scolaire", ~ren) * 2.5)
    if any(d.month in (7, 8) for d in C.jours(sc["debut"], sc["fin"])):
        ete = pl.col("d").map_elements(lambda d: d.month in (7, 8), return_dtype=pl.Boolean)
        check("été : part boissons en hausse", part("boissons", ete) > part("boissons", ~ete) * 1.1)

    # ── 4. formats tunisiens ────────────────────────────────────────────────────
    print("\n4. Formats tunisiens")
    tel = re.compile(r"^(\+216 ?|00216 ?)?[24579]\d( ?|\.)?\d{3}( ?|\.)?\d{3}$")
    def col_ok(df, col, rx):
        v = df[col].drop_nulls()
        return sum(1 for x in v.to_list() if rx.match(x)) / max(1, len(v))
    crm = lire(fich(cfg, "clients/crm/crm_*.csv*")); fi = lire(fich(cfg, "clients/fidelite/fidelite_*.csv*"))
    r1, r2 = col_ok(crm, "nt", tel), col_ok(fi, "tel_cli", tel)
    check("téléphones +216 (mobiles 2/4/5/9, fixes 7) valides", min(r1, r2) > 0.995, f"CRM {r1:.4f} · fidélité {r2:.4f}")
    canon = crm["nt"].drop_nulls().str.contains(r"^\+216\d{8}$").mean()
    check("variantes de téléphone ≈ taux configuré (15 %)", abs((1 - canon) - cfg["defauts"]["telephones_varies"]) < 0.04, f"{1 - canon:.3f}")
    cin = crm["n_ci"].drop_nulls()
    check("CIN : 8 chiffres ; renseigné ≈ 25 %", cin.str.contains(r"^\d{8}$").all() and abs(cin.len() / crm.height - 0.25) < 0.05, f"{cin.len() / crm.height:.3f}")
    emp = lire(fich(cfg, "rh/employes/employes_*.csv*")); ribs = emp["rib"].drop_nulls().to_list()
    check("RIB : 20 chiffres, clé mod 97", all(C.rib_valide(r) for r in ribs), f"{len(ribs)} testés")
    fr = lire(fich(cfg, "fournisseurs/fiches/fiches_*.csv*"))
    mf = re.compile(r"^\d{7}[A-Z]/[A-Z]/[A-Z]/\d{3}$")
    check("matricule fiscal 1234567X/A/M/000", col_ok(fr, "mf", mf) > 0.999 and all(C.rib_valide(r) for r in fr["rib"].drop_nulls().to_list()))
    eans = []
    for table, v in vt["produits"]["tables_produits"].items():
        nat = {n: c for c, n in v["colonnes"].items()}
        if "code_barre_ean13" in nat:
            for f in fich(cfg, f"erp/produits/{table}/export_*.csv*")[:1]:
                eans += lire([f])[nat["code_barre_ean13"]].drop_nulls().to_list()
    check("EAN-13 préfixe 619, clé valide", all(C.ean_valide(e) for e in eans), f"{len(eans)} testés")
    money = re.compile(r"^\d+\.\d{3}$")
    check("montants TND à 3 décimales (tickets, lignes, paiements)", col_ok(T, "mt_ttc", money) == 1 and col_ok(L, "pu_ht", money) == 1 and col_ok(P, "mt", money) == 1)
    geo = {(d, c) for g in R.GEO.values() for d, c in g}
    ok = sum(1 for r in crm.drop_nulls(["vl", "cp"]).iter_rows(named=True) if (r["vl"], int(r["cp"])) in geo if str(r["cp"]).isdigit())
    tot = crm.drop_nulls(["vl", "cp"]).height
    check("adresses : délégation / code postal cohérents (4 chiffres)", ok / tot > 0.95, f"{ok / tot:.3f} (le reste = accents/casse dégradés)")

    # ── 5. défauts et journal ───────────────────────────────────────────────────
    print("\n5. Défauts injectés et journal")
    pires, ab = [], {}
    for t in rap["taux_defauts"]:
        if t["type"] == "aberrant":                       # le taux global est réparti entre les colonnes ciblées : on somme par groupe
            a = ab.setdefault(t["groupe"], dict(t, colonne="(toutes)", nombre=0)); a["nombre"] += t["nombre"]; a["taux_reel"] = a["nombre"] / t["lignes"]
            continue
        if t["taux_config"] and t["lignes"] > 20000 and t["type"] != "doublon_intra_source" and not (t["type"] == "orphelin" and t["colonne"] == "cli"):   # cli : dénominateur = tickets avec carte, contrôlé plus haut
            pires.append((abs(t["taux_reel"] - t["taux_config"]) / t["taux_config"], t))
    for t in ab.values():
        if t["lignes"] > 20000:
            pires.append((abs(t["taux_reel"] - t["taux_config"]) / t["taux_config"], t))
    pires.sort(key=lambda x: -x[0])
    check("taux réels conformes à la config (écart relatif < 25 % sur les groupes > 20 000 lignes)", not pires or pires[0][0] < 0.25,
          f"pire : {pires[0][1]['groupe']} {pires[0][1]['type']}/{pires[0][1]['colonne']} {pires[0][1]['taux_reel']} vs {pires[0][1]['taux_config']}" if pires else "")
    # journal exact : nulls et doublons dans tickets
    jr = pl.read_csv(C.meta(cfg, "journal_anomalies.csv"), infer_schema_length=0)
    jt = jr.filter(pl.col("fichier").is_in([os.path.relpath(f, raw).replace(".gz", "") for f in ft_s]))
    n_null = jt.filter((pl.col("type") == "null") & (pl.col("colonne") == "caissier"))["nombre"].cast(pl.Int64).sum()
    n_dup = jt.filter(pl.col("type") == "doublon_exact")["nombre"].cast(pl.Int64).sum()
    check("journal exact : nulls (tickets.caissier) = valeurs nulles lues", int(T["caissier"].is_null().sum()) == int(n_null), f"{int(n_null)} journalisés")
    check("journal exact : doublons exacts (tickets) = lignes en trop lues", nd["tickets"] - T.height == int(n_dup), f"{int(n_dup)} journalisés")
    mixte = T["dt_v"].str.contains(r"^\d{2}/\d{2}/\d{4}").sum()
    jm = jt.filter(pl.col("type") == "date_format_mixte")["nombre"].cast(pl.Int64).sum()
    check("journal exact : dates au format mixte", int(mixte) == int(jm), f"{int(jm)} journalisés")

    # ── 6. évolutions de schéma ─────────────────────────────────────────────────
    print("\n6. Évolutions de schéma")
    ev = cfg["evolutions"]
    for d, nom in ((ev["tickets_renomme_mp"], "mp → mode_paiement"), ):
        dd = C.to_date(d)
        av = [f for f in ft if re.search(r"_(\d{4}-\d{2}-\d{2})", f).group(1) < dd.isoformat()][:1]
        ap_ = [f for f in ft if re.search(r"_(\d{4}-\d{2}-\d{2})", f).group(1) >= dd.isoformat()][:1]
        if av and ap_:
            check(f"tickets : {nom} à partir du {dd}", "mp" in lire(av).columns and "mode_paiement" in lire(ap_).columns)
    dd = C.to_date(ev["lignes_ajout_pt_fid"])
    fl = fich(cfg, "ventes/lignes/*/annee=*/mois=*/lignes_*.csv*")
    av = [f for f in fl if re.search(r"_(\d{4}-\d{2}-\d{2})", f).group(1) < dd.isoformat()][:1]; ap_ = [f for f in fl if re.search(r"_(\d{4}-\d{2}-\d{2})", f).group(1) >= dd.isoformat()][:1]
    if av and ap_:
        check(f"lignes : colonne pt_fid ajoutée à partir du {dd}", "pt_fid" not in lire(av).columns and "pt_fid" in lire(ap_).columns)
    for key, nom, cle in (("produits_ajout_dt_maj", "dt_maj ajouté", "dt_maj"), ("produits_renomme_unite", "unité → cond_vte", "cond_vte")):
        dd = C.to_date(ev[key]).isoformat()
        fs = fich(cfg, "erp/produits/prod_a/export_*.csv*")
        av = [f for f in fs if re.search(r"export_(\d{4}-\d{2}-\d{2})", f).group(1) < dd][:1]; ap_ = [f for f in fs if re.search(r"export_(\d{4}-\d{2}-\d{2})", f).group(1) >= dd][:1]
        if av and ap_:
            check(f"produits : {nom} à partir du {dd}", cle not in lire(av).columns and cle in lire(ap_).columns)

    # ── 7. opacité des noms ─────────────────────────────────────────────────────
    print("\n7. Noms de tables / colonnes peu parlants")
    interdits = ["alim", "boisson", "textile", "vetement", "pharma", "hygiene", "scolaire", "electro", "category", "categorie"]
    tabs = list(vt["produits"]["tables_produits"])
    cols = [c for v in vt["produits"]["tables_produits"].values() for c in v["colonnes"]] + list(crm.columns) + list(fi.columns) + list(T.columns) + list(L.columns)
    check("aucun nom de table produit ne révèle sa catégorie", not any(w in t.lower() for t in tabs for w in interdits), ", ".join(tabs))
    trop = [c for c in cols if c.lower() in ("telephone", "adresse", "prix", "prix_achat", "marge", "libelle", "designation", "email_client", "date_naissance")]
    check("aucune colonne CSV ne porte directement sa nature", not trop, str(trop[:5]))

    ko = [n for n, ok in RES if not ok]
    print(f"\n{'✔ TOUS LES CONTRÔLES PASSENT' if not ko else '✘ ' + str(len(ko)) + ' contrôle(s) en échec'} ({len(RES) - len(ko)}/{len(RES)})")
    sys.exit(1 if ko else 0)


if __name__ == "__main__":
    main()
