#!/usr/bin/env python3
"""
run_all.py — Orchestrateur du générateur RAW.

    python run_all.py --estimate            # extrapole le volume complet (mini-génération mesurée)
    python run_all.py                       # mode du config.yaml (sample par défaut)
    python run_all.py --mode full           # génération complète
    python run_all.py --skip-ref            # réutilise les référentiels déjà générés (magasins, produits, clients…)
    python run_all.py --workers 1           # séquentiel (débogage)
    python run_all.py --only ventes,avis    # sous-ensemble de sources
Reprenable : relancer la commande ignore les fichiers déjà générés.
"""
import argparse
import copy
import datetime as dt
import json
import os
import shutil
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import common as C
import referentiels as R
import src_avis, src_clients, src_documents, src_ecommerce, src_fournisseurs, src_produits, src_ref, src_ventes

GO = 1024 ** 3
ETAPES_REF = [("referentiel", src_ref.run), ("produits", src_produits.run), ("fournisseurs_fiches", src_fournisseurs.run_fiches),
              ("fournisseurs_tarifs", src_fournisseurs.run_tarifs), ("clients", src_clients.run)]
ETAPES = [("ventes", src_ventes.run), ("ecommerce", src_ecommerce.run), ("avis", src_avis.run), ("documents", src_documents.run)]
MAP_DEFAUT = {"null": "nulls", "doublon_exact": "doublons_exacts", "date_format_mixte": "dates_mixtes", "orphelin": "orphelins", "aberrant": "aberrants",
              "telephone_format_varie": "telephones_varies", "texte_degrade": "texte", "doublon_intra_source": "doublons_clients"}
# regroupement "budget" du cahier des charges
BUDGET = {"ventes structurées": (["ventes/", "erp/stock"], 16), "logs POS": (["pos_logs"], 10), "e-commerce": (["ecommerce/"], 6),
          "XML/PDF": (["fournisseurs/factures_xml", "fournisseurs/contrats_pdf", "fournisseurs/fiches_techniques_pdf"], 3),
          "texte libre": (["avis/", "appels/"], 3),
          "clients/produits/fournisseurs/RH/réf.": (["clients/", "erp/produits", "erp/hist_prix", "erp/promos", "fournisseurs/fiches", "fournisseurs/tarifs", "rh/", "referentiel/"], 1)}


def _parts(cfg):
    base = C.meta(cfg, "_parts")
    out = {}
    for src in (os.listdir(base) if os.path.isdir(base) else []):
        for f in os.listdir(os.path.join(base, src)):
            if f.endswith(".json"):
                with open(os.path.join(base, src, f), encoding="utf-8") as fh:
                    out.update(json.load(fh))
    return out


def rapport(cfg, durees):
    parts = _parts(cfg)
    groupes, anom, fichiers = {}, {}, 0
    budget = {k: dict(octets=0, fichiers=0, lignes=0) for k in BUDGET}
    for rel, e in parts.items():
        g = groupes.setdefault(e["g"], dict(fichiers=0, octets=0, lignes=0))
        g["fichiers"] += 1; g["octets"] += e["bytes"]; g["lignes"] += e["rows"]
        for k, (pref, _) in BUDGET.items():
            if any(rel.startswith(p) for p in pref):
                budget[k]["octets"] += e["bytes"]; budget[k]["fichiers"] += 1; budget[k]["lignes"] += e["rows"]
                break
        for (f, typ, col, n) in e["anom"]:
            a = anom.setdefault((e["g"], typ, col), 0)
            anom[(e["g"], typ, col)] = a + n
    # journal des anomalies (fichier, type, colonne, nombre)
    jp = C.meta(cfg, "journal_anomalies.csv")
    os.makedirs(os.path.dirname(jp), exist_ok=True)
    with open(jp, "w", encoding="utf-8") as f:
        f.write("fichier,type,colonne,nombre\n")
        for rel, e in sorted(parts.items()):
            for (fi, typ, col, n) in e["anom"]:
                f.write(f'{fi},{typ},"{col}",{n}\n')
    taux = []
    for (g, typ, col), n in sorted(anom.items()):
        rows = groupes[g]["lignes"]
        cle = MAP_DEFAUT.get(typ)
        taux.append(dict(groupe=g, type=typ, colonne=col, nombre=n, lignes=rows, taux_reel=round(n / rows, 5) if rows else None,
                         taux_config=cfg["defauts"].get(cle) if cle else None))
    if cfg.get("_distribue"):          # données dans le stockage objet : taille = somme des tailles journalisées
        disque = sum(e["bytes"] for e in parts.values())
    else:
        disque = sum(os.path.getsize(os.path.join(dp, f)) for dp, dn, fn in os.walk(cfg["_raw"]) if "_meta" not in dp.split(os.sep) for f in fn)
    rap = dict(genere_le=dt.datetime.now().isoformat(timespec="seconds"), mode=cfg["mode"], graine=cfg["graine"], compression=cfg["compression"],
               periode={k: str(v) for k, v in C.scope(cfg).items() if k in ("debut", "fin")}, magasins=C.scope(cfg)["magasins"],
               taille_totale_octets=disque, taille_totale_go=round(disque / GO, 3), nombre_fichiers=len(parts),
               volumes_par_budget={k: dict(go=round(v["octets"] / GO, 3), cible_go=BUDGET[k][1], fichiers=v["fichiers"], lignes=v["lignes"]) for k, v in budget.items()},
               volumes_par_source={g: dict(go=round(v["octets"] / GO, 4), fichiers=v["fichiers"], lignes=v["lignes"]) for g, v in sorted(groupes.items())},
               taux_defauts=taux, durees_s={k: round(v, 1) for k, v in durees.items()})
    with open(C.meta(cfg, "rapport_generation.json"), "w", encoding="utf-8") as f:
        json.dump(rap, f, ensure_ascii=False, indent=1)
    # vérité terrain fusionnée
    vt = {}
    vd = C.meta(cfg, "_verite")
    for fn in sorted(os.listdir(vd)) if os.path.isdir(vd) else []:
        vt[fn[:-5]] = json.load(open(os.path.join(vd, fn), encoding="utf-8"))
    json.dump(vt, open(C.meta(cfg, "verite_terrain.json"), "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    return rap


def _facteur(d, plafond, croiss=None, cfg=None):
    f = min(R.facteurs_jour(d)[0], plafond)
    return f * (src_ecommerce.croissance(d, cfg) if croiss else 1)


def estimer(cfg):
    """Mini-génération mesurée (≈ 2-3 min) puis extrapolation au périmètre complet de la config."""
    t0 = time.time()
    tmp = tempfile.mkdtemp(prefix="est_ref_")
    ce = copy.deepcopy(cfg); ce["_raw"] = tmp; ce["mode"] = "sample"; ce["max_workers"] = 1
    ce["echantillon"] = dict(debut="2025-02-10", fin="2025-03-12", magasins=["magasin_01", "magasin_03", "magasin_11"], facteur_clients=0.1)
    print("• estimation : référentiels de mesure …", flush=True)
    for _, fn in ETAPES_REF:
        fn(ce)
    r_ref = _parts(ce)
    full_sc = C.scope(dict(cfg, mode="full"))
    jours_full = list(C.jours(*C.periode_complete(cfg)))
    tot = {}
    # 1. ventes (octets par ticket x tickets attendus)
    print("• estimation : ventes / logs POS / stock …", flush=True)
    cv = copy.deepcopy(cfg); cv["_raw"] = tmp; cv["mode"] = "full"
    mes = src_ventes.mesurer(cv)
    ev = src_ventes.estimer(dict(cv), mes)
    tot["ventes structurées"] = ev["ventes"] + ev["stock"]; tot["logs POS"] = ev["logs"]
    # 2. sources quotidiennes : mini-run d'une semaine complète
    print("• estimation : e-commerce / texte libre / documents (1 semaine) …", flush=True)
    tmp2 = tempfile.mkdtemp(prefix="est_mini_")
    os.makedirs(os.path.join(tmp2, "_meta"))
    os.symlink(os.path.join(tmp, "_meta", "_etat"), os.path.join(tmp2, "_meta", "_etat"))
    cm = copy.deepcopy(ce); cm["_raw"] = tmp2
    cm["echantillon"] = dict(debut="2025-05-12", fin="2025-05-18", magasins=["magasin_01", "magasin_03", "magasin_11"], facteur_clients=0.1)
    src_ecommerce.run(cm); src_avis.run(cm); src_documents.run(cm)
    pm = _parts(cm)
    jm = list(C.jours(dt.date(2025, 5, 12), dt.date(2025, 5, 18)))
    b = lambda pref: sum(e["bytes"] for r, e in pm.items() if r.startswith(pref))
    n = lambda pref: sum(1 for r in pm if r.startswith(pref))
    ratio = lambda pl_, cr=None: sum(_facteur(d, pl_, cr, cfg) for d in jours_full) / sum(_facteur(d, pl_, cr, cfg) for d in jm)
    eco = (b("ecommerce/commandes") + b("ecommerce/clickstream")) * ratio(1.5, True)
    cat = b("ecommerce/catalogue_api") / max(1, n("ecommerce/catalogue_api")) * (len(jours_full) / cfg["ecommerce"]["catalogue_pas_jours"])
    tot["e-commerce"] = eco + cat
    tot["texte libre"] = (b("avis/") + b("appels/")) * ratio(1.5)
    fm = {f: 0 for f in ("hypermarche", "supermarche", "proximite")}
    for r, e in pm.items():
        if r.startswith("fournisseurs/factures_xml/"):
            fm[next(x["format"] for x in C.magasins_cfg(cfg) if x["id"] == r.split("/")[2])] += e["bytes"]
    nm = {f: sum(1 for x in C.magasins_cfg(cfg) if x["format"] == f) for f in fm}
    fac = sum(fm[f] * nm[f] for f in fm) * ratio(1.4)
    pdf_c = b("fournisseurs/contrats_pdf") / max(1, n("fournisseurs/contrats_pdf")) * cfg["volumes"]["fournisseurs"] * cfg["documents"]["contrats_par_fournisseur"]
    pdf_f = b("fournisseurs/fiches_techniques_pdf") / max(1, n("fournisseurs/fiches_techniques_pdf")) * cfg["volumes"]["produits"] * cfg["documents"]["fiches_techniques_versions"]
    tot["XML/PDF"] = fac + pdf_c + pdf_f
    # 3. référentiels : extrapolation depuis la mini-génération (produits ×24 mois ; clients proportionnels aux personnes)
    pr = sum(e["bytes"] for r, e in r_ref.items() if r.startswith(("erp/produits", "erp/hist_prix", "erp/promos", "fournisseurs/tarifs"))) / 2 * len(C.mois_de(*C.periode_complete(cfg)))
    cl = sum(e["bytes"] for r, e in r_ref.items() if r.startswith("clients/")) / 0.70 * (cfg["volumes"]["personnes"] / max(1000, int(cfg["volumes"]["personnes"] * 0.1)))
    petit = sum(e["bytes"] for r, e in r_ref.items() if r.startswith(("rh/", "referentiel/", "fournisseurs/fiches"))) * 4
    tot["clients/produits/fournisseurs/RH/réf."] = pr + cl + petit
    total = sum(tot.values())
    cible = cfg["volume_cible_go"]
    print("\n══ ESTIMATION DU VOLUME COMPLET (période %s → %s, 20 magasins) ══" % C.periode_complete(cfg))
    print(f"{'source':44s}{'estimé (Go)':>13s}{'cible (Go)':>12s}")
    for k, v in tot.items():
        print(f"{k:44s}{v / GO:13.2f}{BUDGET[k][1]:12.1f}")
    print(f"{'TOTAL':44s}{total / GO:13.2f}{cible:12.1f}   ({100 * (total / GO / cible - 1):+.1f} % ; tolérance ±10 %)")
    print(f"tickets attendus ≈ {ev['tickets'] / 1e6:.1f} M  ·  disque pour les données ≈ {total / GO:.0f} Go sur 120 Go prévus")
    sec = ev["sec"] / max(1, cfg["max_workers"]) + (time.time() - t0) * 0
    print(f"durée indicative ventes+logs ≈ {sec / 60:.0f} min avec {cfg['max_workers']} worker(s) (hors e-commerce / avis / documents)")
    shutil.rmtree(tmp, ignore_errors=True); shutil.rmtree(tmp2, ignore_errors=True)
    return tot


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.yaml"))
    ap.add_argument("--mode", choices=["sample", "full"])
    ap.add_argument("--out", help="dossier de sortie (remplace 'sortie' du config)")
    ap.add_argument("--workers", type=int)
    ap.add_argument("--estimate", action="store_true")
    ap.add_argument("--skip-ref", action="store_true")
    ap.add_argument("--only")
    a = ap.parse_args()
    cfg = C.load_config(a.config, a.mode, a.out, a.workers)
    if a.estimate:
        estimer(cfg)
        return
    sc = C.scope(cfg)
    print(f"Mode {cfg['mode']} · {sc['debut']} → {sc['fin']} · {len(sc['magasins'])} magasins · workers={cfg['max_workers']} · sortie={cfg['_raw']}", flush=True)
    only = set(a.only.split(",")) if a.only else None
    durees = {}
    if not a.skip_ref:
        for nom, fn in ETAPES_REF:
            if only and nom.split("_")[0] not in only:
                continue
            t = time.time(); print(f"▶ {nom}", flush=True); r = fn(cfg); durees[nom] = time.time() - t; print(f"  {r}  ({durees[nom]:.0f}s)", flush=True)
    elif not os.path.exists(C.etat_path(cfg, "produits.parquet")):
        sys.exit("--skip-ref : référentiels absents (lancer une première fois sans --skip-ref)")
    for nom, fn in ETAPES:
        if only and nom not in only:
            continue
        t = time.time(); print(f"▶ {nom}", flush=True); fn(cfg); durees[nom] = time.time() - t; print(f"  terminé ({durees[nom]:.0f}s)", flush=True)
    rap = rapport(cfg, durees)
    print(f"\n✔ {rap['taille_totale_go']} Go · {rap['nombre_fichiers']} fichiers → {cfg['_raw']}/_meta/rapport_generation.json")


if __name__ == "__main__":
    main()
