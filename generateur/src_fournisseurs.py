"""src_fournisseurs.py — Fournisseurs (~120) : fiches (2 snapshots), tarifs mensuels. (documents : voir src_documents.py)"""
import json
import os
import zlib

import numpy as np
import polars as pl

import common as C
import referentiels as R

DOMAINES = [("alimentaire", 40), ("boissons", 12), ("parapharmacie", 12), ("hygiene", 18), ("scolaire", 10), ("textile", 18),
            ("electromenager", 10)]
SUFFIXES = {"alimentaire": ["AGRO", "ALIMENTAIRE", "DISTRIBUTION", "INDUSTRIES LAITIERES", "CONSERVERIE", "MINOTERIE"],
            "boissons": ["BOISSONS", "EMBOUTEILLAGE", "EAUX ET JUS"], "parapharmacie": ["PHARMA", "LABORATOIRES", "PARAMEDICAL"],
            "hygiene": ["HYGIENE", "DETERGENTS", "COSMETIQUES"], "scolaire": ["PAPETERIE", "IMPRIMERIE", "FOURNITURES"],
            "textile": ["TEXTILE", "CONFECTION", "HABILLEMENT"], "electromenager": ["ELECTROMENAGER", "IMPORT", "ELECTRONIQUE"]}
NOMS_BASE = ["CARTHAGE", "SAHEL", "MEDINA", "TANIT", "ZITOUNA", "SALAMBO", "DJERBA", "KAIROUAN", "SFAX", "CAP BON", "BIZERTE",
             "GABES", "SAHARA", "OASIS", "BARAKA", "JASMIN", "MIMOSA", "HAMMAMET", "MONASTIR", "MAHDIA", "EL WAFA", "AL BAHR"]
VILLES_IND = ["Tunis", "Sfax", "Sousse", "Nabeul", "Ben Arous", "Bizerte", "Monastir", "Ariana", "Mahdia", "Manouba"]


def construire_fournisseurs(cfg):
    """Fournisseurs maîtres (déterministe, indépendant de la portée)."""
    rng = C.rng_for(cfg, "fournisseurs")
    n_tot = int(cfg["volumes"]["fournisseurs"])
    tot = sum(n for _, n in DOMAINES)
    comptes = [max(2, round(n * n_tot / tot)) for _, n in DOMAINES]
    dom = np.repeat([d for d, _ in DOMAINES], comptes)
    n = len(dom)
    rs, em, vil = [], [], []
    for i in range(n):
        base = NOMS_BASE[rng.integers(0, len(NOMS_BASE))]
        suf = SUFFIXES[dom[i]][rng.integers(0, len(SUFFIXES[dom[i]]))]
        leg = ["SARL", "SA", "SUARL"][rng.choice(3, p=[0.6, 0.3, 0.1])]
        pre = ["STE", "STE", "ETS", "GROUPE"][rng.integers(0, 4)]
        nom = f"{pre} {base} {suf} {leg}"
        rs.append(nom)
        em.append("contact@" + (base + suf).lower().replace(" ", "")[:18] + str(i) + ".tn")
        vil.append(VILLES_IND[rng.integers(0, len(VILLES_IND))])
    return pl.DataFrame({"id_frn": [f"F{i + 1:04d}" for i in range(n)], "rs": rs, "mf": C.matricule_fiscal(rng, n),
                         "rib": C.rib_20(rng, n), "tel8": C.tel_chiffres(rng, n), "em": em, "vil": vil,
                         "delai_j": rng.integers(1, 15, n), "dom": dom})


def dates_snapshots(cfg, pas_mois=None):
    """Dates de snapshots : mode full = tous les pas_mois mois depuis le début ; sample = début + milieu de la portée."""
    sc = C.scope(cfg)
    d0 = sc["debut"].replace(day=1)
    if sc["sample"]:
        mid = sc["debut"] + (sc["fin"] - sc["debut"]) / 2
        return [d0, mid.replace(day=1) if mid.replace(day=1) > d0 else (d0.replace(day=28) + __import__("datetime").timedelta(days=5)).replace(day=1)]
    out = []
    for (y, m) in C.mois_de(*C.periode_complete(cfg))[::pas_mois or 12]:
        out.append(__import__("datetime").date(y, m, 1))
    return out


def run_fiches(cfg, rng_unused=None):
    f = construire_fournisseurs(cfg)
    sink = C.Sink(cfg, "fournisseurs", "fiches")
    snaps = dates_snapshots(cfg, 12)
    if len(snaps) == 1:
        snaps = snaps + []
    for k, d in enumerate(snaps):
        rel = f"fournisseurs/fiches/fiches_{d.isoformat()}.csv"
        if C.deja_fait(C.raw(cfg, rel), cfg):
            continue
        rng = C.rng_for(cfg, "fiches", d.isoformat())
        g = f
        if k > 0:       # 2e snapshot : mises à jour (téléphone, e-mail, ville, délai)
            g = f.clone()
            upd = rng.random(g.height) < 0.18
            tel8 = np.where(upd & (rng.random(g.height) < 0.6), C.tel_chiffres(rng, g.height), g["tel8"].to_numpy())
            vil = np.where(upd & (rng.random(g.height) < 0.3), np.array(VILLES_IND)[rng.integers(0, len(VILLES_IND), g.height)], g["vil"].to_numpy())
            dl = np.where(upd & (rng.random(g.height) < 0.5), rng.integers(1, 15, g.height), g["delai_j"].to_numpy())
            em = np.where(upd & (rng.random(g.height) < 0.25), np.char.add(g["em"].to_numpy().astype(str), ""), g["em"].to_numpy())
            em = np.where(upd & (rng.random(g.height) < 0.25), np.array([e.replace("contact@", "commercial@") for e in g["em"].to_list()]), em)
            g = g.with_columns(pl.Series("tel8", tel8), pl.Series("vil", vil), pl.Series("delai_j", dl), pl.Series("em", em))
        tel, _ = C.tel_formats(g["tel8"].to_numpy(), rng, cfg["defauts"]["telephones_varies"])
        out = g.select(["id_frn", "rs", "mf", "rib"]).with_columns(pl.Series("tel", tel)).with_columns(g["em"], g["vil"], g["delai_j"])
        spec = {"nulls": ["em", "vil"], "texte": ["rs", "vil"], "doublons": True}
        C.sortie_csv(cfg, sink, rel, "fournisseurs/fiches", out, spec, rng)
    sink.close()
    return {"fiches": len(snaps)}


def run_tarifs(cfg):
    """Tarifs mensuels fournisseur : prix d'achat HT du mois par (fournisseur, article)."""
    P = C.charger_etat(cfg, "produits.parquet")
    PX = C.charger_etat(cfg, "prix.npz")
    sc = C.scope(cfg)
    p0 = C.periode_complete(cfg)[0]
    sink = C.Sink(cfg, "fournisseurs", "tarifs")
    n = 0
    for (y, m) in C.mois_de(sc["debut"], sc["fin"]):
        mi = (y - p0.year) * 12 + (m - p0.month)
        rel = f"fournisseurs/tarifs/tarifs_{y}-{m:02d}.csv"
        if C.deja_fait(C.raw(cfg, rel), cfg):
            continue
        rng = C.rng_for(cfg, "tarifs", y, m)
        ok = P["m_lanc"].to_numpy() <= mi
        df = pl.DataFrame({"id_frn": P["frn"].to_numpy()[ok], "art": P["code"].to_numpy()[ok],
                           "px_ach": PX["PA"][mi][ok], "dt_eff": [f"{y}-{m:02d}-01"] * int(ok.sum()),
                           "cmd_min": rng.choice([1, 6, 12, 24, 48], int(ok.sum()))})
        gen = lambda r, k: np.array([f"F{9000 + int(x):04d}" for x in r.integers(0, 999, k)])
        spec = {"nulls": ["cmd_min"], "dates": ["dt_eff"], "aberr": {"px_ach": [10, 100]}, "orphelins": {"id_frn": gen}}
        C.sortie_csv(cfg, sink, rel, "fournisseurs/tarifs", df, spec, rng, post=lambda d: d.with_columns(C.tnd("px_ach")))
        n += 1
    sink.close()
    return {"tarifs": n}
