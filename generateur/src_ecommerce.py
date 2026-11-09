"""src_ecommerce.py — E-commerce : commandes (JSONL imbriqué), clickstream (JSONL événementiel), catalogue API (JSON, snapshots)."""
import datetime as dt
import json

import numpy as np
import polars as pl

import common as C
import referentiels as R

UA = ["Mozilla/5.0 (Linux; Android 13; SM-A135F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36",
      "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1",
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0"]
REF = ["direct", "google", "facebook", "instagram", "newsletter", "comparateur_prix"]
EVT = ["page_view", "view_item", "search", "add_to_cart", "remove_from_cart", "begin_checkout", "purchase", "wishlist_add"]
PEVT = [0.30, 0.38, 0.10, 0.10, 0.02, 0.05, 0.02, 0.03]
RAYON = {"alimentaire": ["EPICERIE", "CREMERIE", "CONSERVES"], "boissons": ["BOISSONS"], "parapharmacie": ["PARAPHARMACIE", "SANTE"],
         "hygiene": ["HYGIENE-BEAUTE", "ENTRETIEN"], "scolaire": ["PAPETERIE"], "textile": ["TEXTILE"], "electromenager": ["ELECTROMENAGER", "MAISON"]}
REQ = ["lait", "huile d'olive", "harissa", "couscous", "pâtes", "thon", "cahier", "stylo", "cartable", "paracétamol", "shampoing", "lessive", "t-shirt",
       "jean", "robe", "tv", "réfrigérateur", "machine à laver", "mixeur", "dattes", "eau minérale", "jus d'orange", "couches bébé", "café"]
STATUTS = ["livree"] * 7 + ["en_cours", "annulee", "retour_demande"]


def _etat(cfg):
    return C.charger_etat(cfg, "produits.parquet"), C.charger_etat(cfg, "prix.npz"), C.charger_etat(cfg, "web.parquet")


def croissance(d, cfg):
    d0, d1 = C.periode_complete(cfg)
    return 1.0 + 0.6 * (d - d0).days / max(1, (d1 - d0).days)


def tache_mois(args):
    cfg, y, m = args
    sc = C.scope(cfg)
    P, PX, W = _etat(cfg)
    d0 = C.periode_complete(cfg)[0]
    E = cfg["ecommerce"]
    sink = C.Sink(cfg, "ecommerce", f"{y}-{m:02d}")
    code, cat, w0 = P["code"].to_numpy(), P["cat"].to_numpy(), P["poids"].to_numpy()
    boost = np.array([{"electromenager": 40.0, "textile": 3.0}.get(c, 1.0) for c in cat])
    lib, ean, marque = P["lib"].to_list(), P["ean"].to_list(), P["marque"].to_list()
    tva_np, taille = P["tva"].to_numpy(), P["taille"].to_list()
    m_lanc = P["m_lanc"].to_numpy()
    last = (dt.date(y + (m == 12), 1 if m == 12 else m + 1, 1) - dt.timedelta(days=1))
    for d in C.jours(max(sc["debut"], dt.date(y, m, 1)), min(sc["fin"], last)):
        suf = f"annee={d.year}/mois={d.month:02d}"
        mi = (d.year - d0.year) * 12 + (d.month - d0.month)
        mult, catm, fl = R.facteurs_jour(d)
        mult = min(mult, 1.5) * croissance(d, cfg)
        ok = m_lanc <= mi
        K = int(np.searchsorted(W["ordre"].to_numpy(), d.toordinal(), side="right"))
        uids = W["uid"].to_numpy()
        # --- commandes ---
        rel = f"ecommerce/commandes/{suf}/commandes_{d.isoformat()}.jsonl"
        if not C.deja_fait(C.raw(cfg, rel), cfg):
            rng = C.rng_for(cfg, "cmd", d.toordinal())
            n = int(rng.poisson(E["commandes_jour"] * mult))
            wt = w0 * boost * ok * np.array([catm[c] for c in cat]); cdf = np.cumsum(wt); cdf /= cdf[-1]
            lines = []
            for i in range(n):
                nl = int(min(12, 1 + rng.geometric(0.35)))
                arts = np.unique(np.minimum(np.searchsorted(cdf, rng.random(nl)), len(cdf) - 1))
                ls, tot_ht, tot_ttc = [], 0, 0
                for a in arts:
                    q = int(rng.choice([1, 1, 1, 2, 3])) if cat[a] != "electromenager" else 1
                    pu = int(PX["PV"][mi][a]); ht = pu * q; ttc = int(round(ht * (100 + int(tva_np[a])) / 100))
                    tot_ht += ht; tot_ttc += ttc
                    ls.append({"art": int(code[a]), "qte": q, "pu_ht_mil": pu, "tva": int(tva_np[a])})
                dom = bool(rng.random() < 0.7)
                frais = 7000 if (dom and tot_ttc < 150000) else 0
                u = uids[min(int(K * rng.random() ** 1.3), K - 1)] if (K > 0 and rng.random() < 0.8) else None
                s = int(rng.integers(8 * 3600, 23 * 3600 + 3000))
                lines.append(json.dumps({"cmd_id": f"WEB{d.strftime('%y%m%d')}{i + 1:05d}", "uid": u, "dt_cmd": f"{d.isoformat()}T{s // 3600:02d}:{s % 3600 // 60:02d}:{s % 60:02d}",
                                         "canal": ["web", "web", "app_android", "app_ios"][int(rng.integers(0, 4))], "statut": STATUTS[int(rng.integers(0, 10))], "lignes": ls,
                                         "livraison": {"mode": "domicile" if dom else "retrait_magasin", "mg": None if dom else f"magasin_{int(rng.integers(1, 21)):02d}", "frais_mil": frais,
                                                       "creneau": ["matin", "apres_midi", "soir"][int(rng.integers(0, 3))]},
                                         "paiement": {"mode": ["CB", "paiement_livraison", "e-dinar", "virement"][int(rng.choice(4, p=[0.45, 0.4, 0.12, 0.03]))]},
                                         "mt_ht_mil": tot_ht, "mt_ttc_mil": tot_ttc + frais}, ensure_ascii=False))
            C.sortie_texte(cfg, sink, rel, "ecommerce/commandes", ("\n".join(lines) + "\n") if lines else "", len(lines))
        # --- clickstream ---
        rel = f"ecommerce/clickstream/{suf}/clics_{d.isoformat()}.jsonl"
        if not C.deja_fait(C.raw(cfg, rel), cfg):
            rng = C.rng_for(cfg, "clic", d.toordinal())
            n = int(rng.poisson(E["evenements_jour"] * mult))
            h = np.clip(rng.normal(np.where(rng.random(n) < 0.6, 21.0, 13.0), 2.5), 0, 23.99)
            t = np.sort((h * 3600).astype(np.int64))
            ts = np.datetime64(d.isoformat(), "s") + t.astype("timedelta64[s]")
            nsess = max(1, n // 6)
            ev = rng.choice(len(EVT), n, p=PEVT)
            wt = w0 * ok; cdf = np.cumsum(wt); cdf /= cdf[-1]
            art = np.minimum(np.searchsorted(cdf, rng.random(n)), len(cdf) - 1)
            item = np.isin(ev, [1, 3, 4, 7])
            ua = rng.integers(0, len(UA), nsess)
            sid = rng.integers(0, nsess, n)
            conn = (rng.random(nsess) < 0.4) & (K > 0)
            uu = np.array([uids[min(int(K * x ** 1.3), K - 1)] if (K > 0 and c) else None for x, c in zip(rng.random(nsess), conn)], dtype=object)
            df = pl.DataFrame({
                "ts": np.char.add(np.datetime_as_string(ts, unit="s"), "Z"), "sid": np.char.add("s-", np.char.zfill(sid.astype(str), 6)),
                "uid": pl.Series([uu[s] for s in sid], dtype=pl.Utf8), "evt": np.array(EVT)[ev],
                "page": np.where(item, np.char.add("/produit/", code[art].astype(str)), np.where(ev == 2, "/recherche", np.where(ev == 5, "/panier/validation", "/accueil"))),
                "art": pl.Series([int(code[a]) if it else None for a, it in zip(art, item)], dtype=pl.Int64),
                "q": pl.Series([REQ[int(x)] if e == 2 else None for x, e in zip(rng.integers(0, len(REQ), n), ev)], dtype=pl.Utf8),
                "ua": np.array(UA)[ua[sid]], "ref": np.array(REF)[rng.integers(0, len(REF), n)],
                "ip": np.char.add(np.char.add(np.array(["41.", "197.", "196."])[rng.integers(0, 3, n)], rng.integers(0, 255, n).astype(str)), np.char.add(".", rng.integers(0, 255, n).astype(str))),
                "lat_ms": rng.integers(40, 2500, n), "viewport": np.array(["390x844", "412x915", "1366x768", "1920x1080"])[rng.integers(0, 4, n)]})
            C.sortie_texte(cfg, sink, rel, "ecommerce/clickstream", df.write_ndjson(), n)
        # --- catalogue API (snapshot tous les N jours) ---
        if (d - d0).days % int(E["catalogue_pas_jours"]) == 0:
            rel = f"ecommerce/catalogue_api/{suf}/catalogue_{d.isoformat()}.json"
            if not C.deja_fait(C.raw(cfg, rel), cfg):
                rng = C.rng_for(cfg, "cat", d.toordinal())
                items = []
                for i in np.flatnonzero(ok):
                    ra = RAYON[cat[i]]
                    items.append({"sku": int(code[i]), "ean13": ean[i], "titre": lib[i], "marque": marque[i],
                                  "rayon": ra[int(rng.integers(0, len(ra)))] if rng.random() < 0.7 else None,
                                  "prix": {"ht_mil": int(PX["PV"][mi][i]), "ttc_mil": int(PX["TTC"][mi][i]), "devise": "TND"},
                                  "dispo": {"en_stock": bool(rng.random() < 0.93), "qte": int(rng.integers(0, 400))},
                                  "attributs": {"conditionnement": taille[i], "tva": int(tva_np[i])},
                                  "images": [f"https://cdn.exemple.tn/img/{code[i]}_{k}.jpg" for k in range(int(rng.integers(1, 4)))],
                                  "description": f"{lib[i].capitalize()} — livraison à domicile ou retrait en magasin. Référence {code[i]}.", "note_moy": round(float(rng.uniform(3, 5)), 1),
                                  "nb_avis": int(rng.integers(0, 300))})
                C.sortie_texte(cfg, sink, rel, "ecommerce/catalogue_api", json.dumps({"genere_le": d.isoformat(), "version_api": "v2", "total": len(items), "articles": items}, ensure_ascii=False), len(items))
    sink.close()
    return 1


def run(cfg):
    sc = C.scope(cfg)
    C.run_pool(tache_mois, [(cfg, y, m) for (y, m) in C.mois_de(sc["debut"], sc["fin"])], int(cfg["max_workers"]), "ecommerce")
    return {}
