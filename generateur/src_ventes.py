"""src_ventes.py — Ventes structurées (tickets, lignes, paiements : 1 fichier/magasin/jour), mouvements de stock,
logs POS JSONL. Vectorisé NumPy/Polars ; tâche = (magasin, mois) ; reprenable ; graine par (magasin, jour)."""
import datetime as dt
import os
import tempfile
import time
import zlib

import numpy as np
import polars as pl

import common as C
import referentiels as R

_ST = {}
MODES = ["ESP", "CB", "TR", "CHQ", "AUT", "MIXTE"]
BINS = np.array(["497861", "540133", "406573", "521180", "459120"])
VER = "4.2.1"


def _etat(cfg):
    k = os.path.realpath(C.etat_path(cfg, ""))
    if k in _ST:
        return _ST[k]
    P, PX, PR = C.charger_etat(cfg, "produits.parquet"), C.charger_etat(cfg, "prix.npz"), C.charger_etat(cfg, "promos.parquet")
    FID, EMP, MAG = C.charger_etat(cfg, "fid.parquet"), C.charger_etat(cfg, "employes.parquet"), C.charger_etat(cfg, "magasins.parquet")
    cat_i = np.array([R.CATEGORIES.index(c) for c in P["cat"]])
    st = dict(
        code=P["code"].to_numpy(), cat_i=cat_i, tva=P["tva"].to_numpy(), poids=P["poids"].to_numpy(), m_lanc=P["m_lanc"].to_numpy(),
        PV=PX["PV"], PA=PX["PA"], p_deb=PR["dt_deb"].dt.ordinal_day().to_numpy() * 0 + np.array([d.toordinal() for d in PR["dt_deb"]]),
        p_fin=np.array([d.toordinal() for d in PR["dt_fin"]]), p_art=(PR["art"].to_numpy() - 1000001), p_tx=PR["tx_rem"].to_numpy(),
        fid_ids=FID["fid_id"].to_numpy(), fid_ord=FID["ordre"].to_numpy(),
        caissiers={m: EMP.filter((pl.col("mg") == m) & pl.col("permanent"))["mat"].to_numpy() for m in MAG["id"]},
        mag={r["id"]: r for r in MAG.iter_rows(named=True)}, d0=C.periode_complete(cfg)[0])
    _ST[k] = st
    return st


def _secondes(rng, n, h0, h1, fl, d):
    if fl["ramadan"]:
        comp, pw = [(15.5, 1.2), (21.0, 1.4), (12.0, 2.0)], [0.35, 0.50, 0.15]
        h1 = min(h1 + 1, 23)
    elif d.weekday() >= 5:
        comp, pw = [(11.5, 1.8), (16.5, 2.0)], [0.35, 0.65]
    else:
        comp, pw = [(11.0, 1.3), (14.5, 1.5), (18.0, 1.6)], [0.30, 0.20, 0.50]
    k = rng.choice(len(comp), n, p=pw)
    mu = np.array([c[0] for c in comp])[k]; sg = np.array([c[1] for c in comp])[k]
    h = rng.normal(mu, sg)
    hors = (h < h0) | (h > h1 - 0.1)                       # troncature : on retire dans la plage d'ouverture
    h[hors] = rng.uniform(h0 + 0.5, h1 - 0.2, int(hors.sum()))
    return np.sort((h * 3600 + rng.random(n) * 60).astype(np.int64)), h1


def jour_magasin(cfg, mid, d):
    """Génère tous les tableaux d'un (magasin, jour). Retourne dict de DataFrames + matériel pour les logs."""
    S = _etat(cfg)
    mg = S["mag"][mid]
    rng = C.rng_for(cfg, "ventes", mid, d.toordinal())
    mult, catm, fl = R.facteurs_jour(d)
    mi = (d.year - S["d0"].year) * 12 + (d.month - S["d0"].month)
    jit = 0.92 + (zlib.crc32(mid.encode()) % 17) / 100.0
    mu = mg["tickets_jour"] * mult * (R.mult_touristique(d) if mg["tourisme"] else 1.0) * jit
    n = max(30, int(rng.poisson(mu)))
    # paniers
    nl = np.clip(np.rint(rng.lognormal(np.log(mg["lignes_moy"]) - 0.245, 0.7, n)), 1, 40).astype(np.int64)
    L = int(nl.sum())
    cw = np.array([catm[c] * R.AFFINITE[mg["format"]].get(c, 1.0) for c in R.CATEGORIES])
    w = S["poids"] * cw[S["cat_i"]] * (S["m_lanc"] <= mi)
    cdf = np.cumsum(w); cdf /= cdf[-1]
    art = np.minimum(np.searchsorted(cdf, rng.random(L)), len(w) - 1)
    tk = np.repeat(np.arange(n), nl)
    key = tk * len(w) + art
    _, first = np.unique(key, return_index=True)
    first.sort()
    tk, art = tk[first], art[first]
    nl = np.bincount(tk, minlength=n)
    starts = np.cumsum(nl) - nl
    nlig = np.arange(len(tk)) - np.repeat(starts, nl) + 1
    q = rng.choice(np.arange(1, 7), len(tk), p=[0.62, 0.22, 0.08, 0.04, 0.02, 0.02])
    ci = S["cat_i"][art]
    q = np.where(ci == R.CATEGORIES.index("electromenager"), 1, np.where((ci == R.CATEGORIES.index("textile")) & (q > 3), 3, q))
    pu = C.arrondi5(S["PV"][mi][art] * mg["coef_prix"])
    tx = np.zeros(len(w), dtype=np.int64)
    act = (S["p_deb"] <= d.toordinal()) & (S["p_fin"] >= d.toordinal())
    np.maximum.at(tx, S["p_art"][act], S["p_tx"][act])
    rem = np.maximum(tx[art], np.where(rng.random(len(tk)) < 0.02, rng.choice([5, 10, 15], len(tk)), 0))
    mt_ht = np.rint(q * pu * (100 - rem) / 100.0).astype(np.int64)
    tva = S["tva"][art]
    mt_ttc = np.rint(mt_ht * (100 + tva) / 100.0).astype(np.int64)
    t_ht = np.bincount(tk, weights=mt_ht, minlength=n).astype(np.int64)
    t_ttc = np.bincount(tk, weights=mt_ttc, minlength=n).astype(np.int64)
    t_art = np.bincount(tk, weights=q, minlength=n).astype(np.int64)
    # tickets
    secs, h1 = _secondes(rng, n, mg["h0"], mg["h1"], fl, d)
    ts = np.datetime64(d.isoformat(), "s") + secs.astype("timedelta64[s]")
    dtv = np.char.replace(np.datetime_as_string(ts, unit="s"), "T", " ")
    seq = np.arange(1, n + 1)
    tk_id = np.char.add(f"TK-{mid}-{d:%Y%m%d}-", np.char.zfill(seq.astype(str), 6))          # ex. TK-magasin_01-20250314-000123
    caisse = rng.integers(1, mg["caisses"] + 1, n)
    cais = S["caissiers"][mid]
    cmap = rng.choice(cais, mg["caisses"], replace=len(cais) < mg["caisses"])
    caissier = cmap[caisse - 1]
    K = int(np.searchsorted(S["fid_ord"], d.toordinal(), side="right"))
    if K > 0:
        a_carte = rng.random(n) < cfg["volumes"]["part_tickets_carte"]
        cli = np.where(a_carte, S["fid_ids"][np.minimum((K * rng.random(n) ** 1.6).astype(int), K - 1)], None)
    else:
        a_carte, cli = np.zeros(n, bool), np.full(n, None, dtype=object)
    pc = np.array([cfg["paiements"][m] for m in MODES]); pc = pc / pc.sum()
    mp = rng.choice(len(MODES), n, p=pc)
    evo = cfg["evolutions"]
    col_mp = "mode_paiement" if d >= C.to_date(evo["tickets_renomme_mp"]) else "mp"
    tickets = pl.DataFrame({"tk_id": tk_id, "mg": mid, "dt_v": dtv, "caisse": caisse, "caissier": caissier.astype(str), "cli": pl.Series([None if x is None else str(x) for x in cli], dtype=pl.Utf8),
                            "nb_art": t_art, "mt_ht": t_ht, "mt_tva": t_ttc - t_ht, "mt_ttc": t_ttc, col_mp: np.array(MODES)[mp]})
    # lignes
    ld = {"tk_id": tk_id[tk], "n_lig": nlig, "art": S["code"][art], "qte": q, "pu_ht": pu, "rem": rem, "mt_ht": mt_ht, "tva": tva, "mt_ttc": mt_ttc}
    if d >= C.to_date(evo["lignes_ajout_pt_fid"]):
        ld["pt_fid"] = pl.Series([int(v // 1000) if a else None for v, a in zip(mt_ttc, a_carte[tk])], dtype=pl.Int64)
    lignes = pl.DataFrame(ld)
    # paiements
    mix = mp == MODES.index("MIXTE")
    frac = rng.uniform(0.3, 0.7, n)
    m1 = np.where(mix, C.arrondi5(t_ttc * frac), t_ttc)
    mode1 = np.where(mix, np.where(rng.random(n) < 0.5, "ESP", "TR"), np.array(MODES)[mp])
    pay = [pl.DataFrame({"tk_id": tk_id, "seq": 1, "mp": mode1, "mt": m1})]
    if mix.any():
        pay.append(pl.DataFrame({"tk_id": tk_id[mix], "seq": 2, "mp": "CB", "mt": t_ttc[mix] - m1[mix]}))
    paie = pl.concat(pay).sort(["tk_id", "seq"])
    cb = (paie["mp"] == "CB").to_numpy()
    k = int(cb.sum())
    msk = np.full(paie.height, None, dtype=object); au = np.full(paie.height, None, dtype=object)
    if k:
        msk[cb] = np.char.add(np.char.add(rng.choice(BINS, k), "******"), np.char.zfill(rng.integers(0, 10000, k).astype(str), 4))
        au[cb] = np.char.zfill(rng.integers(0, 10 ** 6, k).astype(str), 6)
    paie = paie.with_columns(pl.Series("carte_msk", [None if x is None else str(x) for x in msk], dtype=pl.Utf8),
                             pl.Series("auth", [None if x is None else str(x) for x in au], dtype=pl.Utf8))
    # stock : sorties agrégées + réceptions les jours de livraison + casse
    stock = None
    if cfg["stock"]["actif"]:
        qs = np.bincount(art, weights=q, minlength=len(w)).astype(np.int64)
        ia = np.flatnonzero(qs)
        rows = [("S", ia, qs[ia])]
        cyc = {"hypermarche": (1, None), "supermarche": (2, (0, 2, 4, 5)), "proximite": (3.5, (1, 4))}[mg["format"]]
        if cyc[1] is None or d.weekday() in cyc[1]:
            sel = ia[rng.random(len(ia)) < 0.6]
            rows.append(("E", sel, np.maximum(6, np.rint(qs[sel] * cyc[0] * rng.uniform(0.9, 1.3, len(sel)))).astype(np.int64)))
        cas = ia[rng.random(len(ia)) < 0.01]
        rows.append(("C", cas, rng.integers(1, 4, len(cas))))
        stock = pl.DataFrame({"mg": mid, "dt_mv": d.isoformat(), "art": np.concatenate([S["code"][r[1]] for r in rows]),
                              "typ": np.concatenate([np.full(len(r[1]), r[0]) for r in rows]), "qte": np.concatenate([r[2] for r in rows]),
                              "val_pa": np.concatenate([r[2] * S["PA"][mi][r[1]] for r in rows])})
    brut = dict(n=n, secs=secs, tk_id=tk_id, caisse=caisse, caissier=caissier, nb_lig=nl, t_ttc=t_ttc, mp=np.array(MODES)[mp], cli=cli, h1=h1,
                art=art, nlig=nlig, tk=tk, q=q, pu=pu, code=S["code"][art], h0=mg["h0"])
    return tickets, lignes, paie, stock, brut


def lignes_logs(cfg, mid, d, b):
    """JSONL POS : 1 événement TICKET_CLOSE/ticket + détail (SCAN/PAY) sur une part des tickets + erreurs verbeuses + heartbeats."""
    L = cfg["pos_logs"]
    rng = C.rng_for(cfg, "logs", mid, d.toordinal())
    n = b["n"]
    base = np.datetime64(d.isoformat(), "s")
    iso = lambda s, ms=0: f"{np.datetime_as_string(base + np.timedelta64(int(s), 's'), unit='s')}.{ms:03d}+01:00"
    out = []
    caisses = b["caisse"].tolist(); cais = b["caissier"].tolist(); tkid = b["tk_id"].tolist(); nlg = b["nb_lig"].tolist()
    ttc = b["t_ttc"].tolist(); mp = b["mp"].tolist(); cli = b["cli"].tolist(); secs = b["secs"].tolist()
    durs = rng.integers(40_000, 420_000, n).tolist()
    det = np.flatnonzero(rng.random(n) < L["detail_ratio"]).tolist()
    detset = set(det)
    starts = (np.cumsum(b["nb_lig"]) - b["nb_lig"]).tolist()
    for i in range(n):
        s, ms = secs[i], int(rng.integers(0, 1000))
        c = f'"{cli[i]}"' if cli[i] else "null"
        out.append((s, f'{{"ts":"{iso(s, ms)}","lvl":"INFO","app":"pos-core","ver":"{VER}","host":"POS-{mid}-{caisses[i]:02d}","mg":"{mid}","caisse":{caisses[i]},'
                       f'"caissier":"{cais[i]}","evt":"TICKET_CLOSE","tk":"{tkid[i]}","nb_lig":{nlg[i]},"mt_ttc_mil":{ttc[i]},"mode_paiement":"{mp[i]}","cli":{c},'
                       f'"duree_ms":{durs[i]},"sess":"{int(rng.integers(0, 2**31)):08x}","net":{{"lat_ms":{int(rng.integers(2, 90))},"retries":0}}}}'))
        if i in detset:
            s0 = max(b["h0"] * 3600, s - durs[i] // 1000)
            out.append((s0, f'{{"ts":"{iso(s0)}","lvl":"DEBUG","app":"pos-core","ver":"{VER}","host":"POS-{mid}-{caisses[i]:02d}","evt":"TICKET_OPEN","tk":"{tkid[i]}","caissier":"{cais[i]}","mode":"scan"}}'))
            for j in range(starts[i], starts[i] + nlg[i]):
                sj = s0 + 1 + (j - starts[i])
                out.append((sj, f'{{"ts":"{iso(sj, int(rng.integers(0, 1000)))}","lvl":"DEBUG","app":"pos-scanner","ver":"{VER}","host":"POS-{mid}-{caisses[i]:02d}","evt":"SCAN","tk":"{tkid[i]}",'
                                f'"n_lig":{int(b["nlig"][j])},"art":{int(b["code"][j])},"qte":{int(b["q"][j])},"pu_ht_mil":{int(b["pu"][j])},"scan_ms":{int(rng.integers(8, 400))},"src":"laser","qualite":{rng.uniform(0.6, 1):.3f}}}'))
            out.append((s, f'{{"ts":"{iso(s, 0)}","lvl":"DEBUG","app":"pos-paiement","ver":"{VER}","host":"POS-{mid}-{caisses[i]:02d}","evt":"PAY_OK","tk":"{tkid[i]}","mode":"{mp[i]}","mt_mil":{ttc[i]},"tpe":"TPE-{int(rng.integers(1, 30)):03d}"}}'))
    MSG = ["Papier imprimante presque épuisé (niveau {x}%) — remplacer le rouleau thermique avant la prochaine impression de ticket",
           "Délai dépassé lors de la lecture du code-barres, nouvelle tentative {x}/3 sur le scanner laser principal du poste de caisse",
           "Latence élevée du terminal de paiement électronique ({x} ms) lors de la demande d'autorisation vers le serveur bancaire",
           "File de synchronisation vers le serveur central en retard : {x} tickets en attente d'envoi, reprise automatique programmée"]
    for i in np.flatnonzero(rng.random(n) < L["erreurs_ratio"]).tolist():
        t = int(rng.integers(0, 4))
        out.append((secs[i], f'{{"ts":"{iso(secs[i], 5)}","lvl":"{"ERROR" if t in (1, 3) else "WARN"}","app":"pos-core","ver":"{VER}","host":"POS-{mid}-{caisses[i]:02d}","evt":"INCIDENT",'
                             f'"code":"E{int(rng.integers(100, 999))}","msg":"{MSG[t].format(x=int(rng.integers(2, 900)))}","tk":"{tkid[i]}","trace":"pos.hw.{["printer", "scanner", "tpe", "sync"][t]}.Handler.run(Handler.java:{int(rng.integers(40, 900))})"}}'))
    pas = max(1, int(L["heartbeat_min"])) * 60
    for c in range(1, int(max(caisses)) + 1):
        for s in range(int(b["h0"]) * 3600 - 1800, int(b["h1"]) * 3600, pas):
            out.append((s, f'{{"ts":"{iso(s)}","lvl":"INFO","app":"pos-agent","ver":"{VER}","host":"POS-{mid}-{c:02d}","evt":"HEARTBEAT","cpu_pct":{int(rng.integers(3, 70))},'
                           f'"mem_mb":{int(rng.integers(300, 1800))},"queue":{int(rng.integers(0, 6))},"printer":"OK","scanner":"OK","tpe":"OK"}}'))
    out.sort(key=lambda x: x[0])
    return [x[1] for x in out]


SPEC_T = {"nulls": ["caissier", "caisse"], "dates": ["dt_v"], "orphelins": {"cli": lambda r, k: np.array([f"FID9{int(x):06d}" for x in r.integers(0, 999999, k)])}}
SPEC_L = {"nulls": ["rem"], "aberr": {"pu_ht": [10, 100], "qte": [100]},
          "orphelins": {"art": lambda r, k: 8000000 + r.integers(0, 999999, k)}}
SPEC_P = {"nulls": ["auth"]}
SPEC_S = {"nulls": ["val_pa"], "dates": ["dt_mv"]}


def _fmt(cols):
    return lambda x: x.with_columns([C.tnd(c) for c in cols if c in x.columns])


def tache_mois(args):
    cfg, mid, y, m = args
    sc = C.scope(cfg)
    sink = C.Sink(cfg, "ventes", f"{mid}_{y}-{m:02d}")
    n_f = 0
    for d in C.jours(max(sc["debut"], dt.date(y, m, 1)), min(sc["fin"], (dt.date(y + (m == 12), 1 if m == 12 else m + 1, 1) - dt.timedelta(days=1)))):
        suf = f"{mid}/annee={d.year}/mois={d.month:02d}"
        rels = {"tickets": f"ventes/tickets/{suf}/tickets_{mid}_{d.isoformat()}.csv", "lignes": f"ventes/lignes/{suf}/lignes_{mid}_{d.isoformat()}.csv",
                "paiements": f"ventes/paiements/{suf}/paiements_{mid}_{d.isoformat()}.csv", "stock": f"erp/stock/{suf}/stock_{mid}_{d.isoformat()}.csv",
                "pos": f"pos_logs/{suf}/pos_{d.isoformat()}.jsonl"}
        if not cfg["stock"]["actif"]:
            rels.pop("stock")
        manque = {k for k, r in rels.items() if not C.deja_fait(C.raw(cfg, r), cfg)}
        if not manque:
            continue
        T, Lg, Pa, St, b = jour_magasin(cfg, mid, d)
        r = lambda tag: C.rng_for(cfg, "defauts", tag, mid, d.toordinal())
        if "tickets" in manque: C.sortie_csv(cfg, sink, rels["tickets"], "ventes/tickets", T, SPEC_T, r("t"), _fmt(["mt_ht", "mt_tva", "mt_ttc"]))
        if "lignes" in manque: C.sortie_csv(cfg, sink, rels["lignes"], "ventes/lignes", Lg, SPEC_L, r("l"), _fmt(["pu_ht", "mt_ht", "mt_ttc"]))
        if "paiements" in manque: C.sortie_csv(cfg, sink, rels["paiements"], "ventes/paiements", Pa, SPEC_P, r("p"), _fmt(["mt"]))
        if "stock" in manque: C.sortie_csv(cfg, sink, rels["stock"], "erp/stock", St, SPEC_S, r("s"), _fmt(["val_pa"]))
        if "pos" in manque:
            lg = lignes_logs(cfg, mid, d, b)
            C.sortie_texte(cfg, sink, rels["pos"], "pos_logs", "\n".join(lg) + "\n", len(lg))
        n_f += 1
    sink.close()
    return n_f


def run(cfg):
    sc = C.scope(cfg)
    taches = [(cfg, mid, y, m) for mid in sc["magasins"] for (y, m) in C.mois_de(sc["debut"], sc["fin"])]
    C.run_pool(tache_mois, taches, int(cfg["max_workers"]), "ventes")
    return {"taches": len(taches)}


# ───────── estimation du volume ─────────
def mesurer(cfg, jours_test=None):
    """Mini-génération (3 magasins x 3 jours) dans un dossier temporaire -> octets par ticket et par format."""
    tmp = tempfile.mkdtemp(prefix="est_")
    c2 = dict(cfg); c2["_raw"] = tmp; c2["mode"] = "sample"
    base = C.raw(cfg, "_meta", "_etat")
    os.makedirs(os.path.join(tmp, "_meta"), exist_ok=True)
    if not os.path.exists(os.path.join(tmp, "_meta", "_etat")):
        os.symlink(base, os.path.join(tmp, "_meta", "_etat"))
    jours_test = jours_test or [dt.date(2025, 5, 14), dt.date(2025, 5, 17), dt.date(2025, 3, 12)]
    res = {}
    for mid in ("magasin_01", "magasin_03", "magasin_11"):
        fmt = _etat(c2)["mag"][mid]["format"]
        tk = b_ventes = b_logs = b_stock = 0
        t0 = time.time()
        sink = C.Sink(c2, "est", mid)
        for d in jours_test:
            suf = f"{mid}/annee={d.year}/mois={d.month:02d}"
            T, Lg, Pa, St, b = jour_magasin(c2, mid, d)
            tk += T.height
            for tag, df, spec, cols in (("ventes/tickets", T, SPEC_T, ["mt_ht", "mt_tva", "mt_ttc"]), ("ventes/lignes", Lg, SPEC_L, ["pu_ht", "mt_ht", "mt_ttc"]), ("ventes/paiements", Pa, SPEC_P, ["mt"])):
                b_ventes += C.sortie_csv(c2, sink, f"x/{tag}/{suf}/{d}.csv", tag, df, spec, C.rng_for(cfg, "est", tag, mid, d.toordinal()), _fmt(cols))
            if St is not None:
                b_stock += C.sortie_csv(c2, sink, f"x/stock/{suf}/{d}.csv", "erp/stock", St, SPEC_S, C.rng_for(cfg, "est", "s", mid, d.toordinal()), _fmt(["val_pa"]))
            lg = lignes_logs(c2, mid, d, b)
            b_logs += C.sortie_texte(c2, sink, f"x/pos/{suf}/{d}.jsonl", "pos_logs", "\n".join(lg) + "\n", len(lg))
        res[fmt] = dict(tickets=tk, ventes=b_ventes / tk, logs=b_logs / tk, stock=b_stock / tk, sec_par_ticket=(time.time() - t0) / tk)
    import shutil
    shutil.rmtree(tmp, ignore_errors=True)
    return res


def estimer(cfg, mes=None):
    """Extrapole : somme sur tous les (magasin, jour) du nombre attendu de tickets x octets/ticket mesurés."""
    mes = mes or mesurer(cfg)
    sc = C.scope(cfg)
    tot = dict(tickets=0, ventes=0.0, logs=0.0, stock=0.0, sec=0.0)
    for m in C.magasins_cfg(cfg):
        if m["id"] not in sc["magasins"]:
            continue
        jit = 0.92 + (zlib.crc32(m["id"].encode()) % 17) / 100.0
        t = sum(m["tickets_jour"] * R.facteurs_jour(d)[0] * (R.mult_touristique(d) if m["tourisme"] else 1.0) * jit for d in C.jours(sc["debut"], sc["fin"]))
        x = mes[m["format"]]
        tot["tickets"] += t; tot["ventes"] += t * x["ventes"]; tot["logs"] += t * x["logs"]; tot["stock"] += t * x["stock"]; tot["sec"] += t * x["sec_par_ticket"]
    return tot
