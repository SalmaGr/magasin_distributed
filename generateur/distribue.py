#!/usr/bin/env python3
"""
distribue.py — Génération RAW DISTRIBUÉE : N conteneurs « générateurs » se partagent les tâches et écrivent
directement dans le stockage objet (MinIO distribué, protocole S3).

    python distribue.py plan     --shards 4              # répartition des tâches par shard
    python distribue.py gen      --shard 0/4 [--workers 2]   # génère sa part + téléverse (le shard 0 produit aussi les référentiels)
    python distribue.py finalize                         # agrège journal des anomalies + rapport (dans le store)

Principe : tâche = (magasin, mois) | mois | …  ·  tâche k -> shard (k % N)  ·  graine par (magasin, jour) => résultat
identique à la génération mono-nœud.  Reprenable : un marqueur _meta/_done/<type>/<tâche> est posé APRÈS le téléversement.
Stockage : S3 (S3_ENDPOINT, S3_BUCKET, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY) ou dossier local (--store-dir, pour tests).
"""
import argparse
import copy
import multiprocessing as mp
import os
import shutil
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import common as C
import run_all as RA
import src_avis, src_documents, src_ecommerce, src_ventes


# ────────────────────────── Stockage objet ──────────────────────────
class LocalStore:
    """Dossier = bucket simulé (tests sans S3)."""
    def __init__(self, root): self.root = root

    def _p(self, key): return os.path.join(self.root, key)

    def put(self, local, key):
        p = self._p(key); os.makedirs(os.path.dirname(p), exist_ok=True)
        shutil.copyfile(local, p + ".tmp"); os.replace(p + ".tmp", p)

    def put_bytes(self, data, key):
        p = self._p(key); os.makedirs(os.path.dirname(p), exist_ok=True)
        open(p, "wb").write(data)

    def exists(self, key): return os.path.exists(self._p(key))

    def get(self, key, local):
        os.makedirs(os.path.dirname(local), exist_ok=True); shutil.copyfile(self._p(key), local)

    def list(self, prefix):
        base = self._p(prefix)
        out = []
        for dp, _, fn in os.walk(base):
            out += [os.path.relpath(os.path.join(dp, f), self.root) for f in fn]
        return sorted(out)


class S3Store:
    def __init__(self, endpoint, bucket, key=None, secret=None):
        import boto3
        from botocore.config import Config
        self.bucket = bucket
        self.c = boto3.client("s3", endpoint_url=endpoint, aws_access_key_id=key, aws_secret_access_key=secret, region_name="us-east-1",
                              config=Config(signature_version="s3v4", s3={"addressing_style": "path"}, retries={"max_attempts": 8}))

    def put(self, local, key): self.c.upload_file(local, self.bucket, key)

    def put_bytes(self, data, key): self.c.put_object(Bucket=self.bucket, Key=key, Body=data)

    def exists(self, key):
        from botocore.exceptions import ClientError
        try:
            self.c.head_object(Bucket=self.bucket, Key=key); return True
        except ClientError as e:
            if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"): return False
            raise

    def get(self, key, local):
        os.makedirs(os.path.dirname(local), exist_ok=True); self.c.download_file(self.bucket, key, local)

    def list(self, prefix):
        out = []
        for pg in self.c.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=prefix):
            out += [o["Key"] for o in pg.get("Contents", [])]
        return out


def make_store(spec):
    if spec["kind"] == "local":
        return LocalStore(spec["root"])
    s = S3Store(spec["endpoint"], spec["bucket"], spec.get("key"), spec.get("secret"))
    return s


def store_spec(a, cfg):
    if a.store_dir:
        return dict(kind="local", root=os.path.abspath(a.store_dir))
    bucket = os.environ.get("S3_BUCKET") or ("raw" if cfg["mode"] == "full" else "raw-echantillon")
    return dict(kind="s3", endpoint=os.environ.get("S3_ENDPOINT", "http://minio:9000"), bucket=bucket,
                key=os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"), secret=os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin123"))


def televerser(store, root, ignorer=("_meta/_etat",)):
    """Téléverse tout l'arbre local (clé = chemin relatif). Retourne (fichiers, octets)."""
    n = b = 0
    for dp, dn, fn in os.walk(root):                   # os.walk ne suit pas les symlinks (le lien _etat est ignoré)
        for f in fn:
            loc = os.path.join(dp, f); key = os.path.relpath(loc, root).replace(os.sep, "/")
            if any(key.startswith(i) for i in ignorer) or key.endswith(".tmp"):
                continue
            store.put(loc, key); n += 1; b += os.path.getsize(loc)
    return n, b


# ────────────────────────── Tâches ──────────────────────────
def liste_taches(cfg):
    sc = C.scope(cfg)
    mois = C.mois_de(sc["debut"], sc["fin"])
    T = []
    for mid in sc["magasins"]:
        for y, m in mois:
            T.append(("ventes", f"{mid}_{y}-{m:02d}", (mid, y, m)))
            T.append(("fac", f"fac_{mid}_{y}-{m:02d}", (mid, y, m)))
    for y, m in mois:
        T.append(("ecommerce", f"{y}-{m:02d}", (y, m)))
        T.append(("avis", f"{y}-{m:02d}", (y, m)))
    T.append(("pdf", "pdf", ()))
    return T


def executer(arg):
    """Exécute UNE tâche dans un dossier de travail propre, téléverse, pose le marqueur, nettoie."""
    cfg, spec, kind, taskid, targs, etat_dir, scratch = arg
    store = make_store(spec)
    done = f"_meta/_done/{kind}/{taskid}"
    if store.exists(done):
        return kind, taskid, "déjà fait", 0, 0.0
    t0 = time.time()
    root = os.path.join(scratch, f"{kind}_{taskid}")
    shutil.rmtree(root, ignore_errors=True)
    os.makedirs(os.path.join(root, "_meta"))
    os.symlink(etat_dir, os.path.join(root, "_meta", "_etat"))
    c = dict(cfg); c["_raw"] = root; c["max_workers"] = 1
    try:
        if kind == "ventes": src_ventes.tache_mois((c,) + targs)
        elif kind == "fac": src_documents.tache_factures((c,) + targs)
        elif kind == "ecommerce": src_ecommerce.tache_mois((c,) + targs)
        elif kind == "avis": src_avis.tache_mois((c,) + targs)
        elif kind == "pdf": src_documents.tache_pdf(c)
        n, b = televerser(store, root)
        store.put_bytes(b"ok", done)                   # marqueur APRÈS téléversement : reprise sûre
    finally:
        shutil.rmtree(root, ignore_errors=True)
    return kind, taskid, "ok", n, time.time() - t0


# ────────────────────────── Commandes ──────────────────────────
def cmd_plan(cfg, a):
    T = liste_taches(cfg)
    print(f"{len(T)} tâches pour {a.shards} shards  (mode {cfg['mode']}, {C.scope(cfg)['debut']} → {C.scope(cfg)['fin']})")
    for s in range(a.shards):
        mine = [t for i, t in enumerate(T) if i % a.shards == s]
        k = {}
        for t in mine: k[t[0]] = k.get(t[0], 0) + 1
        print(f"  shard {s}/{a.shards}: {len(mine):4d} tâches  {k}")


def attendre_etat(store, attente):
    t0 = time.time()
    while not store.exists("_meta/_etat/_READY"):
        if time.time() - t0 > attente:
            sys.exit("Référentiels (shard 0) introuvables : abandon")
        print("   … attente des référentiels du shard 0", flush=True); time.sleep(5)


def cmd_gen(cfg, a, spec):
    sh, N = (int(x) for x in a.shard.split("/"))
    store = make_store(spec)
    scratch = os.path.abspath(a.scratch)
    os.makedirs(scratch, exist_ok=True)
    etat_dir = os.path.join(scratch, "etat")
    print(f"Shard {sh}/{N} · store={spec['kind']}:{spec.get('bucket') or spec.get('root')} · workers={a.workers}", flush=True)
    if sh == 0 and not store.exists("_meta/_etat/_READY"):
        print("▶ référentiels (shard 0)", flush=True)
        rc = dict(cfg); rc["_raw"] = os.path.join(scratch, "ref")
        shutil.rmtree(rc["_raw"], ignore_errors=True)
        for nom, fn in RA.ETAPES_REF:
            t = time.time(); r = fn(rc); print(f"  {nom}: {r} ({time.time() - t:.0f}s)", flush=True)
        n, b = televerser(store, rc["_raw"], ignorer=("_meta/_etat",))
        for f in os.listdir(os.path.join(rc["_raw"], "_meta", "_etat")):
            store.put(os.path.join(rc["_raw"], "_meta", "_etat", f), f"_meta/_etat/{f}")
        store.put_bytes(b"ok", "_meta/_etat/_READY")
        shutil.rmtree(etat_dir, ignore_errors=True)
        shutil.copytree(os.path.join(rc["_raw"], "_meta", "_etat"), etat_dir)
        for d in os.listdir(rc["_raw"]):
            if d != "_meta": shutil.rmtree(os.path.join(rc["_raw"], d), ignore_errors=True)
        print(f"  référentiels téléversés ({n} fichiers, {b / 1e6:.0f} Mo)", flush=True)
    else:
        attendre_etat(store, a.attente)
        os.makedirs(etat_dir, exist_ok=True)
        for k in store.list("_meta/_etat/"):
            if not k.endswith("_READY"): store.get(k, os.path.join(etat_dir, os.path.basename(k)))
    T = [t for i, t in enumerate(liste_taches(cfg)) if i % N == sh]
    print(f"▶ {len(T)} tâches à traiter", flush=True)
    args = [(cfg, spec, k, tid, ta, etat_dir, scratch) for k, tid, ta in T]
    ko, t0 = [], time.time()
    ctx = mp.get_context("spawn")                          # jamais fork (Polars)
    with ctx.Pool(max(1, a.workers)) as pool:
        it = pool.imap_unordered(executer, args, chunksize=1) if a.workers > 1 else map(executer, args)
        for i, r in enumerate(it, 1):
            print(f"  [{i}/{len(args)}] {r[0]} {r[1]} {r[2]} ({r[3]} fichiers, {r[4]:.0f}s)", flush=True)
    print(f"✔ shard {sh}/{N} terminé en {time.time() - t0:.0f}s", flush=True)


def cmd_finalize(cfg, a, spec):
    store = make_store(spec)
    scratch = os.path.abspath(a.scratch); fin = os.path.join(scratch, "fin")
    shutil.rmtree(fin, ignore_errors=True)
    for pref in ("_meta/_parts/", "_meta/_verite/"):
        for k in store.list(pref):
            store.get(k, os.path.join(fin, k))
    done = [k for k in store.list("_meta/_done/")]
    T = liste_taches(cfg)
    print(f"tâches terminées : {len(done)}/{len(T)}")
    manq = {f"_meta/_done/{k}/{t}" for k, t, _ in T} - set(done)
    if manq:
        print(f"⚠ {len(manq)} tâches manquantes (relancer les shards concernés), ex. {sorted(manq)[:3]}")
    fc = dict(cfg); fc["_raw"] = fin; fc["_distribue"] = True
    rap = RA.rapport(fc, {})
    for f in ("rapport_generation.json", "journal_anomalies.csv", "verite_terrain.json"):
        store.put(os.path.join(fin, "_meta", f), f"_meta/{f}")
    print(f"✔ rapport : {rap['taille_totale_go']} Go · {rap['nombre_fichiers']} fichiers · journal et vérité terrain publiés dans _meta/")
    sys.exit(1 if manq else 0)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("cmd", choices=["plan", "gen", "finalize"])
    ap.add_argument("--config", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.yaml"))
    ap.add_argument("--mode", choices=["sample", "full"], default=os.environ.get("GEN_MODE"))
    ap.add_argument("--shard", default="0/1"); ap.add_argument("--shards", type=int, default=4)
    ap.add_argument("--workers", type=int, default=int(os.environ.get("GEN_WORKERS", 2)))
    ap.add_argument("--scratch", default=os.environ.get("GEN_SCRATCH", "/tmp/gen_scratch"))
    ap.add_argument("--store-dir"); ap.add_argument("--attente", type=int, default=7200)
    a = ap.parse_args()
    cfg = C.load_config(a.config, a.mode, a.scratch)
    if a.cmd == "plan": return cmd_plan(cfg, a)
    spec = store_spec(a, cfg)
    cmd_gen(cfg, a, spec) if a.cmd == "gen" else cmd_finalize(cfg, a, spec)


if __name__ == "__main__":
    main()
