"""src_avis.py — Texte libre : avis réseaux sociaux (JSONL), formulaire du site (CSV note + niveau de satisfaction),
emails (JSONL), transcriptions d'appels (JSONL). Génération par grammaire à gabarits (français, quelques mots de darija)."""
import datetime as dt
import json

import numpy as np
import polars as pl

import common as C
import referentiels as R

ASPECTS = {
    "caisse": (["Peu d'attente aux caisses, le personnel est efficace", "Très rapide au passage en caisse, merci", "Les caisses ouvertes sont efficaces et souriantes"],
               ["Une seule caisse ouverte un samedi, c'est inadmissible", "30 minutes d'attente à la caisse, mouch normal", "File interminable et caissier désagréable"]),
    "prix": (["Les prix sont corrects et les promotions intéressantes", "Bon rapport qualité prix, behi barcha", "Prix compétitifs sur les produits de base"],
             ["Les prix ont encore augmenté, ghali barcha", "Écart entre le prix affiché en rayon et celui du ticket", "Trop cher par rapport aux autres enseignes"]),
    "rayon": (["Rayons propres et bien approvisionnés", "Large choix et produits frais", "Le rayon est bien organisé, on trouve tout facilement"],
              ["Rayon vide, encore une rupture de stock", "Produits périmés trouvés en rayon, à vérifier", "Rayon désordonné et produits mal rangés"]),
    "accueil": (["Accueil chaleureux, personnel aimable", "Le responsable du magasin a été très serviable", "Équipe disponible et à l'écoute, yaatikom essaha"],
                ["Personnel peu aimable et pas de réponse à ma question", "Accueil froid, personne pour renseigner", "Manque de respect de la part d'un employé"]),
    "proprete": (["Magasin propre et climatisé, agréable", "Parking facile et magasin impeccable"], ["Magasin sale, odeurs dans l'allée", "Parking saturé et chariots cassés"]),
    "livraison": (["Livraison à domicile dans les délais, colis bien emballé", "Commande en ligne reçue en 24h, très satisfait"],
                  ["Livraison en retard de deux jours, personne ne répond", "Colis abîmé et article manquant dans ma commande"]),
}
PROD_P = ["Le {p} est excellent, je le rachète à chaque fois", "J'adore le {p}, qualité au rendez-vous", "{p} : très bon produit, conforme à la description"]
PROD_N = ["Le {p} était de mauvaise qualité, je suis déçu", "Le {p} est arrivé abîmé, remboursement demandé", "Problème avec le {p} : date limite trop courte"]
PROD_U = ["J'ai acheté du {p}, rien de spécial à signaler", "Pris un {p} en promotion, correct sans plus"]
CONCL_P = ["Je recommande vivement !", "À refaire sans hésiter, inchallah", "Continuez comme ça 👍", "5 étoiles méritées ⭐⭐⭐⭐⭐", "Merci à toute l'équipe"]
CONCL_N = ["Je ne reviendrai pas.", "Service client à revoir d'urgence 😡", "Décevant, j'attends une réponse de la direction.", "À éviter pour le moment…", "Mouch behi du tout."]
CONCL_U = ["Moyen, peut mieux faire.", "Ça passe, sans plus.", "On verra la prochaine fois."]
PLATEF = ["facebook", "instagram", "x", "google_maps", "tiktok"]
PSEUDO = ["Sami", "Nour", "Amine", "Rim", "Yassine", "Hela", "Karim", "Meriem", "Walid", "Ines", "Mehdi", "Sarra"]
HASH = ["#tunisie", "#courses", "#promo", "#ramadan", "#hypermarche", "#sfax", "#tunis", "#sousse", "#bonplan", "#rentree"]
NIV = {1: "Très insatisfait", 2: "Insatisfait", 3: "Neutre", 4: "Satisfait", 5: "Très satisfait"}
MOTIFS = ["réclamation sur un ticket de caisse", "demande de remboursement", "question sur la carte de fidélité", "horaires d'ouverture",
          "disponibilité d'un produit", "problème de livraison d'une commande en ligne", "demande de facture", "produit défectueux"]
OBJETS = ["Réclamation — ticket du {j}", "Demande de remboursement", "Carte de fidélité : points non crédités", "Question sur ma commande en ligne",
          "Proposition de partenariat fournisseur", "Produit défectueux acheté le {j}", "Demande de facture", "Remerciements"]


def _prod(P, rng):
    i = int(rng.integers(0, P.height))
    return str(P["base"][i]).lower() if P["cat"][i] != "textile" else str(P["base"][i]).lower()[:-2]


def avis_texte(rng, P, sent):
    """sent 0 = négatif, 1 = neutre, 2 = positif."""
    k = list(ASPECTS)
    a = ASPECTS[k[int(rng.integers(0, len(k)))]]
    parts = []
    if sent == 2:
        parts += [a[0][int(rng.integers(0, len(a[0])))], PROD_P[int(rng.integers(0, 3))].format(p=_prod(P, rng)), CONCL_P[int(rng.integers(0, 5))]]
    elif sent == 0:
        parts += [a[1][int(rng.integers(0, len(a[1])))], PROD_N[int(rng.integers(0, 3))].format(p=_prod(P, rng)), CONCL_N[int(rng.integers(0, 5))]]
    else:
        parts += [PROD_U[int(rng.integers(0, 2))].format(p=_prod(P, rng)), CONCL_U[int(rng.integers(0, 3))]]
    if rng.random() < 0.5:
        parts = parts[:2] if rng.random() < 0.4 else parts
    return ". ".join(parts[:3] if rng.random() < 0.75 else parts[::-1][:3])


def corps_email(rng, P, mg, motif):
    p = _prod(P, rng)
    cor = [f"Bonjour,", f"Je vous écris au sujet de : {motif}.",
           f"J'ai effectué des achats au magasin de {mg} le {int(rng.integers(1, 28)):02d}/{int(rng.integers(1, 13)):02d}, dont du {p}, pour un montant de {rng.uniform(15, 380):.3f} TND.",
           ["Le ticket mentionne un prix différent de celui affiché en rayon.", "Le produit était défectueux et je souhaite être remboursé.", "Mes points de fidélité n'ont pas été crédités.",
            "Je n'ai reçu aucune réponse à mon précédent message.", "Je souhaite obtenir une copie de ma facture."][int(rng.integers(0, 5))],
           "Pouvez-vous me contacter au plus vite ? Mon numéro est le " + str(C.tel_chiffres(rng, 1)[0]) + ".", "Cordialement,"]
    return "\n".join(cor)


def transcription(rng, P, motif):
    p = _prod(P, rng)
    t = [f"Agent : Service client, bonjour, je vous écoute.", f"Client : Bonjour, j'appelle pour {motif}.",
         "Agent : Pouvez-vous me donner votre numéro de carte de fidélité ou de ticket ?", f"Client : Oui, c'est le ticket de samedi, j'avais pris du {p}.",
         "Agent : Un instant, je vérifie dans le système.", "Client : Merci, je patiente.",
         ["Agent : Je vois le problème, je fais le nécessaire immédiatement.", "Agent : Je transmets votre demande au responsable du magasin.", "Agent : Je vous propose un avoir de la même valeur."][int(rng.integers(0, 3))],
         ["Client : Merci beaucoup, behi, c'est très aimable.", "Client : D'accord, mais j'espère que ça sera réglé vite.", "Client : Ça fait la deuxième fois que j'appelle, c'est mouch normal."][int(rng.integers(0, 3))],
         "Agent : Merci de votre appel et bonne journée."]
    return "\n".join(t[: int(rng.integers(6, 10))])


def tache_mois(args):
    cfg, y, m = args
    sc = C.scope(cfg)
    P = C.charger_etat(cfg, "produits.parquet")
    A = cfg["avis"]
    sink = C.Sink(cfg, "avis", f"{y}-{m:02d}")
    last = dt.date(y + (m == 12), 1 if m == 12 else m + 1, 1) - dt.timedelta(days=1)
    jr = list(C.jours(max(sc["debut"], dt.date(y, m, 1)), min(sc["fin"], last)))
    mags = [int(x.split("_")[1]) for x in sc["magasins"]]
    gv = {n: g for n, _, g, _ in R.MAGASINS}
    suf = f"annee={y}/mois={m:02d}"
    # --- réseaux sociaux : un fichier par jour ---
    for d in jr:
        rel = f"avis/reseaux_sociaux/{suf}/posts_{d.isoformat()}.jsonl"
        if C.deja_fait(C.raw(cfg, rel), cfg):
            continue
        rng = C.rng_for(cfg, "posts", d.toordinal())
        mult, _, fl = R.facteurs_jour(d)
        n = int(rng.poisson(A["posts_jour"] * min(mult, 1.5)))
        sent = rng.choice(3, n, p=[0.27, 0.18, 0.55])
        L = []
        for i in range(n):
            mg = mags[int(rng.integers(0, len(mags)))]
            tags = [HASH[int(x)] for x in rng.choice(len(HASH), int(rng.integers(0, 3)), replace=False)]
            L.append(json.dumps({"id": f"p{d.strftime('%y%m%d')}{i:05d}", "plateforme": PLATEF[int(rng.integers(0, 5))], "auteur": f"{PSEUDO[int(rng.integers(0, 12))]}_{int(rng.integers(10, 9999))}",
                                 "date": f"{d.isoformat()}T{int(rng.integers(7, 23)):02d}:{int(rng.integers(0, 60)):02d}:00", "lieu": gv[mg], "magasin": f"magasin_{mg:02d}" if rng.random() < 0.4 else None,
                                 "texte": avis_texte(rng, P, int(sent[i])) + (" " + " ".join(tags) if tags else ""), "likes": int(rng.geometric(0.3)) - 1, "langue": "fr"}, ensure_ascii=False))
        C.sortie_texte(cfg, sink, rel, "avis/reseaux_sociaux", ("\n".join(L) + "\n") if L else "", len(L))
    # --- formulaire du site : un fichier par mois ---
    rel = f"avis/formulaire_site/formulaire_{y}-{m:02d}.csv"
    if not C.deja_fait(C.raw(cfg, rel), cfg) and jr:
        rng = C.rng_for(cfg, "form", y, m)
        rows = {k: [] for k in ["id_av", "dt_av", "mg", "note", "niv_sat", "rayon", "commentaire", "email", "recontact"]}
        for d in jr:
            n = int(rng.poisson(A["formulaires_jour"]))
            for i in range(n):
                note = int(rng.choice([1, 2, 3, 4, 5], p=[0.12, 0.10, 0.18, 0.30, 0.30]))
                niv = NIV[note] if rng.random() > 0.08 else NIV[int(rng.integers(1, 6))]
                rows["id_av"].append(f"AV{d.strftime('%y%m%d')}{i:04d}"); rows["dt_av"].append(d.isoformat()); rows["mg"].append(f"magasin_{mags[int(rng.integers(0, len(mags)))]:02d}")
                rows["note"].append(note); rows["niv_sat"].append(niv); rows["rayon"].append(["caisse", "fruits_legumes", "epicerie", "textile", "service_client", "parking"][int(rng.integers(0, 6))])
                rows["commentaire"].append(avis_texte(rng, P, 2 if note >= 4 else 0 if note <= 2 else 1))
                rows["email"].append(f"client{int(rng.integers(1, 99999))}@gmail.com" if rng.random() < 0.3 else None); rows["recontact"].append("O" if rng.random() < 0.2 else "N")
        spec = {"nulls": ["rayon"], "texte": ["commentaire"], "dates": ["dt_av"], "doublons": True}
        C.sortie_csv(cfg, sink, rel, "avis/formulaire_site", pl.DataFrame(rows, schema_overrides={"email": pl.Utf8}), spec, rng)
    # --- emails : un fichier par mois ---
    rel = f"avis/emails/emails_{y}-{m:02d}.jsonl"
    if not C.deja_fait(C.raw(cfg, rel), cfg) and jr:
        rng = C.rng_for(cfg, "emails", y, m)
        L = []
        for d in jr:
            for i in range(int(rng.poisson(A["emails_jour"]))):
                mg = mags[int(rng.integers(0, len(mags)))]; motif = MOTIFS[int(rng.integers(0, len(MOTIFS)))]
                L.append(json.dumps({"id": f"M{d.strftime('%y%m%d')}{i:04d}", "de": f"client{int(rng.integers(1, 99999))}@{['gmail.com', 'yahoo.fr', 'hotmail.fr', 'topnet.tn'][int(rng.integers(0, 4))]}",
                                     "a": "service.clients@enseigne.tn", "objet": OBJETS[int(rng.integers(0, len(OBJETS)))].format(j=f"{int(rng.integers(1, 28)):02d}/{m:02d}"),
                                     "date": f"{d.isoformat()}T{int(rng.integers(7, 22)):02d}:{int(rng.integers(0, 60)):02d}:00", "corps": corps_email(rng, P, gv[mg], motif),
                                     "pj": [f"ticket_{int(rng.integers(1, 99999))}.jpg"] if rng.random() < 0.15 else []}, ensure_ascii=False))
        C.sortie_texte(cfg, sink, rel, "avis/emails", ("\n".join(L) + "\n") if L else "", len(L))
    # --- appels : transcriptions, un fichier par mois ---
    rel = f"appels/transcriptions/appels_{y}-{m:02d}.jsonl"
    if not C.deja_fait(C.raw(cfg, rel), cfg) and jr:
        rng = C.rng_for(cfg, "appels", y, m)
        L = []
        for d in jr:
            for i in range(int(rng.poisson(A["appels_jour"]))):
                motif = MOTIFS[int(rng.integers(0, len(MOTIFS)))]
                tel, _ = C.tel_formats(C.tel_chiffres(rng, 1), rng, 0.15)
                L.append(json.dumps({"id_appel": f"A{d.strftime('%y%m%d')}{i:04d}", "dt_debut": f"{d.isoformat()}T{int(rng.integers(8, 20)):02d}:{int(rng.integers(0, 60)):02d}:00",
                                     "duree_s": int(rng.integers(40, 900)), "mg": f"magasin_{mags[int(rng.integers(0, len(mags)))]:02d}", "appelant": str(tel[0]), "agent": f"AG{int(rng.integers(1, 40)):03d}",
                                     "motif": motif, "transcription": transcription(rng, P, motif)}, ensure_ascii=False))
        C.sortie_texte(cfg, sink, rel, "appels/transcriptions", ("\n".join(L) + "\n") if L else "", len(L))
    sink.close()
    return 1


def run(cfg):
    sc = C.scope(cfg)
    C.run_pool(tache_mois, [(cfg, y, m) for (y, m) in C.mois_de(sc["debut"], sc["fin"])], int(cfg["max_workers"]), "avis")
    return {}
