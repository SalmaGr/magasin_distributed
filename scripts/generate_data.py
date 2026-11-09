"""
generate_data.py
================
Génère toutes les données batch du projet Smart Library :
  - utilisateurs.csv          (1 000 utilisateurs)
  - livres.json         (500 livres)
  - catalogue.xml      (arborescence catégories)
  - commandes.csv         (5 000 commandes historiques)
  - inventaire.csv      (stock par livre)
  - factures/          (50 faux PDFs de factures)

Usage :
    python scripts/generate_data.py
"""

import csv
import json
import os
import random
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from pathlib import Path

from faker import Faker

fake = Faker("fr_FR")
random.seed(42)

OUTPUT = Path("/home/jovyan/data/batch")
OUTPUT.mkdir(parents=True, exist_ok=True)

# ── Données de référence ──────────────────────────────────────
GENRES = ["Roman", "Science-fiction", "Policier", "Histoire",
          "Philosophie", "Jeunesse", "Biographie", "Poésie",
          "Développement personnel", "Sciences"]

EDITEURS = ["Gallimard", "Seuil", "Flammarion", "Hachette",
            "Actes Sud", "Minuit", "Albin Michel", "Fayard"]

LANGUES = ["fr", "ar", "en", "es"]

STATUTS_COMMANDE = ["livré", "en_cours", "annulé", "remboursé"]

METHODES_PAIEMENT = ["carte", "virement", "paypal", "chèque"]

# ─────────────────────────────────────────────────────────────
# 1. UTILISATEURS — utilisateurs.csv
# ─────────────────────────────────────────────────────────────
def generate_users(n=1000):
    path = OUTPUT / "utilisateurs.csv"
    fields = ["user_id", "nom", "prenom", "email", "telephone",
              "ville", "pays", "date_inscription", "genre_prefere",
              "nb_commandes_total", "statut", "updated_at"]

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for i in range(1, n + 1):
            writer.writerow({
                "user_id"           : f"USR{i:05d}",
                "nom"               : fake.last_name(),
                "prenom"            : fake.first_name(),
                "email"             : fake.unique.email(),
                "telephone"         : fake.phone_number(),
                "ville"             : fake.city(),
                "pays"              : random.choices(
                                        ["Tunisie", "Maroc", "France", "Algérie"],
                                        weights=[40, 30, 20, 10])[0],
                "date_inscription"  : fake.date_between(
                                        start_date="-3y", end_date="today").isoformat(),
                "genre_prefere"     : random.choice(GENRES),
                "nb_commandes_total": random.randint(0, 50),
                "statut"            : random.choices(
                                        ["actif", "inactif", "suspendu"],
                                        weights=[75, 20, 5])[0],
                "updated_at"        : datetime.now().isoformat(),
            })
    print(f"✓ {n} utilisateurs → {path}")
    return [f"USR{i:05d}" for i in range(1, n + 1)]


# ─────────────────────────────────────────────────────────────
# 2. LIVRES — livres.json
# ─────────────────────────────────────────────────────────────
def generate_books(n=500):
    path = OUTPUT / "livres.json"
    books = []
    for i in range(1, n + 1):
        genre = random.choice(GENRES)
        prix = round(random.uniform(9.99, 49.99), 2)
        books.append({
            "book_id"         : f"BK{i:04d}",
            "titre"           : fake.sentence(nb_words=random.randint(2, 6))
                                    .replace(".", "").title(),
            "auteur"          : fake.name(),
            "isbn"            : fake.isbn13(),
            "editeur"         : random.choice(EDITEURS),
            "genre"           : genre,
            "sous_genre"      : f"{genre} contemporain" if random.random() > 0.5 else genre,
            "langue"          : random.choices(LANGUES, weights=[50, 20, 25, 5])[0],
            "nb_pages"        : random.randint(80, 900),
            "date_publication": fake.date_between(
                                    start_date="-30y", end_date="today").isoformat(),
            "prix_vente"      : prix,
            "prix_achat"      : round(prix * random.uniform(0.3, 0.5), 2),
            "note_moyenne"    : round(random.uniform(1.0, 5.0), 1),
            "nb_avis"         : random.randint(0, 500),
            "nb_ventes_total" : random.randint(0, 2000),
            "disponible"      : random.random() > 0.1,
            "tags"            : random.sample(
                                    ["bestseller", "prix_littéraire", "nouveauté",
                                     "classique", "recommandé", "série"], k=random.randint(0,3)),
            "description"     : fake.paragraph(nb_sentences=3),
            "updated_at"      : datetime.now().isoformat(),
        })

    with open(path, "w", encoding="utf-8") as f:
        json.dump(books, f, ensure_ascii=False, indent=2)
    print(f"✓ {n} livres → {path}")
    return [b["book_id"] for b in books]


# ─────────────────────────────────────────────────────────────
# 3. CATALOGUE — catalogue.xml
# ─────────────────────────────────────────────────────────────
def generate_catalogue(book_ids):
    path = OUTPUT / "catalogue.xml"
    root = ET.Element("catalogue", {
        "nom": "Al Maktaba",
        "version": "2.0",
        "date_export": datetime.now().isoformat()
    })

    for genre in GENRES:
        cat_elem = ET.SubElement(root, "categorie", {"nom": genre, "id": genre.lower()[:6]})
        desc = ET.SubElement(cat_elem, "description")
        desc.text = f"Collection {genre} — tous âges et niveaux"

        sous_cats = [f"{genre} classique", f"{genre} contemporain", f"{genre} jeunesse"]
        for sc in sous_cats:
            sc_elem = ET.SubElement(cat_elem, "sous_categorie", {"nom": sc})
            # Assigner aléatoirement quelques livres
            livres_cat = random.sample(book_ids, k=random.randint(5, 15))
            for bid in livres_cat:
                livre_elem = ET.SubElement(sc_elem, "livre", {"id": bid})
                ordre = ET.SubElement(livre_elem, "ordre_affichage")
                ordre.text = str(random.randint(1, 100))
                featured = ET.SubElement(livre_elem, "en_vedette")
                featured.text = str(random.random() > 0.8).lower()

    tree = ET.ElementTree(root)
    ET.indent(tree, space="  ")
    tree.write(path, encoding="utf-8", xml_declaration=True)
    print(f"✓ Catalogue XML → {path}")


# ─────────────────────────────────────────────────────────────
# 4. COMMANDES — commandes.csv
# ─────────────────────────────────────────────────────────────
def generate_orders(user_ids, book_ids, n=5000):
    path = OUTPUT / "commandes.csv"
    fields = ["order_id", "user_id", "book_id", "quantite", "prix_unitaire",
              "montant_total", "statut", "methode_paiement", "date_commande",
              "date_livraison", "adresse_livraison", "updated_at"]

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for i in range(1, n + 1):
            date_cmd = fake.date_time_between(start_date="-2y", end_date="now")
            statut   = random.choices(STATUTS_COMMANDE, weights=[65, 20, 10, 5])[0]
            qte      = random.randint(1, 5)
            prix     = round(random.uniform(9.99, 49.99), 2)
            writer.writerow({
                "order_id"         : f"ORD{i:07d}",
                "user_id"          : random.choice(user_ids),
                "book_id"          : random.choice(book_ids),
                "quantite"         : qte,
                "prix_unitaire"    : prix,
                "montant_total"    : round(prix * qte, 2),
                "statut"           : statut,
                "methode_paiement" : random.choice(METHODES_PAIEMENT),
                "date_commande"    : date_cmd.isoformat(),
                "date_livraison"   : (date_cmd + timedelta(
                                        days=random.randint(1, 7))).isoformat()
                                     if statut == "livré" else "",
                "adresse_livraison": fake.address().replace("\n", ", "),
                "updated_at"       : datetime.now().isoformat(),
            })
    print(f"✓ {n} commandes → {path}")


# ─────────────────────────────────────────────────────────────
# 5. STOCK — inventaire.csv
# ─────────────────────────────────────────────────────────────
def generate_inventory(book_ids):
    path = OUTPUT / "inventaire.csv"
    fields = ["stock_id", "book_id", "quantite_disponible", "quantite_reservee",
              "seuil_alerte", "entrepot", "derniere_livraison", "updated_at"]

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for bid in book_ids:
            writer.writerow({
                "stock_id"             : f"STK-{bid}",
                "book_id"              : bid,
                "quantite_disponible"  : random.randint(0, 200),
                "quantite_reservee"    : random.randint(0, 20),
                "seuil_alerte"         : random.choice([5, 10, 15, 20]),
                "entrepot"             : random.choice(["Tunis-Nord", "Tunis-Sud", "Sfax", "Sousse"]),
                "derniere_livraison"   : fake.date_between(
                                            start_date="-90d", end_date="today").isoformat(),
                "updated_at"           : datetime.now().isoformat(),
            })
    print(f"✓ {len(book_ids)} lignes stock → {path}")


# ─────────────────────────────────────────────────────────────
# 6. FACTURES — PDFs (texte simulé, structure JSON)
#    On génère des fichiers .txt structurés (simulation PDF)
#    Dans le notebook, on montrera l'extraction avec pdfplumber
# ─────────────────────────────────────────────────────────────
def generate_factures(user_ids, book_ids, n=50):
    inv_dir = OUTPUT / "factures"
    inv_dir.mkdir(exist_ok=True)

    for i in range(1, n + 1):
        inv_id  = f"FAC{i:04d}"
        user    = random.choice(user_ids)
        items   = []
        total   = 0
        for _ in range(random.randint(1, 5)):
            prix  = round(random.uniform(9.99, 49.99), 2)
            qte   = random.randint(1, 3)
            items.append({
                "livre_id": random.choice(book_ids),
                "qte": qte,
                "prix": prix,
                "sous_total": round(prix * qte, 2)
            })
            total += round(prix * qte, 2)

        contenu = f"""
=== FACTURE {inv_id} ===
Date     : {fake.date_between(start_date="-1y", end_date="today").isoformat()}
Client   : {user}
Adresse  : {fake.address().replace(chr(10), ', ')}

ARTICLES :
"""
        for it in items:
            contenu += f"  - {it['livre_id']}  x{it['qte']}  {it['prix']}€  = {it['sous_total']}€\n"

        tva = round(total * 0.19, 2)
        contenu += f"""
Sous-total HT : {round(total, 2)}€
TVA (19%)     : {tva}€
TOTAL TTC     : {round(total + tva, 2)}€

Méthode de paiement : {random.choice(METHODES_PAIEMENT)}
Statut              : payé
"""
        with open(inv_dir / f"{inv_id}.txt", "w", encoding="utf-8") as f:
            f.write(contenu)

    print(f"✓ {n} factures simulées → {inv_dir}")


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("\n=== Génération des données Smart Library ===\n")
    user_ids = generate_users(1000)
    book_ids = generate_books(500)
    generate_catalogue(book_ids)
    generate_orders(user_ids, book_ids, 5000)
    generate_inventory(book_ids)
    generate_factures(user_ids, book_ids, 50)
    print("\n✅ Toutes les données batch générées avec succès !")
    print(f"   Dossier : {OUTPUT}")
