"""
referentiels.py — Données de référence tunisiennes : géographie, noms, catalogue produits,
calendrier de saisonnalité. Aucune E/S ici (uniquement des constantes et des fonctions pures).
"""
import datetime as dt

D = dt.date

# ────────────────────────── Géographie ──────────────────────────
# gouvernorat -> [(délégation, code postal)]   (CP cohérent avec la délégation)
GEO = {
    "Tunis": [("Tunis Médina", 1008), ("Bab Bhar", 1000), ("Le Bardo", 2000), ("La Marsa", 2078), ("Carthage", 2016),
              ("El Menzah", 1004), ("Lafayette", 1002), ("Le Kram", 2015), ("La Goulette", 2060)],
    "Ariana": [("Ariana Ville", 2080), ("Ettadhamen", 2041), ("La Soukra", 2036), ("Raoued", 2056), ("Mnihla", 2094)],
    "Ben Arous": [("Ben Arous", 2013), ("Hammam Lif", 2050), ("Radès", 2040), ("Ezzahra", 2034), ("Mégrine", 2033), ("Mourouj", 2074)],
    "Manouba": [("Manouba", 2010), ("Douar Hicher", 2086), ("Oued Ellil", 2022)],
    "Sfax": [("Sfax Ville", 3000), ("Sfax Sud", 3052), ("Sakiet Ezzit", 3021), ("Sakiet Eddaïer", 3011), ("Thyna", 3080)],
    "Sousse": [("Sousse Ville", 4000), ("Sousse Jawhara", 4059), ("Hammam Sousse", 4011), ("Msaken", 4070), ("Akouda", 4022)],
    "Monastir": [("Monastir", 5000), ("Moknine", 5050), ("Ksar Hellal", 5070), ("Jemmal", 5020), ("Skanès", 5060)],
    "Mahdia": [("Mahdia", 5100), ("Ksour Essef", 5180), ("Chebba", 5170), ("El Jem", 5160)],
    "Nabeul": [("Nabeul", 8000), ("Hammamet", 8050), ("Kélibia", 8090), ("Korba", 8070), ("Grombalia", 8030)],
    "Bizerte": [("Bizerte Nord", 7000), ("Menzel Bourguiba", 7050), ("Mateur", 7030), ("Ras Jebel", 7070)],
    "Gabès": [("Gabès Ville", 6000), ("Mareth", 6080), ("El Hamma", 6020)],
    "Médenine": [("Médenine", 4100), ("Houmt Souk", 4180), ("Midoun", 4116), ("Zarzis", 4170), ("Ben Gardane", 4160)],
    "Kairouan": [("Kairouan Nord", 3100), ("Oueslatia", 3150), ("Haffouz", 3130), ("Sbikha", 3140)],
    "Gafsa": [("Gafsa Nord", 2100), ("Métlaoui", 2130), ("Redeyef", 2120), ("El Ksar", 2111)],
    "Béja": [("Béja Nord", 9000), ("Testour", 9060)], "Jendouba": [("Jendouba", 8100), ("Tabarka", 8110)],
    "Le Kef": [("Le Kef Ouest", 7100), ("Dahmani", 7160)], "Siliana": [("Siliana Nord", 6100)],
    "Zaghouan": [("Zaghouan", 1100), ("Zriba", 1120)], "Kasserine": [("Kasserine Nord", 1200), ("Sbeitla", 1250)],
    "Sidi Bouzid": [("Sidi Bouzid Ouest", 9100), ("Regueb", 9170)], "Tozeur": [("Tozeur", 2200), ("Nefta", 2240)],
    "Kébili": [("Kébili Sud", 4200), ("Douz", 4260)], "Tataouine": [("Tataouine Nord", 3200)],
}
POIDS_GOUV = {"Tunis": 14, "Ariana": 8, "Ben Arous": 7, "Manouba": 4, "Sfax": 12, "Sousse": 8, "Monastir": 5, "Mahdia": 3,
              "Nabeul": 6, "Bizerte": 4, "Gabès": 3, "Médenine": 3, "Kairouan": 4, "Gafsa": 3, "Béja": 1.5, "Jendouba": 1.5,
              "Le Kef": 1.2, "Siliana": 1, "Zaghouan": 1.2, "Kasserine": 1.8, "Sidi Bouzid": 1.8, "Tozeur": 1, "Kébili": 1, "Tataouine": 1}
RUES = ["Avenue Habib Bourguiba", "Rue de la République", "Avenue de la Liberté", "Rue Ibn Khaldoun", "Avenue Mohamed V",
        "Rue Hédi Chaker", "Rue de Marseille", "Avenue Farhat Hached", "Rue du 2 Mars 1934", "Avenue Taïeb Mhiri",
        "Rue Ali Belhouane", "Rue de Palestine", "Avenue de Carthage", "Rue El Jazira", "Avenue de l'Environnement",
        "Rue Mongi Slim", "Avenue Hédi Nouira", "Rue de Rome", "Rue Ahmed Amine", "Avenue Ouled Haffouz", "Rue de l'Indépendance",
        "Rue Abou El Kacem Chebbi", "Cité El Khadra", "Cité Ennasr", "Cité Ibn Sina", "Rue Tahar Haddad", "Avenue Kheireddine Pacha",
        "Rue des Orangers", "Rue de la Kasbah", "Avenue de Paris", "Rue Oqba Ibn Nafaa", "Rue Moncef Bey", "Avenue 7 Novembre"]

# 20 magasins : (num, format, gouvernorat, délégation)
MAGASINS = [
    (1, "hypermarche", "Tunis", "La Marsa"), (2, "hypermarche", "Sfax", "Sfax Ville"),
    (3, "supermarche", "Ariana", "Ariana Ville"), (4, "supermarche", "Tunis", "El Menzah"),
    (5, "supermarche", "Ben Arous", "Mourouj"), (6, "supermarche", "Sousse", "Sousse Ville"),
    (7, "supermarche", "Sfax", "Sakiet Ezzit"), (8, "supermarche", "Nabeul", "Hammamet"),
    (9, "supermarche", "Monastir", "Monastir"), (10, "supermarche", "Bizerte", "Bizerte Nord"),
    (11, "proximite", "Tunis", "Lafayette"), (12, "proximite", "Ariana", "La Soukra"),
    (13, "proximite", "Manouba", "Manouba"), (14, "proximite", "Sfax", "Sfax Sud"),
    (15, "proximite", "Sousse", "Hammam Sousse"), (16, "proximite", "Mahdia", "Mahdia"),
    (17, "proximite", "Gabès", "Gabès Ville"), (18, "proximite", "Médenine", "Houmt Souk"),
    (19, "proximite", "Kairouan", "Kairouan Nord"), (20, "proximite", "Gafsa", "Gafsa Nord")]
GOUV_TOURISTIQUES = {"Sousse", "Monastir", "Mahdia", "Nabeul", "Médenine"}

# ────────────────────────── Noms ──────────────────────────
PRENOMS_H = ("Mohamed Ahmed Ali Youssef Amine Walid Sami Karim Slim Anis Mehdi Hichem Nizar Fares Rached Oussama Zied Marouane "
             "Bilel Hatem Skander Aymen Khalil Mourad Riadh Tarek Wassim Houssem Ghassen Seifeddine Mongi Habib Lotfi Fethi "
             "Nabil Chokri Adel Kamel Jalel Mohamed-Ali Ayoub Yassine Iheb Rami Hamza Souhaïl Moez Anouar Sofiane Béchir").split()
PRENOMS_F = ("Fatma Mariem Salma Sarra Ines Rim Amel Nour Yosra Hela Imen Wafa Asma Rania Sonia Olfa Leila Meriem Dorra Emna "
             "Khadija Syrine Ons Manel Hajer Safa Cyrine Nesrine Chaima Ghada Hanen Najet Samia Latifa Zohra Radhia Aziza "
             "Mouna Nadia Souad Rahma Houda Lamia Marwa Wided Abir Amira Jihen Sabrine").split()
NOMS = ("Ben_Salah Trabelsi Jebali Mejri Gharbi Hamdi Chaabane Dridi Bouzid Mansouri Sfaxi Khemiri Ben_Ali Ben_Amor Ben_Youssef "
        "Karoui Belhaj Ayari Hammami Zouari Slimani Cherif Jlassi Bouazizi Gharsallah Ferchichi Haddad Mzoughi Riahi Messaoudi "
        "Sassi Naceur Tlili Louati Baccouche Kammoun Zarrouk Abid Ben_Hassine Brahmi Charfi Hajji Laabidi Ouertani Rebai "
        "Saidi Temimi Turki Yahyaoui Zaouali Boukadida Ben_Mahmoud Ben_Romdhane Bouaziz Dhaouadi Garbi Jaziri Marzouki "
        "Nasri Oueslati Souissi Triki Ben_Othman Ben_Abdallah Ben_Fredj Mabrouk Lahmar Fourati Selmi Amara Jemni").split()
PRENOMS_ETR = "Jean Pierre Marie Sophie Luca Giulia Mustapha Aïcha Ibrahim Aminata Mehmet Elena Carlos Ivan Omar Fatou Marc Chloé".split()
NOMS_ETR = ("Martin Dubois Lefebvre Rossi Esposito Bouzidi Benkirane Al-Mansour Okafor Diallo Yilmaz Petrov Garcia Moreau "
            "Ferrari Haddadi Benali Traoré").split()
DOMAINES_EMAIL = ["gmail.com"] * 6 + ["yahoo.fr"] * 2 + ["hotmail.fr"] * 2 + ["topnet.tn", "planet.tn", "outlook.com"]
BANQUES = ["08", "10", "12", "14", "25", "46", "07", "04"]

# ────────────────────────── Catalogue produits ──────────────────────────
CATEGORIES = ["alimentaire", "boissons", "parapharmacie", "hygiene", "scolaire", "textile", "electromenager"]
TABLE_CAT = {"alimentaire": "prod_a", "boissons": "art_02", "parapharmacie": "t_prd_03", "hygiene": "ref_produit_04",
             "scolaire": "catalogue_05", "textile": "items_06", "electromenager": "articles_07"}
TABLES_MIXTES = ["prod_x1", "art_divers"]
MARGES = {"alimentaire": (0.10, 0.25), "boissons": (0.15, 0.25), "hygiene": (0.20, 0.35), "parapharmacie": (0.20, 0.30),
          "scolaire": (0.25, 0.40), "electromenager": (0.12, 0.20), "textile": (0.40, 0.60)}
MARGE_ENCADREE = (0.03, 0.08)
PART_CAT = {"alimentaire": 0.27, "boissons": 0.11, "hygiene": 0.13, "parapharmacie": 0.10, "scolaire": 0.08, "textile": 0.22,
            "electromenager": 0.09}                      # part du nombre de produits
PART_LIGNES = {"alimentaire": 0.54, "boissons": 0.18, "hygiene": 0.15, "parapharmacie": 0.06, "scolaire": 0.03,
               "textile": 0.037, "electromenager": 0.003}   # part des lignes de vente (hors saisonnalité)
AFFINITE = {  # multiplicateur par format de magasin
    "hypermarche": {"electromenager": 1.6, "textile": 1.3, "scolaire": 1.2},
    "supermarche": {},
    "proximite": {"electromenager": 0.05, "textile": 0.15, "scolaire": 0.5, "parapharmacie": 0.8, "alimentaire": 1.3, "boissons": 1.3}}

# Gabarit : (libellé, [(conditionnement, prix d'achat HT de base en TND)], TVA %, 'E' si prix encadré)
# Chaque groupe : (marques, [gabarits]).
CATALOGUE = {
 "alimentaire": [
  (["SOLEIL", "DELICE CAP", "MABROUKA", "ZITOUNA", "FERME CAP BON", "LAITERIE MEDINA", "VITAL", "SAHEL"], [
    ("LAIT DEMI-ECR", [("1L", 1.30)], 7, "E"), ("LAIT ENTIER", [("1L", 1.30)], 7, "E"), ("LAIT UHT 1/2 ECR", [("6X1L", 7.5)], 7, "E"),
    ("YAOURT NATURE", [("125G", 0.38), ("4X125G", 1.5)], 7, ""), ("YAOURT AUX FRUITS", [("125G", 0.45), ("4X125G", 1.8)], 7, ""),
    ("LBEN", [("1L", 1.6), ("50CL", 0.9)], 7, ""), ("FROMAGE FONDU", [("8P", 1.8), ("16P", 3.2)], 7, ""),
    ("CREME FRAICHE", [("20CL", 1.2), ("50CL", 2.6)], 7, ""), ("BEURRE", [("125G", 2.2), ("250G", 4.1)], 7, ""),
    ("DESSERT LACTE VANILLE", [("100G", 0.6)], 13, ""), ("RAIBI", [("1L", 1.8)], 7, ""), ("FROMAGE RAPE", [("100G", 1.9), ("250G", 4.4)], 7, "")]),
  (["CARTHAGE", "ZITOUNA", "SAHARA", "OASIS", "DJERBA", "TANIT", "BARAKA", "MIMOSA", "EL WAFA", "CAP BON"], [
    ("HUILE D'OLIVE", [("1L", 17.0), ("5L", 80.0)], 7, ""), ("HUILE DE TOURNESOL", [("1L", 3.2), ("2L", 6.3), ("5L", 15.0)], 7, "E"),
    ("SUCRE", [("1KG", 1.5)], 7, "E"), ("FARINE", [("1KG", 0.8)], 7, "E"), ("SEMOULE", [("1KG", 0.9), ("5KG", 4.5)], 7, "E"),
    ("COUSCOUS MOYEN", [("1KG", 1.9), ("500G", 1.1)], 7, ""), ("COUSCOUS FIN", [("1KG", 1.9)], 7, ""),
    ("PATES SPAGHETTI", [("500G", 1.1)], 7, ""), ("PATES MACARONI", [("500G", 1.1)], 7, ""), ("PATES COQUILLETTES", [("500G", 1.1)], 7, ""),
    ("RIZ LONG GRAIN", [("1KG", 3.2)], 7, ""), ("LENTILLES", [("500G", 2.2), ("1KG", 4.0)], 7, ""),
    ("POIS CHICHES", [("500G", 2.0), ("1KG", 3.8)], 7, ""), ("SEL FIN", [("1KG", 0.6)], 7, ""),
    ("HARISSA", [("380G", 1.8), ("190G", 1.1), ("70G", 0.6)], 13, ""), ("TOMATE CONCENTREE", [("400G", 2.4), ("800G", 4.5), ("70G", 0.65)], 13, ""),
    ("RAS EL HANOUT", [("100G", 2.0)], 13, ""), ("DATTES DEGLET NOUR", [("500G", 4.0), ("1KG", 7.5)], 7, ""),
    ("OEUFS PLATEAU", [("X30", 11.5)], 7, "")]),
  (["SAHARA", "CARTHAGE", "MIMOSA", "TANIT", "SALAMBO", "HAMMAMET", "SOLEIL", "KAIROUAN"], [
    ("THON A L'HUILE", [("160G", 3.4), ("80G", 2.0)], 13, ""), ("SARDINES", [("125G", 1.7)], 13, ""), ("MAYONNAISE", [("250G", 2.6)], 13, ""),
    ("CONFITURE ABRICOT", [("370G", 3.0)], 13, ""), ("CONFITURE FRAISE", [("370G", 3.0)], 13, ""), ("MIEL", [("250G", 9.0), ("500G", 17.0)], 13, ""),
    ("BISCUITS", [("200G", 1.2), ("400G", 2.2)], 13, ""), ("GAUFRETTES", [("100G", 0.9)], 13, ""),
    ("CHOCOLAT AU LAIT TABLETTE", [("100G", 2.0)], 19, ""), ("PATE A TARTINER", [("400G", 6.5), ("750G", 11.0)], 19, ""),
    ("CAFE MOULU", [("250G", 4.2), ("500G", 8.1)], 13, ""), ("THE VERT", [("200G", 2.8), ("100G", 1.6)], 13, ""),
    ("CEREALES PETIT DEJ", [("375G", 5.5)], 13, ""), ("PETITS POIS CONSERVE", [("400G", 1.9)], 13, ""),
    ("HARICOTS VERTS CONSERVE", [("400G", 1.9)], 13, ""), ("OLIVES VERTES", [("500G", 3.2)], 13, ""),
    ("FEUILLES DE BRIK", [("X10", 1.2)], 7, ""), ("CONCENTRE DE FRUITS SECS", [("250G", 5.0)], 13, "")])],
 "boissons": [
  (["SOURCE ZAGHOUAN", "OUED EL AIN", "OASIS", "CAP VERT", "SAHARA"], [
    ("EAU MINERALE", [("50CL", 0.4), ("1.5L", 0.75), ("5L", 1.9)], 13, ""), ("EAU GAZEUSE", [("1L", 1.1), ("33CL", 0.8)], 13, "")]),
  (["COLA TANIT", "FRIZZ", "SOLEIL", "ORANJA", "SPARKY", "MIMOSA", "POP", "FIZZ"], [
    ("BOISSON GAZEUSE COLA", [("33CL", 0.9), ("1L", 1.7), ("1.5L", 2.2), ("2L", 2.6)], 19, ""),
    ("LIMONADE", [("33CL", 0.85), ("1L", 1.5), ("1.5L", 2.0)], 19, ""), ("BOISSON ORANGE GAZEUSE", [("33CL", 0.9), ("1.5L", 2.2)], 19, ""),
    ("TONIC", [("33CL", 0.95)], 19, ""), ("ENERGISANTE", [("25CL", 2.2)], 19, "")]),
  (["DELICE CAP", "SOLEIL", "VERGER", "CAP BON", "SAHEL"], [
    ("JUS ORANGE", [("1L", 2.8), ("20CL", 0.8)], 13, ""), ("JUS POMME", [("1L", 2.8), ("20CL", 0.8)], 13, ""),
    ("JUS MULTIFRUITS", [("1L", 2.9)], 13, ""), ("NECTAR PECHE", [("1L", 2.7), ("20CL", 0.8)], 13, ""),
    ("SIROP GRENADINE", [("75CL", 3.5)], 13, ""), ("SIROP MENTHE", [("75CL", 3.5)], 13, ""), ("THE GLACE", [("33CL", 1.4)], 19, "")])],
 "parapharmacie": [
  (["LABO CARTHAGE", "PHARMA TANIT", "SALAMBO LABS", "LABO MEDINA", "UNIMED", "SAHEL PHARMA"], [
    ("PARACETAMOL 500MG", [("B/16", 1.4), ("B/20", 1.7)], 7, ""), ("PARACETAMOL 1G", [("B/8", 1.2)], 7, ""),
    ("IBUPROFENE 400MG", [("B/20", 3.1)], 7, ""), ("ASPIRINE 500MG", [("B/20", 2.0)], 7, ""),
    ("VITAMINE C 500MG", [("B/30", 5.5), ("B/60", 9.0)], 7, ""), ("MAGNESIUM B6", [("B/50", 8.5)], 7, ""),
    ("SIROP TOUX ENFANT", [("125ML", 4.2)], 7, ""), ("SIROP TOUX ADULTE", [("150ML", 4.5)], 7, ""), ("SPRAY NASAL", [("20ML", 3.9)], 7, ""),
    ("COLLYRE", [("10ML", 4.5)], 7, ""), ("PASTILLES GORGE", [("B/24", 3.2)], 7, ""), ("ANTISEPTIQUE", [("250ML", 3.8)], 7, ""),
    ("EAU OXYGENEE", [("125ML", 1.0)], 7, ""), ("ALCOOL 70", [("250ML", 1.4)], 7, ""), ("COMPRESSES STERILES", [("B/20", 1.8)], 7, ""),
    ("PANSEMENTS", [("B/20", 2.2)], 7, ""), ("SERUM PHYSIOLOGIQUE", [("X20", 5.0)], 7, ""), ("THERMOMETRE DIGITAL", [("1PCE", 9.0)], 19, "")]),
  (["DERMA SAHEL", "NATURA", "BIOVIE", "SOLEIL", "MIMOSA", "LABO CARTHAGE"], [
    ("CREME HYDRATANTE", [("200ML", 8.5), ("50ML", 4.5)], 19, ""), ("CREME SOLAIRE SPF50", [("100ML", 14.0)], 19, ""),
    ("SAVON DERMATOLOGIQUE", [("100G", 4.2)], 19, ""), ("SHAMPOING ANTI-PELLICULAIRE", [("200ML", 8.0)], 19, ""),
    ("BAUME A LEVRES", [("4G", 2.5)], 19, ""), ("CREME MAINS", [("75ML", 3.8)], 19, ""), ("HUILE D'ARGAN COSMETIQUE", [("30ML", 12.0)], 19, ""),
    ("LAIT DE TOILETTE", [("200ML", 5.5)], 19, ""), ("GEL INTIME", [("200ML", 6.5)], 19, ""), ("DENTIFRICE DENTS SENSIBLES", [("75ML", 5.5)], 19, "")])],
 "hygiene": [
  (["SOLEIL", "MIMOSA", "ZITOUNA", "FLEUR D'ORANGER", "JASMIN", "NATURA", "TANIT"], [
    ("SAVON", [("100G", 0.8), ("X4", 3.0)], 19, ""), ("SAVON LIQUIDE", [("300ML", 2.8)], 19, ""), ("GEL DOUCHE", [("250ML", 3.5), ("500ML", 6.0)], 19, ""),
    ("SHAMPOING", [("250ML", 4.5), ("400ML", 6.5)], 19, ""), ("APRES SHAMPOING", [("200ML", 5.0)], 19, ""), ("DENTIFRICE", [("75ML", 2.5)], 19, ""),
    ("DEODORANT", [("150ML", 5.0)], 19, ""), ("MOUSSE A RASER", [("200ML", 4.8)], 19, ""), ("LAIT CORPOREL", [("400ML", 8.0)], 19, ""),
    ("CREME HYDRATANTE", [("200ML", 7.0)], 19, ""), ("COUCHES BEBE T3", [("X44", 25.0)], 19, ""), ("LINGETTES BEBE", [("X72", 4.5)], 19, ""),
    ("PAPIER TOILETTE", [("X12", 7.5), ("X6", 4.0)], 19, ""), ("MOUCHOIRS", [("X10", 1.5)], 19, ""), ("COTON DEMAQUILLANT", [("100G", 2.2)], 19, "")]),
  (["MAXI PROPRE", "ECLAT", "BLANCHEUR", "SOLEIL", "DJERBA CLEAN", "PROPRETE+", "FRAICHEUR", "ELEGANT"], [
    ("LESSIVE POUDRE", [("3KG", 11.0), ("1.5KG", 6.0), ("6KG", 20.0)], 19, ""), ("LESSIVE LIQUIDE", [("2L", 12.0)], 19, ""),
    ("LIQUIDE VAISSELLE", [("1.25L", 3.5), ("500ML", 1.8)], 19, ""), ("EAU DE JAVEL", [("2L", 1.6), ("1L", 1.0)], 19, ""),
    ("DETERGENT SOL", [("1L", 3.2), ("2L", 5.8)], 19, ""), ("NETTOYANT VITRES", [("500ML", 3.0)], 19, ""), ("DEGRAISSANT", [("500ML", 4.0)], 19, ""),
    ("ASSOUPLISSANT", [("2L", 6.5)], 19, ""), ("ESSUIE-TOUT", [("X2", 3.4)], 19, ""), ("SACS POUBELLE 50L", [("X20", 3.0)], 19, ""),
    ("EPONGES", [("X3", 1.8)], 19, ""), ("INSECTICIDE", [("400ML", 6.5)], 19, ""), ("DESODORISANT INTERIEUR", [("300ML", 4.2)], 19, ""),
    ("PASTILLES LAVE-VAISSELLE", [("X30", 8.5)], 19, "")])],
 "scolaire": [
  (["ACADEMIA", "PLUME D'OR", "KALAM", "TUNISCOLAIRE", "MAESTRO", "COLORIS", "SAVANT"], [
    ("CAHIER 48P GD CARREAUX", [("1PCE", 1.3)], 13, ""), ("CAHIER 96P GD CARREAUX", [("1PCE", 2.2)], 13, ""), ("CAHIER 96P GD SEYES", [("1PCE", 2.2)], 13, ""),
    ("CAHIER 192P GD CARREAUX", [("1PCE", 3.9)], 13, ""), ("CAHIER 48P PT SEYES", [("1PCE", 1.0)], 13, ""), ("STYLO BILLE BLEU", [("1PCE", 0.35), ("X10", 3.2)], 19, ""),
    ("STYLO GEL NOIR", [("1PCE", 0.9)], 19, ""), ("CRAYON HB", [("X12", 3.5)], 19, ""), ("GOMME BLANCHE", [("1PCE", 0.6)], 19, ""),
    ("TAILLE-CRAYON", [("1PCE", 0.7)], 19, ""), ("REGLE 30CM", [("1PCE", 0.8)], 19, ""), ("EQUERRE 60", [("1PCE", 1.2)], 19, ""),
    ("COMPAS", [("1PCE", 2.5)], 19, ""), ("COLLE BATON", [("21G", 1.2)], 19, ""), ("CISEAUX ECOLIER", [("1PCE", 1.6)], 19, ""),
    ("FEUTRES", [("X12", 4.8)], 19, ""), ("CRAYONS DE COULEUR", [("X12", 5.5)], 19, ""), ("PEINTURE GOUACHE", [("X6", 6.0)], 19, ""),
    ("CLASSEUR A4 4 ANNEAUX", [("1PCE", 6.0)], 19, ""), ("PORTE-VUES", [("X40", 5.0)], 19, ""), ("PAPIER A4", [("X500", 14.0)], 19, ""),
    ("CALCULATRICE SCIENTIFIQUE", [("1PCE", 12.5)], 19, ""), ("SURLIGNEUR", [("X4", 5.0)], 19, ""), ("CARTABLE PRIMAIRE", [("1PCE", 45.0)], 19, ""),
    ("TROUSSE", [("1PCE", 8.0)], 19, "")])],
 "textile": "TEXTILE",      # généré à partir de combinaisons (voir TEXTILE_*)
 "electromenager": [
  (["ELECTRA", "TECHNOSTAR", "FRIGOMED", "SAHARA TECH", "ALPHA", "NOVAHOME", "DOMUS"], [
    ("BOUILLOIRE 1.7L", [("1PCE", 28.0)], 19, ""), ("FER A REPASSER VAPEUR", [("1PCE", 35.0)], 19, ""), ("MIXEUR PLONGEANT 600W", [("1PCE", 45.0)], 19, ""),
    ("ROBOT CUISINE 1000W", [("1PCE", 150.0)], 19, ""), ("GRILLE-PAIN 2T", [("1PCE", 30.0)], 19, ""), ("VENTILATEUR 40CM", [("1PCE", 55.0)], 19, ""),
    ("CHAUFFAGE D'APPOINT", [("1PCE", 70.0)], 19, ""), ("TV LED 32P", [("1PCE", 380.0)], 19, ""), ("TV LED 43P", [("1PCE", 620.0)], 19, ""),
    ("TV LED 55P", [("1PCE", 1050.0)], 19, ""), ("MICRO-ONDES 20L", [("1PCE", 210.0)], 19, ""), ("MICRO-ONDES 25L", [("1PCE", 260.0)], 19, ""),
    ("REFRIGERATEUR 300L", [("1PCE", 1100.0)], 19, ""), ("CONGELATEUR 5 TIROIRS", [("1PCE", 900.0)], 19, ""), ("MACHINE A LAVER 7KG", [("1PCE", 900.0)], 19, ""),
    ("MACHINE A LAVER 9KG", [("1PCE", 1150.0)], 19, ""), ("CUISINIERE 4 FEUX", [("1PCE", 750.0)], 19, ""), ("CLIMATISEUR 9000 BTU", [("1PCE", 1000.0)], 19, ""),
    ("ASPIRATEUR 2000W", [("1PCE", 190.0)], 19, ""), ("SECHE-CHEVEUX 2000W", [("1PCE", 32.0)], 19, ""), ("MACHINE A CAFE", [("1PCE", 90.0)], 19, ""),
    ("CAFETIERE FILTRE", [("1PCE", 40.0)], 19, ""), ("BLENDER 1.5L", [("1PCE", 60.0)], 19, ""), ("FRITEUSE SANS HUILE", [("1PCE", 120.0)], 19, ""),
    ("HACHOIR ELECTRIQUE", [("1PCE", 48.0)], 19, ""), ("EPILATEUR", [("1PCE", 65.0)], 19, ""), ("TONDEUSE BARBE", [("1PCE", 38.0)], 19, ""),
    ("RADIATEUR BAIN D'HUILE", [("1PCE", 150.0)], 19, "")])],
}
# Textile : article, prix d'achat HT de base, genres autorisés
TEXTILE_ARTICLES = [("T-SHIRT COT.", 8, "HFE"), ("POLO", 14, "HFE"), ("CHEMISE", 22, "HF"), ("JEAN", 38, "HFE"), ("PANTALON", 32, "HFE"),
                    ("SHORT", 16, "HFE"), ("SURVETEMENT", 45, "HFE"), ("PULL", 30, "HFE"), ("SWEAT CAPUCHE", 35, "HFE"), ("ROBE", 40, "FE"),
                    ("JUPE", 25, "FE"), ("BLOUSE", 24, "F"), ("PYJAMA", 28, "HFE"), ("CHAUSSETTES X3", 6, "HFE"), ("SLIP X3", 9, "HFE"),
                    ("DEBARDEUR", 9, "FE"), ("VESTE", 55, "HFE"), ("MANTEAU", 90, "HF"), ("FOUTA", 18, "F"), ("ECHARPE", 12, "HF"),
                    ("BONNET", 8, "HFE"), ("CASQUETTE", 10, "HE"), ("MAILLOT DE BAIN", 20, "HFE")]
TEXTILE_MARQUES = ["MODA", "ELEGANCE", "TANIT WEAR", "SAHEL", "KIDS CLUB", "STYL'MED", "JASMIN"]
TEXTILE_TAILLES = {"H": ["S", "M", "L", "XL"], "F": ["S", "M", "L", "XL"], "E": ["4A", "8A", "12A"]}
CAT_UNITE = {"alimentaire": "U", "boissons": "U", "parapharmacie": "BTE", "hygiene": "U", "scolaire": "PCE", "textile": "PCE", "electromenager": "PCE"}

# Variantes de noms de colonnes cryptiques (une par table, tirée au hasard)
VARIANTES_COLONNES = {
    "id": ["art_id", "id_art", "cd_art"], "lib": ["lib", "dsg", "des_art"], "ean": ["cb", "gencod", "ean"],
    "pa": ["pa_ht", "px_ach", "pa"], "pv_ht": ["pv_ht", "px_vte", "pvh"], "pv_ttc": ["pv_ttc", "px_ttc", "pvt"],
    "mrg": ["mrg", "mg_pct", "tx_mg"], "tva": ["tva", "tx_tva", "tv"], "frn": ["frn_id", "id_frn", "cd_frn"],
    "unt": ["unt", "ut_v", "unite"]}
NATURE_COLONNES_PRODUIT = {"id": "identifiant_article", "lib": "libelle_produit", "ean": "code_barre_ean13", "pa": "prix_achat_ht",
                           "pv_ht": "prix_vente_ht", "pv_ttc": "prix_vente_ttc", "mrg": "taux_marge", "tva": "taux_tva",
                           "frn": "identifiant_fournisseur", "unt": "unite_conditionnement"}

# ────────────────────────── Calendrier / saisonnalité ──────────────────────────
RAMADAN = [(D(2025, 3, 1), D(2025, 3, 29)), (D(2026, 2, 18), D(2026, 3, 19))]
AID_FITR = [D(2025, 3, 30), D(2026, 3, 20)]
AID_ADHA = [D(2025, 6, 6), D(2026, 5, 27)]
SOLDES = [((1, 10), (2, 15)), ((7, 1), (8, 15))]            # (mois, jour) début-fin, chaque année
FETES_JOUR_SEM = {0: 0.86, 1: 0.86, 2: 0.90, 3: 0.93, 4: 1.05, 5: 1.32, 6: 1.12}   # pic le week-end


def _in(d, a, b):
    return a <= d <= b


def en_ramadan(d):
    return any(_in(d, a, b) for a, b in RAMADAN)


def en_soldes(d):
    return any((d.month, d.day) >= x and (d.month, d.day) <= y for x, y in SOLDES)


def facteurs_jour(d):
    """Retourne (mult_tickets, {catégorie: multiplicateur de lignes}, drapeaux) pour une date.
    Pur : aucune aléa -> sert aussi à l'estimation du volume sans générer."""
    m = FETES_JOUR_SEM[d.weekday()]
    cat = {c: 1.0 for c in CATEGORIES}
    fl = {"ramadan": False, "rentree": False, "soldes": False, "ete": False, "aid": False, "pre_aid": False}
    for a, b in RAMADAN:
        if _in(d, a, b):
            fl["ramadan"] = True
            m *= 1.12
            cat["alimentaire"] *= 1.8; cat["boissons"] *= 1.5; cat["textile"] *= 0.7; cat["scolaire"] *= 0.6
            if (b - d).days < 10:
                cat["textile"] *= 2.2 ; m *= 1.08           # habits de l'Aïd
    for x in AID_FITR:
        if d == x or d == x + dt.timedelta(days=1):
            fl["aid"] = True; m *= 0.45
        if 0 < (x - d).days <= 3:
            fl["pre_aid"] = True; m *= 1.25; cat["alimentaire"] *= 1.4; cat["textile"] *= 1.8
    for x in AID_ADHA:
        if d == x or d == x + dt.timedelta(days=1):
            fl["aid"] = True; m *= 0.40
        if 0 < (x - d).days <= 6:
            fl["pre_aid"] = True; m *= 1.15; cat["alimentaire"] *= 1.3; cat["textile"] *= 1.2; cat["hygiene"] *= 1.2
    if d.month == 9 and d.day <= 20:                      # rentrée scolaire
        fl["rentree"] = True
        m *= 1.08
        cat["scolaire"] *= 10.0 if 5 <= d.day <= 15 else 4.0
    if en_soldes(d):
        fl["soldes"] = True
        m *= 1.02; cat["textile"] *= 2.5
    if (d.month, d.day) >= (6, 15) and (d.month, d.day) <= (8, 31):   # été
        fl["ete"] = True
        cat["boissons"] *= 1.4; cat["electromenager"] *= 1.4
        if (d.month, d.day) >= (7, 15) and (d.month, d.day) <= (8, 25):   # retour des Tunisiens de l'étranger
            m *= 1.10; cat["alimentaire"] *= 1.1
    if d.month in (12, 1, 2):
        cat["electromenager"] *= 1.3
    return m, cat, fl


def mult_touristique(d):
    """Multiplicateur additionnel pour les magasins de zones touristiques."""
    if (d.month, d.day) >= (6, 15) and (d.month, d.day) <= (9, 10):
        return 1.5 if d.month == 8 else 1.35
    if d.month in (4, 5, 10):
        return 1.1
    return 1.0
