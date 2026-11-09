"""src_documents.py — Documents : factures fournisseurs (XML), contrats (PDF), fiches techniques produit (PDF)."""
import datetime as dt
import zlib

import numpy as np

import common as C
import referentiels as R
import src_fournisseurs as F

CLAUSES = ["Article {n} — Paiement : les factures sont réglées à {j} jours fin de mois par virement bancaire sur le RIB du fournisseur.",
           "Article {n} — Pénalités : tout retard de livraison supérieur à 48 heures entraîne une pénalité de 2 % du montant de la commande.",
           "Article {n} — Remise de fin d'année : une remise de {r} % est accordée sur le chiffre d'affaires annuel hors taxes.",
           "Article {n} — Qualité : les produits périssables doivent être livrés avec au moins 70 % de leur durée de vie restante.",
           "Article {n} — Retours : les invendus défectueux sont repris et remboursés ou remplacés dans un délai de 15 jours.",
           "Article {n} — Exclusivité : le fournisseur s'engage à ne pas modifier ses tarifs en cours de trimestre sans préavis de 30 jours.",
           "Article {n} — Confidentialité : les parties s'interdisent de divulguer les conditions commerciales du présent contrat.",
           "Article {n} — Litiges : tout différend relève de la compétence du tribunal de première instance de Tunis."]
MATIERES = ["100 % coton", "coton 65 % polyester 35 %", "polyester", "viscose", "lin et coton"]


def _suffixe(d):
    return f"annee={d.year}/mois={d.month:02d}"


def facture_xml(rng, num, d, mg, f, P, PA, mi, ids_p):
    k = int(rng.integers(8, 61))
    sel = rng.choice(ids_p, min(k, len(ids_p)), replace=False)
    L, tot_ht, tvas = [], 0, {}
    for j, i in enumerate(sel, 1):
        q = int(rng.choice([6, 12, 24, 36, 48, 60, 120]))
        pu = int(PA[mi][i]); rem = int(rng.choice([0, 0, 0, 2, 5]))
        ht = int(round(q * pu * (100 - rem) / 100)); t = int(P["tva"][i])
        tot_ht += ht; tvas[t] = tvas.get(t, 0) + ht * t // 100
        L.append(f'    <Ligne no="{j}"><CodeArticle>{P["code"][i]}</CodeArticle><CodeBarres>{P["ean"][i]}</CodeBarres><Designation>{P["lib"][i].replace("&", "&amp;")}</Designation>'
                 f'<Quantite>{q}</Quantite><Unite>{P["unt"][i]}</Unite><PrixUnitaireHT>{pu / 1000:.3f}</PrixUnitaireHT><Remise>{rem}</Remise><TauxTVA>{t}</TauxTVA>'
                 f'<MontantHT>{ht / 1000:.3f}</MontantHT></Ligne>')
    tva = sum(tvas.values()); timbre = 1000
    tv = "".join(f'<TVA taux="{t}">{v / 1000:.3f}</TVA>' for t, v in sorted(tvas.items()))
    return (f'<?xml version="1.0" encoding="UTF-8"?>\n<Facture xmlns="urn:tn:facture:1.0">\n  <Entete>\n    <NumFacture>{num}</NumFacture><DateFacture>{d.isoformat()}</DateFacture>'
            f'<Devise>TND</Devise><EcheanceJours>{int(rng.choice([30, 45, 60, 90]))}</EcheanceJours>\n  </Entete>\n  <Fournisseur><Code>{f["id_frn"]}</Code><RaisonSociale>{f["rs"]}</RaisonSociale>'
            f'<MatriculeFiscal>{f["mf"]}</MatriculeFiscal><RIB>{f["rib"]}</RIB></Fournisseur>\n  <Client><Magasin>{mg}</Magasin></Client>\n  <Lignes>\n' + "\n".join(L) +
            f'\n  </Lignes>\n  <Totaux><TotalHT>{tot_ht / 1000:.3f}</TotalHT><TVAParTaux>{tv}</TVAParTaux><TimbreFiscal>1.000</TimbreFiscal>'
            f'<TotalTTC>{(tot_ht + tva + timbre) / 1000:.3f}</TotalTTC></Totaux>\n</Facture>\n')


def tache_factures(args):
    cfg, mid, y, m = args
    sc = C.scope(cfg)
    P0, PX = C.charger_etat(cfg, "produits.parquet"), C.charger_etat(cfg, "prix.npz")
    P = {c: P0[c].to_list() for c in ("code", "ean", "lib", "unt", "tva", "frn", "m_lanc")}
    FR = F.construire_fournisseurs(cfg)
    frn = {r["id_frn"]: r for r in FR.iter_rows(named=True)}
    fmt = next(x for x in C.magasins_cfg(cfg) if x["id"] == mid)["format"]
    par_frn = {}
    for i, f in enumerate(P["frn"]):
        par_frn.setdefault(f, []).append(i)
    ids_f = [f for f in frn if f in par_frn]
    sink = C.Sink(cfg, "documents", f"fac_{mid}_{y}-{m:02d}")
    d0 = C.periode_complete(cfg)[0]
    last = dt.date(y + (m == 12), 1 if m == 12 else m + 1, 1) - dt.timedelta(days=1)
    for d in C.jours(max(sc["debut"], dt.date(y, m, 1)), min(sc["fin"], last)):
        rng = C.rng_for(cfg, "fac", mid, d.toordinal())
        n = int(rng.poisson(cfg["documents"]["factures_jour"][fmt] * min(R.facteurs_jour(d)[0], 1.4)))
        mi = (d.year - d0.year) * 12 + (d.month - d0.month)
        for i in range(n):
            num = f"FAC-{d.strftime('%y%m%d')}-{mid[-2:]}-{i + 1:04d}"
            rel = f"fournisseurs/factures_xml/{mid}/{_suffixe(d)}/{num}.xml"
            if C.deja_fait(C.raw(cfg, rel), cfg):
                continue
            f = frn[ids_f[int(rng.integers(0, len(ids_f)))]]
            ids_p = np.array([k for k in par_frn[f["id_frn"]] if P["m_lanc"][k] <= mi])
            if len(ids_p) == 0:
                continue
            C.sortie_texte(cfg, sink, rel, "fournisseurs/factures_xml", facture_xml(rng, num, d, mid, f, P, PX["PA"], mi, ids_p), 1)
    sink.close()
    return 1


def tache_pdf(cfg):
    """Contrats fournisseurs et fiches techniques produit (une seule tâche : volume faible)."""
    sc = C.scope(cfg)
    P = C.charger_etat(cfg, "produits.parquet")
    FR = F.construire_fournisseurs(cfg)
    sink = C.Sink(cfg, "documents", "pdf")
    p0, p1 = C.periode_complete(cfg)
    for r in FR.iter_rows(named=True):
        for v in range(1, int(cfg["documents"]["contrats_par_fournisseur"]) + 1):
            rel = f"fournisseurs/contrats_pdf/contrat_{r['id_frn']}_v{v}.pdf"
            if C.deja_fait(C.raw(cfg, rel), cfg, compressible=False):
                continue
            rng = C.rng_for(cfg, "contrat", r["id_frn"], v)
            d = p0 - dt.timedelta(days=730) + dt.timedelta(days=int(rng.integers(0, 730 + (p1 - p0).days))) if v == 1 else p0 + dt.timedelta(days=int(rng.integers(0, (p1 - p0).days)))
            L = [f"CONTRAT D'APPROVISIONNEMENT N° {r['id_frn']}-{v:02d}", f"Conclu à Tunis le {d.strftime('%d/%m/%Y')}", "", "ENTRE LES SOUSSIGNÉS :",
                 f"{r['rs']}, matricule fiscal {r['mf']}, RIB {r['rib']}, ci-après « le Fournisseur »,", "ET : la société d'exploitation des magasins, ci-après « l'Enseigne ».", ""]
            for k in range(1, int(rng.integers(8, 20))):
                L += [CLAUSES[int(rng.integers(0, len(CLAUSES)))].format(n=k, j=int(rng.choice([30, 60, 90])), r=int(rng.integers(1, 6)))] + [""]
            L += ["Fait en deux exemplaires originaux.", "", "Signature du Fournisseur                                    Signature de l'Enseigne"]
            nb = C.ecrire_octets(C.raw(cfg, rel), C.pdf_texte(L, "Contrat " + r["id_frn"]), cfg, compressible=False)
            sink.put(rel, "fournisseurs/contrats_pdf", 1, nb)
    nv = int(cfg["documents"]["fiches_techniques_versions"])
    n_sel = P.height if not sc["sample"] else max(200, int(P.height * 0.25))
    for k in range(n_sel):
        row = P.row(k, named=True)
        for v in range(1, (nv if not sc["sample"] else 1) + 1):
            rel = f"fournisseurs/fiches_techniques_pdf/{row['frn']}/fiche_{row['code']}_v{v}.pdf"
            if C.deja_fait(C.raw(cfg, rel), cfg, compressible=False):
                continue
            rng = C.rng_for(cfg, "fiche", row["code"], v)
            cat = row["cat"]
            spec = {"alimentaire": ["Ingrédients : voir emballage. Allergènes : peut contenir des traces de gluten, lait, fruits à coque.", "Conservation : à l'abri de la chaleur et de l'humidité. DLC : 12 mois."],
                    "boissons": ["Composition : eau, sucre, arômes naturels. Conservation : après ouverture, 3 jours au réfrigérateur."],
                    "parapharmacie": ["Posologie : se conformer à la notice. Tenir hors de portée des enfants.", "Précautions : ne pas dépasser la dose recommandée."],
                    "hygiene": ["Précautions d'emploi : éviter le contact avec les yeux. Usage externe uniquement.", "Composition conforme à la réglementation cosmétique."],
                    "scolaire": ["Matériau : plastique et carton recyclables. Âge minimum : 3 ans."],
                    "textile": ["Composition : " + MATIERES[int(rng.integers(0, 5))] + ". Entretien : lavage à 30 °C, ne pas blanchir."],
                    "electromenager": ["Alimentation : 220-240 V ~ 50 Hz. Garantie : 24 mois.", "Classe énergétique : voir étiquette. Fabriqué sous licence."]}[cat]
            L = [f"FICHE TECHNIQUE — {row['base']} {row['marque']} {row['taille']}", f"Version {v} — Code article {row['code']} — EAN {row['ean']}", f"Fournisseur : {row['frn']}",
                 f"Conditionnement : {row['taille']}   TVA : {row['tva']} %", "Origine : " + ["Tunisie", "Tunisie", "Turquie", "Italie", "Chine", "France"][int(rng.integers(0, 6))], ""] + spec
            nb = C.ecrire_octets(C.raw(cfg, rel), C.pdf_texte(L, "Fiche technique"), cfg, compressible=False)
            sink.put(rel, "fournisseurs/fiches_techniques_pdf", 1, nb)
    sink.close()
    return 1


def run(cfg):
    sc = C.scope(cfg)
    w = int(cfg["max_workers"])
    C.run_pool(tache_pdf, [cfg], 1, "pdf")
    C.run_pool(tache_factures, [(cfg, mid, y, m) for mid in sc["magasins"] for (y, m) in C.mois_de(sc["debut"], sc["fin"])], w, "factures")
    return {}
