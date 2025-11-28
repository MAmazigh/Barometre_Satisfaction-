# -*- coding: utf-8 -*-
"""
Fonction qui permet de récupérer la liste des périodicités à produire:
Elle se base sur le mois de production courant et renvoie une liste
"""
import pandas as pd
import numpy as np
import prince
from scipy.stats import t
from typing import List
from joblib import Parallel, delayed
import multiprocessing


def significativite_chevauchement(attribut_inf, attribut_sup, frontiere):
    """
    Détermine si l'intervalle de confiance de l'attribut est significativement différent
    de l'intervalle de la frontière (méthode de Windal).

    L'affectation est significative si les deux intervalles NE SE CHEVAUCHENT PAS.
    Retourne 1 pour Significatif (OK), 0 pour Non Significatif (KO).
    """
    # Chevauchement (Non Significatif / KO) : attribut_inf est avant frontiere_sup ET
    # attribut_sup est après frontiere_inf
    chevauchement = (frontiere >= attribut_inf) & (frontiere <= attribut_sup)
    # Non-Chevauchement (Significatif / OK)
    significatif_ok = ~chevauchement

    return np.where(significatif_ok, 1, 0)


def calculate_ic_proportion(p, n, t_value):
    # Fonction de calcul de l'Intervalle de Confiance pour une Proportion (méthode de Wald/Normal-approximée,
    # ajustée par T)
    se = np.sqrt(p * (1 - p) / n)
    # margin_of_error = 2 * t_value * se
    margin_of_error = t_value * se
    borne_inf = p - margin_of_error
    borne_sup = p + margin_of_error
    # Retourne un tuple de Series
    return borne_inf, borne_sup


# Fonction de discrétisation
def discretisation(df, colonnes, mode='2niveaux'):
    df_disc = df.copy()
    for col in colonnes:
        if mode == '3niveaux':
            col_disc = pd.cut(df[col], bins=[-1, 4, 7, 10],
                              labels=[f"{col}_faible", f"{col}_moyenne", f"{col}_forte"])
        elif mode == '2niveaux':
            # On considère satisfait les notes 9 et 10, les autres notes en insatisfait sauf nr
            col_disc = pd.cut(df[col], bins=[-1, 8, 10],
                              labels=[f"{col}_insatisfait", f"{col}_satisfait"])
        else:
            raise ValueError("Mode non reconnu.")
        df_disc[col] = col_disc.astype(object).fillna(f"{col}_NR")
    return df_disc


def calculer_tetraclasse_pour_groupe(df_groupe: pd.DataFrame, dichotomiser_func, liste_items: List[str]):
    """
    Fonction interne qui réalise le calcul Tétraclasse sur un sous-DataFrame (un groupe du groupby).
    """

    # --------------------------------------------------------------------------------
    # Analyse Tétra-Classe
    # --------------------------------------------------------------------------------

    # 1. Discrétisation et Passage en format long (Melt)
    # L'ID_VARS doit inclure les colonnes de groupement qui sont dans le DF_GROUPE

    # NOTE: df_groupe est déjà filtré par le groupement, on peut le traiter comme un DF global.
    df_disc = dichotomiser_func(df_groupe, ['satis_glob_cont'] + liste_items, mode='2niveaux')

    df_long = df_disc.melt(
        id_vars=['satis_glob_cont'],
        value_vars=liste_items,
        value_name='item_modalite'
    )
    df_long = df_long[~df_long['item_modalite'].str.endswith('_NR')]

    # ========================================================
    # CONTROLE DE SÉCURITÉ CONTRE LE SOUS-ÉCHANTILLON VIDE
    # ========================================================
    if df_long.empty:
        # Définir les colonnes attendues en sortie pour que pd.concat fonctionne
        # Ces colonnes doivent correspondre à celles de 'proba_conditionnelles'
        expected_cols = [
            'item', 'P_insat', 'P_satis', 'Freq_insat', 'Freq_satis',
            'frontiere_abscisse', 'frontiere_ordonnee', 'classe',
            'T_insat', 'T_satis', 'borne_inf_insat', 'borne_sup_insat',
            'borne_inf_satis', 'borne_sup_satis', 'signif_insat',
            'signif_satis', 'Significatif'
        ]

        # Retourner un DataFrame vide avec les colonnes correctes
        return pd.DataFrame(columns=expected_cols)

    # 2. Construction de la table de contingence (identique à l'original)
    table_globale = pd.crosstab(
        df_long['item_modalite'],
        df_long['satis_glob_cont'],
        margins=True,
        margins_name='Freq'
    )
    table_globale = table_globale.drop(index='Freq', axis=0, errors='ignore').fillna(0).astype(int)

    # ----------------------------------------------------
    # S'assurer que les colonnes de satisfaction existent
    # ----------------------------------------------------
    col_insat = 'satis_glob_cont_insatisfait'
    col_satis = 'satis_glob_cont_satisfait'

    # On crée les colonnes manquantes avec une valeur par défaut de 0
    if col_insat not in table_globale.columns:
        table_globale[col_insat] = 0
    if col_satis not in table_globale.columns:
        table_globale[col_satis] = 0

    # On s'assure que 'Freq' est la troisième colonne pour le renommage correct
    if 'Freq' not in table_globale.columns:
        # Ceci ne devrait pas arriver avec margins=True, mais par sécurité
        table_globale['Freq'] = table_globale[col_insat] + table_globale[col_satis]

    # Réordonner les colonnes avant le renommage pour éviter les erreurs d'ordre
    table_globale = table_globale[[col_insat, col_satis, 'Freq']]

    # Le renommage devient maintenant sûr (la longueur est garantie à 3)
    table_globale.columns = ['satis_glob_cont_insatisfait', 'satis_glob_cont_satisfait', 'Freq']

    # Profils Lignes
    table_globale["Row_Pct_insatisfait"] = table_globale["satis_glob_cont_insatisfait"] / table_globale["Freq"]
    table_globale["Row_Pct_satisfait"] = table_globale["satis_glob_cont_satisfait"] / table_globale["Freq"]

    table_contingence = table_globale.loc[:, ['satis_glob_cont_insatisfait', 'satis_glob_cont_satisfait']]
    profils_ligne = table_globale.loc[:, ['Row_Pct_insatisfait', 'Row_Pct_satisfait', 'Freq']]

    # 3. AFC, Frontières, et Classification
    afc = prince.CA(n_components=1)
    afc = afc.fit(table_contingence)

    valeur_propre_axe1 = afc.eigenvalues_[0] if len(afc.eigenvalues_) > 0 else 0
    coord_satisfaction = afc.column_coordinates(table_contingence)

    # Calcul des frontières (seuil de classification)
    coord_fact_insatis = coord_satisfaction.loc['satis_glob_cont_insatisfait', 0]
    coord_fact_satis = coord_satisfaction.loc['satis_glob_cont_satisfait', 0]

    # Gérer la division par zéro si les coordonnées sont identiques (cas rare)
    try:
        frontiere_abscisse = (coord_fact_insatis * np.sqrt(valeur_propre_axe1) - coord_fact_satis) / (
                coord_fact_insatis - coord_fact_satis)
    except ZeroDivisionError:
        frontiere_abscisse = np.nan

    try:
        frontiere_ordonnee = (coord_fact_satis * np.sqrt(valeur_propre_axe1) - coord_fact_insatis) / (
                coord_fact_satis - coord_fact_insatis)
    except ZeroDivisionError:
        frontiere_ordonnee = np.nan

    # Séparation et fusion des profils par modalité
    proba_insat = profils_ligne.loc[profils_ligne.index.str.endswith('_insatisfait')].rename(
        columns={'Row_Pct_insatisfait': 'P_insat', 'Freq': 'Freq_insat'}
    ).reset_index()
    proba_satis = profils_ligne.loc[profils_ligne.index.str.endswith('_satisfait')].rename(
        columns={'Row_Pct_satisfait': 'P_satis', 'Freq': 'Freq_satis'}
    ).reset_index()

    proba_insat['item'] = proba_insat['item_modalite'].str.replace('_insatisfait', '')
    proba_satis['item'] = proba_satis['item_modalite'].str.replace('_satisfait', '')

    proba_conditionnelles = pd.merge(proba_insat.set_index('item')[['P_insat', 'Freq_insat']],
                                     proba_satis.set_index('item')[['P_satis', 'Freq_satis']],
                                     left_index=True, right_index=True, how='outer')

    proba_conditionnelles["frontiere_abscisse"] = frontiere_abscisse
    proba_conditionnelles["frontiere_ordonnee"] = frontiere_ordonnee

    filtre_CLE = (proba_conditionnelles['P_insat'] >= frontiere_abscisse) & (
            proba_conditionnelles['P_satis'] >= frontiere_ordonnee)
    filtre_BASIQUE = (proba_conditionnelles['P_insat'] >= frontiere_abscisse) & (
            proba_conditionnelles['P_satis'] < frontiere_ordonnee)
    filtre_SECONDAIRE = (proba_conditionnelles['P_insat'] < frontiere_abscisse) & (
            proba_conditionnelles['P_satis'] < frontiere_ordonnee)
    filtre_PLUS = (proba_conditionnelles['P_insat'] < frontiere_abscisse) & (
            proba_conditionnelles['P_satis'] >= frontiere_ordonnee)

    condlist = [filtre_CLE, filtre_BASIQUE, filtre_SECONDAIRE, filtre_PLUS]
    choicelist = ['M', 'I', 'S', 'B']
    proba_conditionnelles['classe'] = np.select(condlist, choicelist, default='N/A')

    # Calcul des IC et significativité (nécessite les fonctions externes)
    alpha = 0.05
    alpha_half = alpha / 2
    proba_conditionnelles["T_insat"] = t.ppf(1 - alpha_half, proba_conditionnelles["Freq_insat"] - 1)
    proba_conditionnelles["T_satis"] = t.ppf(1 - alpha_half, proba_conditionnelles["Freq_satis"] - 1)

    bornes_insat = calculate_ic_proportion(proba_conditionnelles["P_insat"], proba_conditionnelles["Freq_insat"],
                                           proba_conditionnelles["T_insat"])
    proba_conditionnelles['borne_inf_insat'] = bornes_insat[0]
    proba_conditionnelles['borne_sup_insat'] = bornes_insat[1]

    bornes_satis = calculate_ic_proportion(proba_conditionnelles["P_satis"], proba_conditionnelles["Freq_satis"],
                                           proba_conditionnelles["T_satis"])
    proba_conditionnelles['borne_inf_satis'] = bornes_satis[0]
    proba_conditionnelles['borne_sup_satis'] = bornes_satis[1]

    proba_conditionnelles["signif_insat"] = significativite_chevauchement(proba_conditionnelles["borne_inf_insat"],
                                                                          proba_conditionnelles["borne_sup_insat"],
                                                                          proba_conditionnelles[
                                                                              "frontiere_abscisse"])
    proba_conditionnelles["signif_satis"] = significativite_chevauchement(proba_conditionnelles["borne_inf_satis"],
                                                                          proba_conditionnelles["borne_sup_satis"],
                                                                          proba_conditionnelles[
                                                                              "frontiere_ordonnee"])

    signif_satis_true = proba_conditionnelles["signif_satis"] == 1
    signif_insat_true = proba_conditionnelles["signif_insat"] == 1
    proba_conditionnelles["significatif"] = np.where(signif_satis_true & signif_insat_true, "Oui", "Non")

    return proba_conditionnelles.reset_index()


def modele_tetraclasse_windal_parallelise(df: pd.DataFrame,
                                          dichotomiser_func,
                                          liste_items: List[str],
                                          group_by_cols: List[str],
                                          seuil_min: int = 80,
                                          ) -> None:
    # Étape 1 : Créer les groupes et les listes d'arguments
    grouped = df.groupby(group_by_cols)

    # Calculer l'effectif de chaque groupe
    group_sizes = grouped.size()

    # Filtre préalable dans la fonction enveloppe pour identifier les groupes significatifs à conserver
    valid_groups = group_sizes[group_sizes >= seuil_min].index.tolist()
    group_keys = valid_groups

    # 2. Définir le nombre de cœurs à utiliser (ex: tous les cœurs disponibles - 1)
    # Probablement trop agressif en mémoire: MemoryError:
    # Unable to allocate 86.2 MiB for an array with shape (33, 342363) and data type float64
    n_cores = multiprocessing.cpu_count() - 1
    if n_cores <= 0:
        n_cores = 1

    # On limite le nombre de cœurs à 2 ou 3 au maximum
    # n_cores_disponibles = multiprocessing.cpu_count()
    # n_coeurs_prudents = 2
    # n_cores = max(1, min(n_cores_disponibles, n_coeurs_prudents))

    # 3. Exécuter en parallèle
    # delayed est un wrapper qui rend la fonction appelable en parallèle
    results_list = Parallel(n_jobs=n_cores)(delayed(calculer_tetraclasse_pour_groupe)
                                            (grouped.get_group(key),
                                             dichotomiser_func,
                                             liste_items)
                                            for key in group_keys
                                            )
    # 4. Concaténer les résultats
    final_df = pd.concat(results_list, keys=group_keys, names=group_by_cols).reset_index()

    # Nettoyage de l'index de niveau
    cols_to_drop = [col for col in final_df.columns if col.startswith('level_')]
    return final_df.drop(columns=cols_to_drop, errors='ignore')
