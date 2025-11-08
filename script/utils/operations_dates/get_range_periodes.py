# -*- coding: utf-8 -*-
"""
Fonction qui permet de récupérer les plages de date:
du mois courant. Permettra de créer un flag MC
du mois précédent. Permettra de créer un flag MP
du même mois de l'année précdente. Permettra de créer un flag AP
"""

from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
from typing import Dict
import calendar

def get_range_periodes_bis(periode: str, periodicite: str) -> Dict[str, str]:
    """
    Calcule les plages de dates pour le mois courant, le mois/trimestre/semestre précédent,
    et l'année précédente, en fonction de la périodicité.

    Paramètres :
    ----------
    periode : str
        Période au format 'yyyymm' (ex: '202508').
    periodicite : str
        Type de périodicité : 'M' (mois), 'Q' (trimestre), 'S' (semestre),
        'P' (spécifique), 'Y' (année).

    Retour :
    -------
    Dict[str, str]
        Dictionnaire contenant les dates de début/fin pour chaque période,
        ainsi que des libellés formatés.
    """

    # Conversion de la période en date
    # strptime pour passer un string en date: strptime(string, format de lecture)
    periode_dt = datetime.strptime(periode, '%Y%m')
    dt_month_start = periode_dt.date()

    # Initialisation des plages
    dt_deb_mc = dt_fin_mc = dt_deb_mp = dt_fin_mp = dt_deb_ap = dt_fin_ap = None

    # Calcul des plages selon la périodicité
    if periodicite == 'M':  # Mensuelle
        dt_deb_mc = dt_month_start
        dt_fin_mc = pd.date_range(start=dt_deb_mc, periods=1, freq='M')[0].date()

        dt_deb_mp = dt_deb_mc - relativedelta(months=1)
        dt_fin_mp = pd.date_range(start=dt_deb_mp, periods=1, freq='M')[0].date()

        dt_deb_ap = dt_deb_mc - relativedelta(months=12)
        dt_fin_ap = pd.date_range(start=dt_deb_ap, periods=1, freq='M')[0].date()

    elif periodicite == 'Q':  # Trimestrielle
        dt_deb_mc = dt_month_start - relativedelta(months=2)
        dt_fin_mc = pd.date_range(start=dt_month_start, periods=1, freq='M')[0].date()

        dt_deb_mp = dt_deb_mc - relativedelta(months=3)
        dt_fin_mp = dt_fin_mc - relativedelta(months=3)

        dt_deb_ap = dt_deb_mc - relativedelta(months=12)
        dt_fin_ap = dt_fin_mc - relativedelta(months=12)

    elif periodicite == 'S':  # Semestrielle
        dt_deb_mc = dt_month_start - relativedelta(months=5)
        dt_fin_mc = pd.date_range(start=dt_month_start, periods=1, freq='M')[0].date()

        dt_deb_mp = dt_deb_mc - relativedelta(months=6)
        dt_fin_mp = dt_fin_mc - relativedelta(months=6)

        dt_deb_ap = dt_deb_mc - relativedelta(months=12)
        dt_fin_ap = dt_fin_mc - relativedelta(months=12)

    elif periodicite in ('P', 'Y'):  # Spécifique ou annuelle
        dt_deb_mc = dt_month_start - relativedelta(months=8 if periodicite == 'P' else 11)
        dt_fin_mc = pd.date_range(start=dt_month_start, periods=1, freq='M')[0].date()

        # Mois précédent non applicable ici
        dt_deb_mp = dt_fin_mp = datetime.now().date()

        dt_deb_ap = dt_deb_mc - relativedelta(months=12)
        dt_fin_ap = dt_fin_mc - relativedelta(months=12)

    # Formatage des résultats
    def fmt(date_obj):
        return date_obj.strftime('%d/%m/%Y')

    def label(date_obj):
        return date_obj.strftime('%B - %Y')

    return {
        'debut_mc': fmt(dt_deb_mc),
        'fin_mc': fmt(dt_fin_mc),
        'debut_mp': fmt(dt_deb_mp),
        'fin_mp': fmt(dt_fin_mp),
        'debut_ap': fmt(dt_deb_ap),
        'fin_ap': fmt(dt_fin_ap),
        'dt_results_mc': label(dt_deb_mc),
        'dt_results_mp': label(dt_deb_mp),
        'dt_results_ap': label(dt_deb_ap),
    }


def get_range_periodes(periode: str, periodicite: str) -> Dict[str, str]:
    """
    Calcule les plages de dates de début et de fin pour la période courante (MC),
    la période précédente (MP), et l'année précédente (AP), en fonction de la périodicité.

    Cette version utilise une logique basée sur les débuts de mois, facilitant
    le calcul des fins de mois et des décalages.

    Paramètres :
    ----------
    periode : str
        Période de référence au format 'yyyymm' (ex: '202508').
    periodicite : str
        Type de périodicité : 'M' (mois), 'Q' (trimestre), 'S' (semestre),
        'Y' (année). 'P' (spécifique) est fusionné avec 'Y' pour simplifier.

    Retour :
    -------
    Dict[str, str]
        Dictionnaire contenant les dates de début/fin formatées ('%d/%m/%Y')
        pour chaque période, ainsi que des libellés formatés ('%B - %Y').
    """

    # 1. PRÉPARATION DE LA PÉRIODE DE RÉFÉRENCE
    try:
        # On se place au premier jour du mois de la période donnée
        dt_start_of_period = datetime.strptime(periode, '%Y%m').replace(day=1).date()
    except ValueError:
        raise ValueError("Le format de la période doit être 'YYYYMM'.")

    # Définition des décalages de durée (en mois) pour définir le DEBUT de la période courante
    # ex: Mensuel (M) : 0 mois avant le dt_start_of_period
    # ex: Trimestriel (Q) : 2 mois avant le dt_start_of_period pour couvrir 3 mois
    period_lengths = {
        'M': 0,
        'Q': 2,
        'S': 5,
        'Y': 11,
        'P': 8,  # Garder 'P' pour l'ancienne logique
    }

    if periodicite not in period_lengths:
        raise ValueError(f"Périodicité '{periodicite}' non supportée. Utilisez M, Q, S, Y ou P.")

    # Calcul du décalage pour définir le début de la période courante (MC)
    offset_months_mc = period_lengths[periodicite]

    # --- 2. CALCUL DES PLAGES DE DATES DE DÉBUT ---

    # Début du Mois Courant / Période Courante (MC)
    dt_deb_mc = dt_start_of_period - relativedelta(months=offset_months_mc)

    # Début du Mois Précédent / Période Précédente (MP)
    # On recule d'une période complète (1M, 3M, 6M, 12M)
    months_in_period = offset_months_mc + 1
    dt_deb_mp = dt_deb_mc - relativedelta(months=months_in_period)

    # Début de l'Année Précédente (AP)
    dt_deb_ap = dt_deb_mc - relativedelta(years=1)

    # --- 3. CALCUL DES PLAGES DE DATES DE FIN ---

    # La date de fin de la période courante est le dernier jour du mois de référence
    dt_fin_mc = dt_start_of_period + relativedelta(
        day=calendar.monthrange(dt_start_of_period.year, dt_start_of_period.month)[1])

    # Date de fin de la Période Précédente (MP)
    # C'est le jour avant le début de MC
    dt_fin_mp = dt_deb_mc - relativedelta(days=1)

    # Date de fin de l'Année Précédente (AP)
    # C'est le jour avant le début de MP
    dt_fin_ap = dt_deb_ap + relativedelta(months=months_in_period) - relativedelta(days=1)

    # --- 4. FORMATAGE DES RÉSULTATS ---

    def fmt_date(date_obj):
        # Format pour les requêtes SQL/filtres de données
        return date_obj.strftime('%d/%m/%Y')

    def fmt_label(date_obj):
        # Format pour l'affichage dans les rapports (ex: "Septembre - 2025")
        # Note: Cette fonction renvoie le mois/année du DEBUT de la période
        # Pour les périodes longues (T/S/Y), le libellé doit être affiné dans l'application
        return date_obj.strftime('%B - %Y')

    return {
        'debut_mc': fmt_date(dt_deb_mc),
        'fin_mc': fmt_date(dt_fin_mc),
        'debut_mp': fmt_date(dt_deb_mp),
        'fin_mp': fmt_date(dt_fin_mp),
        'debut_ap': fmt_date(dt_deb_ap),
        'fin_ap': fmt_date(dt_fin_ap),
        'dt_results_mc': fmt_label(dt_deb_mc),
        'dt_results_mp': fmt_label(dt_deb_mp),
        'dt_results_ap': fmt_label(dt_deb_ap),
    }