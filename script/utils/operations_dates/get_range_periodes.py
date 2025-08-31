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


def get_range_periodes(periode: str, periodicite: str) -> Dict[str, str]:
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
