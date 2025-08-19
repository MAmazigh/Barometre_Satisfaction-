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


def get_range_periodes(periode: str, periodicite: str) -> Dict:
    # Mois courant
    # strptime pour passer un string en date: strptime(string, format de lecture)
    dt_deb_mc = dt_fin_mc = dt_deb_mp = dt_fin_mp = dt_deb_ap = dt_fin_ap = None
    periode_dt = datetime.strptime(periode, '%Y%m')
    dt_month_start = periode_dt.date()
    if periodicite == 'M':
        # freq='M': End of Month - freq='MS' = Deb of Month
        dt_deb_mc = dt_month_start
        fin_mc = pd.date_range(start=dt_deb_mc, periods=1, freq='M')
        dt_fin_mc = fin_mc[0].date()
        #  Mois précédent
        dt_deb_mp = dt_deb_mc + relativedelta(months=-1)
        fin_mp = pd.date_range(start=dt_deb_mp, periods=1, freq='M')
        dt_fin_mp = fin_mp[0].date()
        #  Année précédente
        dt_deb_ap = dt_deb_mc + relativedelta(months=-12)
        fin_ap = pd.date_range(start=dt_deb_ap, periods=1, freq='M')
        dt_fin_ap = fin_ap[0].date()
    elif periodicite == 'Q':
        dt_deb_mc = dt_month_start + relativedelta(months=-2)
        fin_mc = pd.date_range(start=dt_month_start, periods=1, freq='M')
        dt_fin_mc = fin_mc[0].date()
        # Trimestre précédent
        dt_deb_mp = dt_deb_mc + relativedelta(months=-3)
        dt_fin_mp = dt_fin_mc + relativedelta(months=-3)
        # Année précédente
        dt_deb_ap = dt_deb_mc + relativedelta(months=-12)
        dt_fin_ap = dt_fin_mc + relativedelta(months=-12)
    elif periodicite == 'S':
        dt_deb_mc = dt_month_start + relativedelta(months=-5)
        fin_mc = pd.date_range(start=dt_month_start, periods=1, freq='M')
        dt_fin_mc = fin_mc[0].date()
        # Semestre précédent
        dt_deb_mp = dt_deb_mc + relativedelta(months=-6)
        dt_fin_mp = dt_fin_mc + relativedelta(months=-6)
        # Année précédente
        dt_deb_ap = dt_deb_mc + relativedelta(months=-12)
        dt_fin_ap = dt_fin_mc + relativedelta(months=-12)

    elif periodicite in ('P', 'Y'):
        if periodicite == 'P':
            dt_deb_mc = dt_month_start + relativedelta(months=-8)
        else:
            dt_deb_mc = dt_month_start + relativedelta(months=-11)

        fin_mc = pd.date_range(start=dt_month_start, periods=1, freq='M')
        dt_fin_mc = fin_mc[0].date()
        dt_deb_mp = datetime.now()
        dt_fin_mp = datetime.now()
        # Année précédente
        dt_deb_ap = dt_deb_mc + relativedelta(months=-12)
        dt_fin_ap = dt_fin_mc + relativedelta(months=-12)

    return {'debut_mc': dt_deb_mc.strftime('%d/%m/%Y'),
            'fin_mc': dt_fin_mc.strftime('%d/%m/%Y'),
            'debut_mp': dt_deb_mp.strftime('%d/%m/%Y'),
            'fin_mp': dt_fin_mp.strftime('%d/%m/%Y'),
            'debut_ap': dt_deb_ap.strftime('%d/%m/%Y'),
            'fin_ap': dt_fin_ap.strftime('%d/%m/%Y'),
            'dt_results_mc': dt_deb_mc.strftime('%B - %Y'),
            'dt_results_mp': dt_deb_mp.strftime('%B - %Y'),
            'dt_results_ap': dt_deb_ap.strftime('%B - %Y'),
            }
