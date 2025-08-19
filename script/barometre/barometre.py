# -*- coding: utf-8 -*-
"""
Classe de production des rapports
"""

import os
import pandas as pd
import numpy as np
import datetime
from typing import Dict

from get_paths import get_current_path, get_path
from operations_dates import get_dict_periodicite, get_liste_periodicite
from sql_operations import SqlOperations
from sql_schema import get_schema_from_json
from sqlalchemy import text
from ordered_set import OrderedSet


class Barometre(SqlOperations):
    # view and production thresholds of kpi's. When kpi lower than threshold then 'NS', 'NI' if NULL
    # when frequency lower than threshold then no production
    threshold_NI_NS = threshold_production = 80

    def __init__(self, type_rapport: str = 'global', periode: str = '201405') -> None:
        """
        On aura 3 types de rapports :
        Global:global,
        Temps: etapes,
        Partenaire: fidelia

        :param type_rapport: global, etapes, fidelia
        :param periode: période de lancement du baromètre

        """
        # SqlOperations.__init__(self)
        super().__init__()
        self.sql_operations = SqlOperations()
        self.path_parameters = self.get_path_parameters()
        self.periode = periode
        self.type_rapport = type_rapport

    @staticmethod
    def get_path_parameters() -> Dict:
        parameters = dict()
        parameters['current_path'] = get_current_path()
        parameters['sql_files'] = get_path(get_current_path(), fic_name='sql_files')
        parameters['json_schema'] = get_path(get_current_path(), fic_name='json_schema')
        parameters['json_queries'] = get_path(get_current_path(), fic_name='barometre.json')
        return parameters

    def run(self) -> None:

        # print(f'Execute pre_production debut {datetime.datetime.now()}...........')
        # self.pre_production()
        # print(f'Execute pre_production fin {datetime.datetime.now()}...........')
        # print(f'Execute extraction debut {datetime.datetime.now()}...........')
        # self.extraction()
        # print(f'Execute extraction fin {datetime.datetime.now()}...........')
        # print(f'Execute calculs debut {datetime.datetime.now()}...........')
        # self.calculs()
        # print(f'Execute calculs fin {datetime.datetime.now()}...........')
        # Mise en forme et sortie sous excel-pdf-html ou à plat pour power bi ?
        print(f'Execute restitution debut {datetime.datetime.now()}...........')
        self.restitution()
        print(f'Execute restitution fin {datetime.datetime.now()}...........')

    def pre_production(self):
        """
        insertion dans sql de fichiers: structure ou autre fichier
        creation de table de reference pour la production

        """
        print('Execute insert_structure debut...........')
        self.insert_structure()
        print('Execute insert_structure fin.............')
        print('Execute table_reference debut............')
        self.table_reference()
        print('Execute table_reference fin..............')

    def insert_structure(self) -> None:
        """
        A partir du chemin du dossier courant (baromètre), on va définir le path
        vers le dossier qui contient le fichier json que l'on veut importer.
        on utilise pour cela les librairies os et pathlib.
        rglob permet de faire une recherche récursive dans les sous dossiers.

        :return:
        """
        # path_parameters = self.get_path_parameters()

        # on récupère le schéma sous forme de liste de tuple et les colonnes sous forme de liste
        json_schema_path = self.get_path_parameters()['json_schema']
        insert_queries_path = os.path.join(self.get_path_parameters()['sql_files'], 'insertion_tables_queries')

        path_to_json_structure = os.path.join(json_schema_path, 'structure.json')
        schema_columns = get_schema_from_json(path_to_json_structure)
        schema_tuple, columns_list = schema_columns[0], schema_columns[1]
        # transforme la liste tuples pour passer un str dans la requête
        schema = ",  ".join(list(map(lambda tp: " ".join(tp), schema_tuple)))
        columns = ",  ".join(columns_list)

        ficname = ''.join(['structure_', self.periode, '.csv'])
        path_to_structure_csv = self.path_to_database + ficname

        query = self.sql_operations.read_query(insert_queries_path, "insert_structure.sql",
                                               format=dict(table='structure',
                                                           schema_structure_from_json=schema,
                                                           columns_from_json=columns,
                                                           path_to_structure_csv=path_to_structure_csv,
                                                           delimiter="';'"))

        self.sql_operations.execute_query(query)

    def table_reference(self) -> None:
        """
        execute la requete table_reference.sql pour tous les niveaux à produire
        """
        iterator = self.get_parameters_table()
        iterator['niveau_inf'] = iterator['niveau'] - 1
        # defining path to query
        query_path = os.path.join(self.get_path_parameters()['sql_files'], 'insertion_tables_queries')

        # read query and pass parameters
        iterator['query'] = iterator.apply(lambda row: self.sql_operations.read_query(query_path, 'table_reference.sql',
                                                                                      format=row.to_dict()), axis=1)
        # then execute queries
        for query in iterator['query'].values.tolist():
            self.sql_operations.execute_query(query)

    def get_parameters_table(self, level: int = 1) -> pd.DataFrame:
        """
        En fonction de la période passée en paramètre on aura différentes périodicités :
        Month            : mensuelle pour tous les mois
        Quarter          : trimestrielle pour les mois 3, 6, 9, 12
        Semiyear         : semestrielle pour les mois 6 et 12
        Year             : annuelle pour le mois 12
        PeriodeSpecifique: période spécifique pour le mois 9

        On définira un axe periode :
        pour les productions M, Q, S
        indic =  MC pour la période mois courant
        indic =  MP pour la période mois précédent
        indic =  AP pour la période même mois de l'année précédente

        pour les productions Y et PS:
        indic =  MC pour la période mois courant
        indic =  AP pour la période même mois de l'année précédente

        Pour chacune des périodes à produire, on devra caculer les effectifs répondant de chaque entité:
        si ils sont supérieurs à un seuil de 80, on produira un rapport.
        si ils sont inférieurs à un seuil de 80,on ne produira pas de rapport.
        :return:
        """
        liste_periodicite = get_liste_periodicite(self.periode)
        dic_periodicite = get_dict_periodicite(self.periode, liste_periodicite)
        period_table = pd.DataFrame(dic_periodicite).T
        # add a key to perform cross join merge to add the levels to product
        period_table['key'] = 0
        period_table.rename_axis('period', axis=0, inplace=True)
        period_table.reset_index(inplace=True)
        # levels to product : 5, 4, 3, 2
        df_level = pd.DataFrame(dict(key=0, niveau=range(5, level, -1)))
        # perform a cross join to add level to product
        parameters_table = pd.merge(df_level, period_table, on='key', how='outer')
        return parameters_table

    def build_extraction_page2to9(self) -> None:
        # We base our loops on a lookup table
        iterator = self.get_parameters_table(level=1)
        iterator['flag_mc'] = iterator.apply(
            lambda row: f"WHEN date_reponse BETWEEN '{row['debut_mc']}' AND '{row['fin_mc']}' THEN 'MC'", axis=1)

        iterator['flag_mp'] = np.where(iterator.period.isin(['Y', 'P']), '', iterator.apply(
            lambda row: f"WHEN date_reponse BETWEEN '{row['debut_mp']}' AND '{row['fin_mp']}' THEN 'MP'", axis=1))

        iterator['flag_ap'] = iterator.apply(
            lambda row: f"WHEN date_reponse BETWEEN '{row['debut_ap']}' AND '{row['fin_ap']}' THEN 'AP'", axis=1)

        iterator['where_mc'] = iterator.apply(
            lambda row: f"date_reponse BETWEEN '{row['debut_mc']}' AND '{row['fin_mc']}' OR ", axis=1)

        iterator['where_mp'] = np.where(iterator.period.isin(['Y', 'P']), '', iterator.apply(
            lambda row: f"date_reponse BETWEEN '{row['debut_mp']}' AND '{row['fin_mp']}' OR ", axis=1))

        iterator['where_ap'] = iterator.apply(
            lambda row: f"date_reponse BETWEEN '{row['debut_ap']}' AND '{row['fin_ap']}' ", axis=1)

        # defining path to query
        extraction_queries_path = os.path.join(self.get_path_parameters()['sql_files'], 'extraction_queries')
        # loop from 2 to 9
        for p in range(2, 10):
            # read query and pass parameters
            iterator['query'] = iterator.apply(lambda row: self.sql_operations.read_query(extraction_queries_path,
                                                                                          f'extraction_page{p}.sql',
                                                                                          format=row.to_dict()), axis=1)
            # then execute queries
            for query in iterator['query'].values.tolist():
                self.sql_operations.execute_query(query)
            # pages 6 to 9 need two extractions of data for the rightness of results
            # it is True only for the 3rd level
            if p > 5:
                # read query and pass parameters
                iterator['query'] = np.where(iterator.niveau == 3, iterator.apply(
                    lambda row: self.sql_operations.read_query(extraction_queries_path,
                                                               f'extraction_page{p}_mcv.sql',
                                                               format=row.to_dict()), axis=1), '')
                # then execute queries
                for query in iterator['query'].values.tolist():
                    if query != '':
                        self.sql_operations.execute_query(query)

    def extraction(self):
        """
        requêtes d'extraction des data par page
        page 2 à 9 pour le bloc commun
        page 10 et plus pour les spécificités qui sont produites à partir du niveau 3
        A partir du niveau 3, on aura deux extractions pour la justesse des calculs par mode de contact valide
        :return:
        """

        self.build_extraction_page2to9()

    def build_calculs_page2(self) -> None:
        # We base our loops on a lookup table
        iterator = self.get_parameters_table()

        iterator['calcul_freq_mp'] = np.where(iterator.period.isin(['Y', 'P']), '', iterator.apply(
            lambda row: f",SUM(CASE WHEN indic= 'MP' THEN freq END) OVER (PARTITION BY N{row['niveau']}_C_ENTITE, Q1 "
                        f"ORDER BY N{row['niveau']}_C_ENTITE, Q1) AS freq_MP ",
            axis=1))
        iterator['calcul_indicateur_mp'] = np.where(iterator.period.isin(['Y', 'P']), '', iterator.apply(
            lambda row: f",SUM(CASE WHEN indic= 'MP' THEN indicateur END) OVER (PARTITION BY N{row['niveau']}_C_ENTITE,"
                        f" Q1 ORDER BY N{row['niveau']}_C_ENTITE, Q1) AS indicateur_MP ",
            axis=1))
        iterator['freq_mp'] = np.where(iterator.period.isin(['Y', 'P']), '', ', d.freq_MP')

        iterator['indicateur_mp'] = np.where(iterator.period.isin(['Y', 'P']), '', ', d.indicateur_MP')

        # defining path to query
        calculs_queries_path = os.path.join(self.get_path_parameters()['sql_files'], 'calculs_queries')

        # read query and pass parameters
        iterator['query'] = iterator.apply(lambda row: self.sql_operations.read_query(calculs_queries_path,
                                                                                      f'calculs_evol_page2.sql',
                                                                                      format=row.to_dict()), axis=1)
        # then execute queries
        for query in iterator['query'].values.tolist():
            self.sql_operations.execute_query(query)

    def build_calculs_page3(self) -> None:
        # We base our loops on a lookup table
        iterator = self.get_parameters_table()

        iterator['calcul_freq_mp'] = np.where(iterator.period.isin(['Y', 'P']), '', iterator.apply(
            lambda row: f", SUM(CASE WHEN indic= 'MP' THEN freq END) OVER (PARTITION BY N{row['niveau']}_C_ENTITE, "
                        f"satisfaction_contact ORDER BY N{row['niveau']}_C_ENTITE, satisfaction_contact) AS freq_MP ",
            axis=1))
        iterator['calcul_indicateur_mp'] = np.where(iterator.period.isin(['Y', 'P']), '', iterator.apply(
            lambda row: f", SUM(CASE WHEN indic= 'MP' THEN indicateur END) OVER (PARTITION BY N{row['niveau']}_C_ENTITE"
                        f", satisfaction_contact ORDER BY N{row['niveau']}_C_ENTITE, satisfaction_contact) "
                        f"AS indicateur_MP ", axis=1))
        iterator['freq_mp'] = np.where(iterator.period.isin(['Y', 'P']), '', ', d.freq_MP')

        iterator['indicateur_mp'] = np.where(iterator.period.isin(['Y', 'P']), '', ', d.indicateur_MP')

        # defining path to query
        calculs_queries_path = os.path.join(self.get_path_parameters()['sql_files'], 'calculs_queries')

        # read query and pass parameters
        iterator['query'] = iterator.apply(lambda row: self.sql_operations.read_query(calculs_queries_path,
                                                                                      f'calculs_evol_page3.sql',
                                                                                      format=row.to_dict()), axis=1)
        # then execute queries
        for query in iterator['query'].values.tolist():
            self.sql_operations.execute_query(query)

    def build_calculs_page4(self) -> None:
        # We base our loops on a lookup table
        iterator = self.get_parameters_table()

        iterator['calcul_freq_mp'] = np.where(iterator.period.isin(['Y', 'P']), '', iterator.apply(
            lambda row: f",SUM(CASE WHEN indic= 'MP' THEN freq END) OVER (PARTITION BY N{row['niveau']}_C_ENTITE, "
                        f"prestation_impact ORDER BY N{row['niveau']}_C_ENTITE, prestation_impact) AS freq_MP ",
            axis=1))
        iterator['calcul_indicateur_mp'] = np.where(iterator.period.isin(['Y', 'P']), '', iterator.apply(
            lambda row: f",SUM(CASE WHEN indic= 'MP' THEN indicateur END) OVER (PARTITION BY N{row['niveau']}_C_ENTITE,"
                        f" prestation_impact ORDER BY N{row['niveau']}_C_ENTITE, prestation_impact) AS indicateur_MP ",
            axis=1))
        iterator['freq_mp'] = np.where(iterator.period.isin(['Y', 'P']), '', ', d.freq_MP')

        iterator['indicateur_mp'] = np.where(iterator.period.isin(['Y', 'P']), '', ', d.indicateur_MP')

        # defining path to query
        calculs_queries_path = os.path.join(self.get_path_parameters()['sql_files'], 'calculs_queries')

        # read query and pass parameters
        iterator['query'] = iterator.apply(lambda row: self.sql_operations.read_query(calculs_queries_path,
                                                                                      f'calculs_evol_page4.sql',
                                                                                      format=row.to_dict()), axis=1)
        # then execute queries
        for query in iterator['query'].values.tolist():
            self.sql_operations.execute_query(query)

    def build_calculs_page5(self) -> None:
        # We base our loops on a lookup table
        iterator = self.get_parameters_table()

        iterator['calcul_freq_mp'] = np.where(iterator.period.isin(['Y', 'P']), '', iterator.apply(
            lambda row: f",SUM(CASE WHEN indic= 'MP' THEN freq END) OVER (PARTITION BY N{row['niveau']}_C_ENTITE, "
                        f"mode_contact_valide ORDER BY N{row['niveau']}_C_ENTITE, mode_contact_valide) AS freq_MP ",
            axis=1))
        iterator['calcul_indicateur_mp'] = np.where(iterator.period.isin(['Y', 'P']), '', iterator.apply(
            lambda row: f",SUM(CASE WHEN indic= 'MP' THEN indicateur END) OVER (PARTITION BY N{row['niveau']}_C_ENTITE,"
                        f" mode_contact_valide ORDER BY N{row['niveau']}_C_ENTITE,mode_contact_valide) AS indicateur_MP"
            , axis=1))
        iterator['freq_mp'] = np.where(iterator.period.isin(['Y', 'P']), '', ', d.freq_MP')

        iterator['indicateur_mp'] = np.where(iterator.period.isin(['Y', 'P']), '', ', d.indicateur_MP')

        # defining path to query
        calculs_queries_path = os.path.join(self.get_path_parameters()['sql_files'], 'calculs_queries')

        # read query and pass parameters
        iterator['query'] = iterator.apply(lambda row: self.sql_operations.read_query(calculs_queries_path,
                                                                                      f'calculs_evol_page5.sql',
                                                                                      format=row.to_dict()), axis=1)
        # then execute queries
        for query in iterator['query'].values.tolist():
            self.sql_operations.execute_query(query)

    def build_calculs_page6to9(self) -> None:
        # We base our loops on a lookup table
        for p in range(6, 10):
            print(f'Execute build_calculs_page{p} ...........')
            iterator = self.get_parameters_table(level=2)
            iterator['suffixe'] = ''
            iterator['mcv'] = ''
            iterator['table_input'] = iterator.apply(lambda row: f"extraction_N{row.niveau}_{row.period}_page{p}",
                                                     axis=1)

            iterator['calcul_freq_mp'] = np.where(iterator.period.isin(['Y', 'P']), '',
                                                  iterator.apply(
                                                      lambda row: f",SUM(CASE WHEN indic= 'MP' THEN freq END) "
                                                                  f"OVER (PARTITION BY N{row['niveau']}_C_ENTITE, "
                                                                  f"col_name ORDER BY N{row['niveau']}_C_ENTITE, "
                                                                  f"col_name) AS freq_MP ",
                                                      axis=1))
            iterator['calcul_indicateur_mp'] = np.where(iterator.period.isin(['Y', 'P']), '',
                                                        iterator.apply(
                                                            lambda row: f",SUM(CASE WHEN indic= 'MP' THEN indicateur "
                                                                        f"END) OVER (PARTITION BY "
                                                                        f" N{row['niveau']}_C_ENTITE, col_name "
                                                                        f"ORDER BY N{row['niveau']}_C_ENTITE, col_name)"
                                                                        f" AS indicateur_MP ",
                                                            axis=1))
            iterator['freq_mp'] = np.where(iterator.period.isin(['Y', 'P']), '', ', d.freq_MP')

            iterator['indicateur_mp'] = np.where(iterator.period.isin(['Y', 'P']), '', ', d.indicateur_MP')

            # defining path to query
            calculs_queries_path = os.path.join(self.get_path_parameters()['sql_files'], 'calculs_queries')

            # read query and pass parameters
            iterator['query'] = iterator.apply(lambda row: self.sql_operations.read_query(calculs_queries_path,
                                                                                          f'calculs_evol_page{p}.sql',
                                                                                          format=row.to_dict()), axis=1)
            # then execute queries
            for query in iterator['query'].values.tolist():
                self.sql_operations.execute_query(query)

            # level 3 only product mcv
            iterator = iterator.query(' niveau == 3 ').copy()
            iterator['suffixe'] = 'mcv'
            iterator['mcv'] = ', mode_contact_valide'
            iterator['table_input'] = iterator.apply(lambda row: f"extraction_N{row.niveau}_{row.period}_page{p}_mcv",
                                                     axis=1)
            iterator['calcul_freq_mp'] = np.where(iterator.period.isin(['Y', 'P']), '',
                                                  iterator.apply(lambda
                                                                     row: f",SUM(CASE WHEN indic= 'MP' THEN freq END) "
                                                                          f"OVER (PARTITION BY "
                                                                          f"N{row['niveau']}_C_ENTITE, "
                                                                          f"mode_contact_valide, col_name "
                                                                          f"ORDER BY N{row['niveau']}_C_ENTITE, "
                                                                          f"mode_contact_valide, col_name) AS freq_MP ",
                                                                 axis=1))
            iterator['calcul_indicateur_mp'] = np.where(iterator.period.isin(['Y', 'P']), '',
                                                        iterator.apply(lambda row: f",SUM(CASE WHEN indic= 'MP' THEN "
                                                                                   f"indicateur END) OVER (PARTITION BY"
                                                                                   f" N{row['niveau']}_C_ENTITE, "
                                                                                   f"mode_contact_valide, col_name "
                                                                                   f"ORDER BY N{row['niveau']}_C_ENTITE"
                                                                                   f", mode_contact_valide, col_name) "
                                                                                   f"AS indicateur_MP ",
                                                                       axis=1))

            iterator['query'] = iterator.apply(lambda row: self.sql_operations.read_query(calculs_queries_path,
                                                                                          f'calculs_evol_page{p}.sql',
                                                                                          format=row.to_dict()), axis=1)
            # then execute queries
            for query in iterator['query'].values.tolist():
                self.sql_operations.execute_query(query)

    def build_format_calculs_page2to5(self) -> None:
        """
        We base our loops on a lookup table
        We need to include the 3 temporal flag of period : current 'MC', preceding 'MP', last year 'AP'
        Pages 6 to 9 do not restitute preceding and last year periods MP and AP
        :return:
        """

        df = self.get_parameters_table(level=1)
        # on ajoute le flag des periodes courante, precedente, année précédente sur lesquelles on bouclera
        df_indic = pd.DataFrame(dict(key=0, indic=['MC', 'MP', 'AP']))
        iterator = pd.merge(df, df_indic, on='key', how='outer')
        # pas de periode precedente en production speciale ou annuelle
        filtre_period = iterator.period.isin(['Y', 'P'])
        filtre_indic = iterator.indic == 'MP'
        iterator['indic'] = np.where(filtre_period & filtre_indic, '', iterator['indic'])
        iterator = iterator.query(" indic !='' ")

        iterator['freq'] = iterator.apply(lambda row: f"freq_{row['indic']}", axis=1)
        iterator['threshold_NI_NS'] = self.threshold_NI_NS
        iterator['indicateur'] = iterator.apply(lambda row: f"indicateur_{row['indic']}", axis=1)

        iterator['fmt_evol'] = np.where(iterator.indic.isin(['MP', 'AP']), '',
                                        ",CASE WHEN evol_indicateur < -1.96 THEN '-' "
                                        "      WHEN evol_indicateur BETWEEN -1.96 AND 1.96 THEN '='"
                                        "      WHEN evol_indicateur > 1.96 THEN '+' "
                                        " END as valeurs_e")

        iterator['kpi'] = np.where(iterator.indic.isin(['MP', 'AP']), ', kpi',
                                   ", CASE WHEN kpi in ('NI', 'NS') THEN kpi ELSE kpi||'|'||valeurs_e END as kpi")

        iterator['flag_periode'] = iterator.apply(lambda row: f", '{row.indic}' AS periode", axis=1)
        # defining path to query
        calculs_queries_path = os.path.join(self.get_path_parameters()['sql_files'], 'calculs_queries')

        # loop from level 2 to 5
        for p in range(2, 6):
            # We concatenate our table by periods (MC, MP, AP) in one table by level
            cols_to_keep = ['niveau', 'period', 'indic']
            iterator_for_output = iterator[cols_to_keep].copy()
            iterator_for_output['output'] = iterator_for_output.apply(
                lambda row: f" format_N{row.niveau}_{row.period}_{row.indic}_page{p}", axis=1)
            iterator_for_output = iterator_for_output.pivot_table(index=['niveau', 'period'], columns='indic',
                                                                  values='output', aggfunc='max')
            iterator_for_output.reset_index(inplace=True)
            # reshape dataframe for final query output table
            iterator_for_output['query'] = iterator_for_output.apply(
                lambda row: f"DROP TABLE IF EXISTS format_calculs_N{row.niveau}_{row.period}_page{p}; " \
                            f"SELECT * INTO format_calculs_N{row.niveau}_{row.period}_page{p} FROM {row.MC} " \
                            f"UNION ALL SELECT * FROM {row.MP} UNION ALL SELECT * FROM {row.AP} ",
                axis=1)

            # cleaning tables
            iterator_for_output['query_drop_table'] = iterator_for_output.apply(
                lambda row: f" DROP TABLE IF EXISTS {row.MC}, {row.MP}, {row.AP} ;", axis=1)
            iterator_for_output['query_drop_table_input'] = iterator_for_output.apply(
                lambda row: f" DROP TABLE calculs_N{row.niveau}_{row.period}_page{p} ;", axis=1)

            # read query and pass parameters
            iterator['query'] = iterator.apply(lambda row: self.sql_operations.read_query(calculs_queries_path,
                                                                                          f'formatage_evol_page{p}.sql',
                                                                                          format=row.to_dict()), axis=1)
            # then execute queries
            for query in iterator['query'].values.tolist():
                self.sql_operations.execute_query(query)
            # Then execute union tables queries
            for query in iterator_for_output['query'].values.tolist():
                self.sql_operations.execute_query(query)
            # Then execute drop table queries
            for query in iterator_for_output['query_drop_table'].values.tolist():
                self.sql_operations.execute_query(query)
            # Finaly execute drop table input queries
            for query in iterator_for_output['query_drop_table_input'].values.tolist():
                self.sql_operations.execute_query(query)

    def build_format_calculs_page6to9(self) -> None:
        # We base our loops on a lookup table
        for p in range(6, 10):
            print(f'Execute build_format_calculs_page{p} debut...........')
            iterator = self.get_parameters_table(level=2)
            iterator['table_input'] = iterator.apply(lambda row: f"calculsa_N{row.niveau}_{row.period}_page{p}", axis=1)
            iterator['table_output'] = iterator.apply(lambda row: f"format_calculsa_N{row.niveau}_{row.period}_page{p}",
                                                      axis=1)
            list_item_pct_p8 = "'item_03700', 'item_03910', 'item_03930', 'item_03940', 'item_03950', 'item_03960', " \
                               "'item_03970' "
            list_item_pct_p6 = " 'item_01300', 'item_01400' "
            iterator['list_item_pct'] = np.where(p == 8, list_item_pct_p8, list_item_pct_p6)
            iterator['threshold_NI_NS'] = self.threshold_NI_NS
            # defining path to query
            calculs_queries_path = os.path.join(self.get_path_parameters()['sql_files'], 'calculs_queries')

            # read query and pass parameters
            iterator['query'] = iterator.apply(lambda row: self.sql_operations
                                               .read_query(calculs_queries_path, f'formatage_evol_page6to9.sql',
                                                           format=row.to_dict()), axis=1)
            # then execute queries
            for query in iterator['query'].values.tolist():
                self.sql_operations.execute_query(query)

            # only level 3 products mcv fields
            iterator = iterator.query(' niveau == 3 ').copy()
            iterator['mcv'] = ', mode_contact_valide'
            iterator['table_input_mcv'] = iterator.apply(lambda row: f"calculsa_N{row.niveau}_{row.period}_page{p}mcv",
                                                         axis=1)
            iterator['table_output_mcv'] = iterator.apply(
                lambda row: f"format_calculsa_N{row.niveau}_{row.period}_page{p}mcv",
                axis=1)
            iterator['final_table'] = iterator.apply(
                lambda row: f"format_calculs_N{row.niveau}_{row.period}_page{p}",
                axis=1)
            # read query and pass parameters
            iterator['query'] = iterator.apply(lambda row: self.sql_operations
                                               .read_query(calculs_queries_path, f'formatage_evol_page6to9mcv.sql',
                                                           format=row.to_dict()), axis=1)
            # then execute queries
            for query in iterator['query'].values.tolist():
                self.sql_operations.execute_query(query)

    def build_format_calculs_page6to9_level5to4(self) -> None:
        # We base our loops on a lookup table this time on level instead of page
        for level in range(5, 3, -1):
            iterator = self.get_parameters_table(level=2)
            # we add page 6 to 9 as field in the dataframe to pass them in our parameters
            df_page = pd.DataFrame(dict(key=0, page=range(6, 10)))
            iterator = pd.merge(iterator, df_page, on='key', how='outer')

            iterator = iterator.query(f' niveau == {level} ').copy()
            iterator['niveau_inf'] = iterator['niveau'] - 1
            dict_prefix = {5: 'format_calculsa', 4: 'format_calculs'}
            iterator['table_input_niveau_inf'] = iterator.apply(
                lambda row: f"{dict_prefix[level]}_N{row.niveau_inf}_{row.period}_page{row.page}", axis=1)
            iterator['table_input'] = iterator.apply(
                lambda row: f"format_calculsa_N{row.niveau}_{row.period}_page{row.page}", axis=1)
            iterator['table_output'] = iterator.apply(
                lambda row: f"format_calculs_N{row.niveau}_{row.period}_page{row.page}", axis=1)

            # prepare parameters to pass to the query based on reference table
            tab_ref = pd.read_sql_query(text(f"select * from tab_ref_n{level} order by tri;"), self.connexion)
            tab_ref['rename_var'] = 'var_' + (tab_ref['tri'] + tab_ref['tri'].max()).astype(str)
            tab_ref['var_partition_by'] = tab_ref.apply(
                lambda row: f" ,MAX(CASE WHEN tri = {row.tri} THEN kpi END) OVER (PARTITION BY N{level}_C_ENTITE, "
                            f" col_name) as {row.rename_var}", axis=1)
            tab_ref['var_transposed'] = ',t.' + tab_ref['rename_var']
            #  with set() we get the random position of the items therefore we'll use OrderedSet from ordered_set lib
            iterator['list_var_partition_by'] = ''.join(OrderedSet(tab_ref['var_partition_by']))
            iterator['list_var_transposed'] = ''.join(OrderedSet(tab_ref['var_transposed']))
            # defining path to query
            calculs_queries_path = os.path.join(self.get_path_parameters()['sql_files'], 'calculs_queries')
            # read query and pass parameters
            iterator['query'] = (iterator
                                 .apply(lambda row: self.sql_operations
                                        .read_query(calculs_queries_path,
                                                    f'formatage_evol_page6to9_level5to4.sql',
                                                    format=row.to_dict()), axis=1)
                                 )
            iterator['query_drop_table_input'] = iterator.apply(
                lambda row: f" DROP TABLE format_calculsa_N{row.niveau}_{row.period}_page{row.page} ;", axis=1)
            # then execute queries
            for query in iterator['query'].values.tolist():
                self.sql_operations.execute_query(query)
            # execute drop tables
            for query in iterator['query_drop_table_input'].values.tolist():
                self.sql_operations.execute_query(query)

    def calculs(self):
        """
        requêtes de calculs des indicateurs par page
        page 2 à 9 pour le bloc commun
        page 10 et plus pour les spécificités qui sont produites à partir du niveau 3
        Dans le bloc commun, les pages 6 à 9 calculent les résultats du niveau courant et
        ceux des entités inférieures associées
        A partir du niveau 3, les calculs seront croisés par la variable mode de contact
        valide
            :return:
            """
        print(f'Execute build_calculs_page2 debut...........')
        self.build_calculs_page2()
        print('Execute build_calculs_page2 fin...........')
        print(f'Execute build_calculs_page3 debut...........')
        self.build_calculs_page3()
        print('Execute build_calculs_page3 fin...........')
        print(f'Execute build_calculs_page4 debut...........')
        self.build_calculs_page4()
        print('Execute build_calculs_page4 fin...........')
        print(f'Execute build_calculs_page5 debut...........')
        self.build_calculs_page5()
        print('Execute build_calculs_page5 fin...........')
        print(f'Execute build_calculs_page6to9 debut...........')
        self.build_calculs_page6to9()
        print('Execute build_calculs_page6to9 fin...........')
        # calculus of KPI and their evolutions
        print('Execute build_format_calculs_page2to5 debut...........')
        self.build_format_calculs_page2to5()
        print('Execute build_format_calculs_page2to5 fin...........')
        print('Execute build_format_calculs_page6to9 debut...........')
        self.build_format_calculs_page6to9()
        self.build_format_calculs_page6to9_level5to4()
        print('Execute build_format_calculs_page6to9 fin...........')

    def build_restitution_threshold(self) -> None:
        # We pass the parameters and execute our queries from a lookup table
        iterator = self.get_parameters_table(level=4)
        iterator['where_mc'] = iterator.apply(
            lambda row: f"date_reponse BETWEEN '{row['debut_mc']}' AND '{row['fin_mc']}' ", axis=1)
        # defining path to query
        restitution_queries_path = os.path.join(self.get_path_parameters()['sql_files'], 'restitution_queries')

        iterator['query'] = iterator.apply(lambda row: self.sql_operations.read_query(restitution_queries_path,
                                                                                      'restitution_threshold.sql',
                                                                                      format=row.to_dict()), axis=1)
        # then execute queries
        for query in iterator['query'].values.tolist():
            self.sql_operations.execute_query(query)

    def build_restitution_level5_page2to5(self) -> None:
        """
        We base our loops on a lookup table

        :return:
        """

        iterator = self.get_parameters_table(level=2)
        cols_to_drop = ['debut_ap', 'debut_mc', 'debut_mp', 'fin_ap', 'fin_mc', 'fin_mp']
        iterator.drop(cols_to_drop, axis=1, inplace=True)
        # Level 5 : max level ! 3 pass through in the query :
        iterator_nsup = iterator.query(" niveau == 5 ").copy()
        df = pd.DataFrame(dict(key=0,
                               tableau=["A", "B", "C"],
                               passage=["02_NC", "03_NINF", "04_NCPP"],
                               ))
        page = pd.DataFrame(dict(key=0, page=range(2, 6)))
        df_iterator_nsup = pd.merge(iterator_nsup, df, on='key', how='outer').merge(page, on='key', how='outer')
        df_iterator_nsup.sort_values(['niveau', 'page', 'tableau', 'passage'], ascending=[False, True, True, True],
                                     inplace=True)

        # Define parameters to pass to the queries of level 5
        df_iterator_nsup['niveau_sup'] = np.where(df_iterator_nsup['niveau'] == 5, '', df_iterator_nsup['niveau'] + 1)
        df_iterator_nsup['niveau_inf'] = df_iterator_nsup['niveau'] - 1
        df_iterator_nsup['distinct'] = np.where(df_iterator_nsup['passage'] == '02_NC', 'DISTINCT', '')

        filtre_nc = df_iterator_nsup['passage'].isin(['02_NC', '04_NCPP'])
        df_iterator_nsup['table_input_level'] = np.where(filtre_nc,
                                                         df_iterator_nsup.apply(
            lambda row: f"format_calculs_n{row.niveau}_{row.period}_page{row.page} as f", axis=1),
                                                         df_iterator_nsup.apply(
            lambda row: f"format_calculs_n{row.niveau_inf}_{row.period}_page{row.page} as f", axis=1))

        condlist = [df_iterator_nsup['passage'] == '02_NC',
                    df_iterator_nsup['passage'] == '03_NINF',
                    df_iterator_nsup['passage'] == '04_NCPP']
        choicelist = [df_iterator_nsup.apply(
            lambda row: f" join tab_ref_n{row.niveau} as t on t.N{row.niveau}_c_entite = f.entite", axis=1),
            df_iterator_nsup.apply(
                lambda row: f" join tab_ref_n{row.niveau} as t on t.N{row.niveau_inf}_c_entite = f.entite", axis=1),
            '']
        df_iterator_nsup['join'] = np.select(condlist, choicelist)

        df_iterator_nsup['where'] = np.where(df_iterator_nsup['passage'] == '04_NCPP',
                                             "where f.periode <> 'MC'",
                                             "where f.periode = 'MC'")
        df_iterator_nsup['entite'] = np.where(df_iterator_nsup['passage'] == '03_NINF',
                                              df_iterator_nsup.apply(lambda row: f" t.N{row.niveau}_c_entite  as entite"
                                                                     , axis=1),
                                              'f.entite')
        choicelist = [df_iterator_nsup.apply(lambda row: f" ,t.N{row.niveau}_lc_entite as short_label_entity", axis=1),
                      df_iterator_nsup.apply(lambda row: f" ,t.N{row.niveau_inf}_lc_entite as short_label_entity",
                                             axis=1),
                      ', NULL as short_label_entity']
        df_iterator_nsup['short_label_entity'] = np.select(condlist, choicelist)

        df_iterator_nsup['var_niveau'] = ',f.niveau'

        condlist_pages = [df_iterator_nsup['page'] == 2,
                          df_iterator_nsup['page'] == 3,
                          df_iterator_nsup['page'] == 4,
                          df_iterator_nsup['page'] == 5]
        choicelist = [" ,f.var_1, f.var_2, f.var_3, f.var_4, f.var_5 ",
                      " ,f.var_1, f.var_2, f.var_3, f.var_4 ",
                      " ,f.var_1, f.var_2, f.var_3, f.var_4, f.var_5, f.var_6 ",
                      " ,f.var_1, f.var_2, f.var_3, f.var_4, f.var_5, f.var_6, f.var_7, f.var_8, f.var_9 "]
        df_iterator_nsup['list_var'] = np.select(condlist_pages, choicelist)

        choicelist = [', 0 as tri',
                      ', t.tri',
                      ",CASE WHEN f.periode = 'MP' THEN 1 ELSE 2 END AS tri"]
        df_iterator_nsup['tri'] = np.select(condlist, choicelist)

        choicelist = [",'boldgrey' as _info_",
                      ',NULL as _info_',
                      ",'bold' as _info_"]
        df_iterator_nsup['_info_'] = np.select(condlist, choicelist)

        choicelist = [', 0 as bloc',
                      ', 1 as bloc',
                      ', 2 as bloc']
        df_iterator_nsup['bloc'] = np.select(condlist, choicelist)

        df_iterator_nsup['tableau_part'] = df_iterator_nsup.apply(lambda row: f", '{row.tableau}' as tableau ", axis=1)

        choicelist = [', NULL as insert_before',
                      ", case when t.tri = 1 then 'linebreak' else NULL end as insert_before",
                      ", case when f.periode = 'MP' then 'linebreak' else NULL end as insert_before "
                      ]
        df_iterator_nsup['insert_before'] = np.select(condlist, choicelist)

        df_iterator_nsup['table_results_level'] = df_iterator_nsup.apply(
            lambda row: f" N{row.niveau}_{row.period}_page{row.page}_tableau_{row.tableau}",
            axis=1)

        query_path = os.path.join(self.get_path_parameters()['sql_files'], 'restitution_queries')
        # loop from level 2 to 5
        # read query and pass parameters
        df_iterator_nsup['query'] = df_iterator_nsup.apply(
            lambda row: self.sql_operations.read_query(query_path, 'restitution_page2to5.sql', format=row.to_dict()),
            axis=1)

        # then execute queries
        for query in df_iterator_nsup['query'].values.tolist():
            self.sql_operations.execute_query(query)

        # reshape dataframe for final query output table
        iterator_for_output = (df_iterator_nsup
                               .sort_values(['table_results_level'])
                               .pivot(index=['niveau', 'period', 'page'],
                                      columns='passage',
                                      values='table_results_level')
                               )
        iterator_for_output.reset_index(inplace=True)
        iterator_for_output['query'] = iterator_for_output.apply(
            lambda row: f"DROP TABLE IF EXISTS N{row.niveau}_{row.period}_page{row.page}; "
                        f" SELECT * INTO N{row.niveau}_{row.period}_page{row.page} FROM {row['02_NC']} "
                        f" UNION ALL SELECT * FROM {row['03_NINF']} UNION ALL SELECT * FROM {row['04_NCPP']} ",
            axis=1)
        for query in iterator_for_output['query'].values.tolist():
            self.sql_operations.execute_query(query)

        # cleaning tables
        iterator_for_output['query_drop_table'] = iterator_for_output.apply(
            lambda row: f" DROP TABLE IF EXISTS {row['02_NC']}, {row['03_NINF']}, {row['04_NCPP']} ;", axis=1)
        # iterator_for_output['query_drop_table_input'] = iterator_for_output.apply(
        #     lambda row: f" DROP TABLE format_calculs_n{row.niveau}_{row.period}_page{row.page} ;", axis=1)

        for query in iterator_for_output['query_drop_table'].values.tolist():
            self.sql_operations.execute_query(query)
        # for query in iterator_for_output['query_drop_table_input'].values.tolist():
        #     self.sql_operations.execute_query(query)

    def build_restitution_levelinf_page2to5(self) -> None:
        """
        We base our loops on a lookup table

        :return:
        """

        iterator = self.get_parameters_table(level=2)
        cols_to_drop = ['debut_ap', 'debut_mc', 'debut_mp', 'fin_ap', 'fin_mc', 'fin_mp']
        iterator.drop(cols_to_drop, axis=1, inplace=True)
        # Level 4 and under : 4 pass through in the query :
        iterator_ninf = iterator.query(" niveau != 5 ").copy()
        df = pd.DataFrame(dict(key=0,
                               tableau=["A", "B", "C", "D"],
                               passage=["01_NSUP", "02_NC", "03_NINF", "04_NCPP"])
                          )
        page = pd.DataFrame(dict(key=0, page=range(2, 6)))
        df_iterator_ninf = pd.merge(iterator_ninf, df, on='key', how='outer').merge(page, on='key', how='outer')
        df_iterator_ninf.sort_values(['niveau', 'page', 'tableau', 'passage'], ascending=[False, True, True, True],
                                     inplace=True)

        # Define parameters to pass to the queries of level 4 and under
        df_iterator_ninf['niveau_sup'] = df_iterator_ninf['niveau'] + 1
        df_iterator_ninf['niveau_inf'] = df_iterator_ninf['niveau'] - 1
        df_iterator_ninf['distinct'] = np.where(df_iterator_ninf['passage'] == '02_NC', 'DISTINCT', '')

        condlist = [df_iterator_ninf['passage'] == '01_NSUP',
                    df_iterator_ninf['passage'] == '02_NC',
                    df_iterator_ninf['passage'] == '03_NINF',
                    df_iterator_ninf['passage'] == '04_NCPP']
        choicelist = [
            df_iterator_ninf.apply(lambda row: f"format_calculs_n{row.niveau_sup}_{row.period}_page{row.page} as f",
                                   axis=1),
            df_iterator_ninf.apply(lambda row: f"format_calculs_n{row.niveau}_{row.period}_page{row.page} as f",
                                   axis=1),
            df_iterator_ninf.apply(lambda row: f"format_calculs_n{row.niveau_inf}_{row.period}_page{row.page} as f",
                                   axis=1),
            df_iterator_ninf.apply(lambda row: f"format_calculs_n{row.niveau}_{row.period}_page{row.page} as f",
                                   axis=1)]
        df_iterator_ninf['table_input_level'] = np.select(condlist, choicelist)

        choicelist = [df_iterator_ninf.apply(
            lambda row: f" join tab_ref_n{row.niveau_sup} as t on t.N{row.niveau_sup}_c_entite = f.entite",
            axis=1),
            df_iterator_ninf.apply(
                lambda row: f" join tab_ref_n{row.niveau} as t on t.N{row.niveau}_c_entite = f.entite and t.tri=1",
                axis=1),
            df_iterator_ninf.apply(
                lambda row: f" join tab_ref_n{row.niveau} as t on t.N{row.niveau_inf}_c_entite = f.entite",
                axis=1),
            '']
        df_iterator_ninf['join'] = np.select(condlist, choicelist)

        df_iterator_ninf['where'] = np.where(df_iterator_ninf['passage'] == '04_NCPP',
                                             "where f.periode <> 'MC'",
                                             "where f.periode = 'MC'")

        df_iterator_ninf['entite'] = np.where(df_iterator_ninf['passage'].isin(['01_NSUP', '03_NINF']),
                                              df_iterator_ninf.apply(lambda row: f" t.N{row.niveau}_c_entite as entite",
                                                                     axis=1), 'f.entite')

        choicelist = [
            df_iterator_ninf.apply(lambda row: f" ,t.N{row.niveau_sup}_lc_entite as short_label_entity", axis=1),
            df_iterator_ninf.apply(lambda row: f" ,t.N{row.niveau}_lc_entite as short_label_entity", axis=1),
            df_iterator_ninf.apply(lambda row: f" ,t.N{row.niveau_inf}_lc_entite as short_label_entity", axis=1),
            ', NULL as short_label_entity']
        df_iterator_ninf['short_label_entity'] = np.select(condlist, choicelist)

        df_iterator_ninf['var_niveau'] = ',f.niveau'

        condlist_var = [df_iterator_ninf['page'] == 2,
                        df_iterator_ninf['page'] == 3,
                        df_iterator_ninf['page'] == 4,
                        df_iterator_ninf['page'] == 5]
        choicelist = [" ,f.var_1, f.var_2, f.var_3, f.var_4, f.var_5 ",
                      " ,f.var_1, f.var_2, f.var_3, f.var_4 ",
                      " ,f.var_1, f.var_2, f.var_3, f.var_4, f.var_5, f.var_6 ",
                      " ,f.var_1, f.var_2, f.var_3, f.var_4, f.var_5, f.var_6, f.var_7, f.var_8, f.var_9 "
                      ]
        df_iterator_ninf['list_var'] = np.select(condlist_var, choicelist)

        choicelist = [', 0 as tri',
                      ', 0 as tri',
                      ', t.tri',
                      ",CASE WHEN f.periode = 'MP' THEN 1 ELSE 2 END AS tri "]
        df_iterator_ninf['tri'] = np.select(condlist, choicelist)

        choicelist = [",'boldgrey' as _info_",
                      ",'bold' as _info_",
                      ',NULL as _info_',
                      ",'bold' as _info_"
                      ]
        df_iterator_ninf['_info_'] = np.select(condlist, choicelist)

        choicelist = [', 0 as bloc',
                      ', 1 as bloc',
                      ', 2 as bloc',
                      ', 3 as bloc']
        df_iterator_ninf['bloc'] = np.select(condlist, choicelist)

        df_iterator_ninf['tableau_part'] = df_iterator_ninf.apply(lambda row: f", '{row.tableau}' as tableau ", axis=1)

        choicelist = [', NULL as insert_before',
                      ", 'linebreak' as insert_before",
                      ", case when t.tri = 1 then 'linebreak' else NULL end as insert_before",
                      ",case when f.periode = 'MP' then 'linebreak' else NULL end as insert_before "
                      ]
        df_iterator_ninf['insert_before'] = np.select(condlist, choicelist)

        df_iterator_ninf['table_results_level'] = df_iterator_ninf.apply(
            lambda row: f" N{row.niveau}_{row.period}_page{row.page}_tableau_{row.tableau}",
            axis=1)

        query_path = os.path.join(self.get_path_parameters()['sql_files'], 'restitution_queries')
        # loop from level 2 to 5
        for p in range(2, 6):
            # read query and pass parameters
            df_iterator_ninf['query'] = df_iterator_ninf.apply(
                lambda row: self.sql_operations.read_query(query_path, 'restitution_page2to5.sql',
                                                           format=row.to_dict()), axis=1)
            # then execute queries
            for query in df_iterator_ninf['query'].values.tolist():
                self.sql_operations.execute_query(query)

        # reshape dataframe for final query output table
        iterator_for_output = (df_iterator_ninf
                               .sort_values(['table_results_level'])
                               .pivot(index=['niveau', 'period', 'page'], columns='passage',
                                      values='table_results_level')
                               )
        iterator_for_output.reset_index(inplace=True)
        iterator_for_output['query'] = iterator_for_output.apply(
            lambda row: f"DROP TABLE IF EXISTS N{row.niveau}_{row.period}_page{row.page}; "
                        f"SELECT * INTO N{row.niveau}_{row.period}_page{row.page} FROM {row['01_NSUP']} "
                        f" UNION ALL SELECT * FROM {row['02_NC']} UNION ALL SELECT * FROM {row['03_NINF']} "
                        f" UNION ALL SELECT * FROM {row['04_NCPP']} ", axis=1)
        # then execute queries
        for query in iterator_for_output['query'].values.tolist():
            self.sql_operations.execute_query(query)

        # cleaning tables
        iterator_for_output['query_drop_table'] = iterator_for_output.apply(
            lambda
                row: f" DROP TABLE IF EXISTS {row['01_NSUP']} , {row['02_NC']}, {row['03_NINF']}, {row['04_NCPP']} ;",
            axis=1)
        for query in iterator_for_output['query_drop_table'].values.tolist():
            self.sql_operations.execute_query(query)

        iterator_for_output['query_drop_table_input'] = iterator_for_output.apply(
            lambda row: f" DROP TABLE format_calculs_n{row.niveau}_{row.period}_page{row.page} ;", axis=1)
        for query in iterator_for_output['query_drop_table_input'].values.tolist():
            self.sql_operations.execute_query(query)

    def build_integration_reference_llosa_page6to9(self) -> None:
        pass

    def restitution(self):
        """
        Requêtes de mise en forme des résultats par page
        On restitue les indicateurs par niveau pour toutes les entités
        ayant un seuil supérieur à 80 répondants pour toutes les pages 2 à 9.
        Le niveau 5 restitue les résultats du niveau 5 et du niveau 4
        Les niveaux inférieurs rappelleront les résultats du niveau supérieur
        auquel ils sont rattachés ainsi que les résultats des niveaux inférieurs
        qui leurs sont rattachés
        Restitution au format excel ou csv pour des reportings sous power bi.

            :return:
        """
        # print(f'Execute build_restitution_threshold debut...........')
        # self.build_restitution_threshold()
        # print('Execute build_restitution_threshold fin...........')
        print(f'Execute build_restitution_level5_page2to5 debut {datetime.datetime.now()}...........')
        self.build_restitution_level5_page2to5()
        print(f'Execute build_restitution_level5_page2to5 fin {datetime.datetime.now()}...........')
        print(f'Execute build_restitution_levelinf_page2to5 debut {datetime.datetime.now()}...........')
        self.build_restitution_levelinf_page2to5()
        print(f'Execute build_restitution_levelinf_page2to5 fin {datetime.datetime.now()}...........')

        # print(f'Execute build_integration_reference_llosa_page6to9 debut {datetime.datetime.now()}...........')
        # self.build_integration_reference_llosa_page6to9()
        # print(f'Execute Execute build_integration_reference_llosa_page6to9 fin {datetime.datetime.now()}...........')


if __name__ == '__main__':
    barometre = Barometre(type_rapport='global', periode='201405')
    barometre.run()
