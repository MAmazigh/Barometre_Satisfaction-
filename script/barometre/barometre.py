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
from sql_utils import enrich_iterator_with_sql_fragments_for_extraction, process_extraction_page_queries
from sql_utils import enrich_iterator_with_sql_fragments_for_calculs
from sqlalchemy import text
from ordered_set import OrderedSet
from concurrent.futures import ThreadPoolExecutor
from itertools import chain


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

        print(f'Execute pre_production debut {datetime.datetime.now()}...........')
        self.pre_production()
        print(f'Execute pre_production fin {datetime.datetime.now()}...........')
        print(f'Execute extraction debut {datetime.datetime.now()}...........')
        self.extraction()
        print(f'Execute extraction fin {datetime.datetime.now()}...........')
        print(f'Execute calculs debut {datetime.datetime.now()}...........')
        self.calculs()
        print(f'Execute calculs fin {datetime.datetime.now()}...........')
        # Mise en forme et sortie sous excel-pdf-html ou à plat pour power bi ?
        # print(f'Execute restitution debut {datetime.datetime.now()}...........')
        # self.restitution()
        # print(f'Execute restitution fin {datetime.datetime.now()}...........')

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

        queries = self.sql_operations.read_query_blocks(insert_queries_path, "insert_structure.sql",
                                                        format=dict(table='structure',
                                                                    schema_structure_from_json=schema,
                                                                    columns_from_json=columns,
                                                                    path_to_structure_csv=path_to_structure_csv,
                                                                    delimiter="';'"))
        self.sql_operations.execute_queries(queries)

    def table_reference(self) -> None:
        """
        execute la requete table_reference.sql pour tous les niveaux à produire
        """
        iterator = self.get_parameters_table()
        iterator['niveau_inf'] = iterator['niveau'] - 1
        # defining path to query
        query_path = os.path.join(self.get_path_parameters()['sql_files'], 'insertion_tables_queries')

        # read query and pass parameters
        for _, row in iterator.iterrows():
            format_dict = row.to_dict()
            queries = self.sql_operations.read_query_blocks(query_path, 'table_reference.sql', format=format_dict)
            self.sql_operations.execute_queries(queries)

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
        """
            Méthode principale pour exécuter les extractions PostGreSQL des pages 2 à 9.

            Args:
                get_parameters_table: Fonction pour récupérer le DataFrame de paramétrage.
                get_path_parameters: Fonction pour récupérer les chemins de fichiers.
                sql_operations: Instance de SQLOperations contenant les méthodes read_query et execute_query.
            """
        iterator = self.get_parameters_table(level=1)
        iterator = enrich_iterator_with_sql_fragments_for_extraction(iterator)

        extraction_queries_path = os.path.join(self.get_path_parameters()['sql_files'], 'extraction_queries')

        for p in range(2, 10):
            process_extraction_page_queries(iterator, p, extraction_queries_path, self.sql_operations)

    def extraction(self):
        """
        requêtes d'extraction des data par page
        page 2 à 9 pour le bloc commun
        page 10 et plus pour les spécificités qui sont produites à partir du niveau 3
        A partir du niveau 3, on aura deux extractions pour la justesse des calculs par mode de contact valide
        :return:
        """

        self.build_extraction_page2to9()

    def build_calculs_page2to5(self) -> None:
        iterator = self.get_parameters_table()

        # dictionary to map each page to its partition variable
        partition_vars = {
            2: 'Q1',
            3: 'satisfaction_contact',
            4: 'prestation_impact',
            5: 'mode_contact_valide'
        }
        # loop on the pages and mapping
        for page, partition_var in partition_vars.items():
            # Conditional calculs
            mask = ~iterator.period.isin(['Y', 'P'])

            iterator.loc[mask, 'calcul_freq_mp'] = iterator[mask].apply(
                lambda
                    row: f",SUM(CASE WHEN indic= 'MP' THEN freq END) OVER (PARTITION BY N{row['niveau']}_C_ENTITE"
                         f", {partition_var} ORDER BY N{row['niveau']}_C_ENTITE, {partition_var}) AS freq_MP ",
                axis=1)

            iterator.loc[mask, 'calcul_indicateur_mp'] = iterator[mask].apply(
                lambda
                    row: f",SUM(CASE WHEN indic= 'MP' THEN indicateur END) OVER (PARTITION BY N{row['niveau']}_C_ENTITE"
                         f", {partition_var} ORDER BY N{row['niveau']}_C_ENTITE, {partition_var}) AS indicateur_MP ",
                axis=1)

            iterator.loc[mask, 'freq_mp'] = ', d.freq_MP'
            iterator.loc[mask, 'indicateur_mp'] = ', d.indicateur_MP'

            # Read and execute query
            calculs_queries_path = os.path.join(self.get_path_parameters()['sql_files'], 'calculs_queries')
            iterator['query'] = iterator.apply(lambda row: self.sql_operations.read_query_blocks(calculs_queries_path,
                                                                                          f'calculs_evol_page{page}.sql',
                                                                                          format=row.to_dict()), axis=1)

            for query in iterator['query'].values.tolist():
                self.sql_operations.execute_queries(query)

    def prepare_iterator(self, df: pd.DataFrame, page: int, suffixe: str, mcv: str) -> pd.DataFrame:
        df['suffixe'] = suffixe
        df['mcv'] = mcv
        df['table_input'] = df.apply(
            lambda row: f"extraction_N{row.niveau}_{row.period}_page{page}" + (f"_{suffixe}" if suffixe else ""),
            axis=1)

        mask = ~df.period.isin(['Y', 'P'])
        partition_base = f"N{{niveau}}_C_ENTITE{mcv}, col_name"

        df.loc[mask, 'calcul_freq_mp'] = df.loc[mask].apply(
            lambda
                row: f",SUM(CASE WHEN indic= 'MP' THEN freq END) OVER (PARTITION BY {partition_base.format(niveau=row['niveau'])} ORDER BY {partition_base.format(niveau=row['niveau'])}) AS freq_MP",
            axis=1
        )
        df.loc[mask, 'calcul_indicateur_mp'] = df.loc[mask].apply(
            lambda
                row: f",SUM(CASE WHEN indic= 'MP' THEN indicateur END) OVER (PARTITION BY {partition_base.format(niveau=row['niveau'])} ORDER BY {partition_base.format(niveau=row['niveau'])}) AS indicateur_MP",
            axis=1
        )
        df['freq_mp'] = ''
        df['indicateur_mp'] = ''
        df.loc[mask, 'freq_mp'] = ', d.freq_MP'
        df.loc[mask, 'indicateur_mp'] = ', d.indicateur_MP'

        return df

    def _generate_queries(self, df: pd.DataFrame, page: int) -> pd.Series:
        query_path = os.path.join(self.get_path_parameters()['sql_files'], 'calculs_queries')
        return df.apply(lambda row: self.sql_operations.read_query_blocks(query_path,
                                                                   f'calculs_evol_page{page}.sql',
                                                                   format=row.to_dict()), axis=1)

    def _build_and_execute_calculs(self, page: int, niveau: int, suffixe: str, mcv: str) -> None:
        df = self.get_parameters_table(level=niveau).copy()
        if niveau == 3:
            df = df.query('niveau == 3')

        df = self.prepare_iterator(df, page, suffixe, mcv)
        df['query'] = self._generate_queries(df, page)

        for query in df['query']:
            self.sql_operations.execute_queries(query)

    def build_calculs_page6to9(self) -> None:
        with ThreadPoolExecutor(max_workers=4) as executor:
            # for page in range(6, 10):
            for page in range(6, 8):
                print(f'Execute _build_and_execute_calculs page{page} ...........')
                executor.submit(self._build_and_execute_calculs, page, 3, 'mcv', ', mode_contact_valide')
                executor.submit(self._build_and_execute_calculs, page, 2, '', '')

    def build_format_calculs_page2to5(self) -> None:
        """
        Exécute les requêtes de formatage des calculs pour les pages 2 à 5 en mode multithreadé.
        Utilise un DataFrame comme table de paramétrage enrichie dynamiquement.
        """
        df = self.get_parameters_table(level=1)
        iterator = enrich_iterator_with_sql_fragments_for_calculs(df, self.threshold_NI_NS)
        calculs_queries_path = os.path.join(self.get_path_parameters()['sql_files'], 'calculs_queries')

        for p in range(2, 6):
            all_queries = []

            # 1. Génération des requêtes de formatage (par ligne)
            for _, row in iterator.iterrows():
                format_dict = row.to_dict()
                queries = self.sql_operations.read_query_blocks(
                    calculs_queries_path, f'formatage_evol_page{p}.sql', format=format_dict
                )
                # On ajoute toutes les requêtes (liste) à la liste globale
                if isinstance(queries, list):
                    all_queries.extend(queries)
                else:
                    all_queries.append(queries)

            # 2. Construction des noms de tables intermédiaires
            iterator_for_output = iterator[['niveau', 'period', 'indic']].copy()
            iterator_for_output['output'] = iterator_for_output.apply(
                lambda r: f"format_N{r.niveau}_{r.period}_{r.indic}_page{p}", axis=1
            )
            pivot = iterator_for_output.pivot(index=['niveau', 'period'], columns='indic',
                                              values='output').reset_index()

            # 3. Génération des requêtes de fusion et suppression
            pivot['query_union'] = pivot.apply(
                lambda r: f"DROP TABLE IF EXISTS format_calculs_N{r.niveau}_{r.period}_page{p}; "
                          f"SELECT * INTO format_calculs_N{r.niveau}_{r.period}_page{p} FROM {r.MC} "
                          f"UNION ALL SELECT * FROM {r.MP} UNION ALL SELECT * FROM {r.AP};", axis=1
            )
            pivot['query_drop_temp'] = pivot.apply(
                lambda r: f"DROP TABLE IF EXISTS {r.MC}, {r.MP}, {r.AP};", axis=1
            )
            pivot['query_drop_input'] = pivot.apply(
                lambda r: f"DROP TABLE IF EXISTS calculs_N{r.niveau}_{r.period}_page{p};", axis=1
            )

            # 4. Ajout des requêtes finales à la liste globale
            for col in ['query_union', 'query_drop_temp', 'query_drop_input']:
                all_queries.extend(pivot[col].tolist())

            # 5. Exécution multithreadée de toutes les requêtes de la page
            self.sql_operations.execute_queries(all_queries)

    # def build_format_calculs_page6to9(self) -> None:
    #     # We base our loops on a lookup table
    #     for p in range(6, 10):
    #         print(f'Execute build_format_calculs_page{p} debut...........')
    #         iterator = self.get_parameters_table(level=2)
    #         iterator['table_input'] = iterator.apply(lambda row: f"calculsa_N{row.niveau}_{row.period}_page{p}", axis=1)
    #         iterator['table_output'] = iterator.apply(lambda row: f"format_calculsa_N{row.niveau}_{row.period}_page{p}",
    #                                                   axis=1)
    #         list_item_pct_p8 = "'item_03700', 'item_03910', 'item_03930', 'item_03940', 'item_03950', 'item_03960', " \
    #                            "'item_03970' "
    #         list_item_pct_p6 = " 'item_01300', 'item_01400' "
    #         iterator['list_item_pct'] = np.where(p == 8, list_item_pct_p8, list_item_pct_p6)
    #         iterator['threshold_NI_NS'] = self.threshold_NI_NS
    #         # defining path to query
    #         calculs_queries_path = os.path.join(self.get_path_parameters()['sql_files'], 'calculs_queries')
    #
    #         # read query and pass parameters
    #         iterator['query'] = iterator.apply(lambda row: self.sql_operations
    #                                            .read_query_blocks(calculs_queries_path, f'formatage_evol_page6to9.sql',
    #                                                        format=row.to_dict()), axis=1)
    #         # then execute queries
    #         for query in iterator['query'].values.tolist():
    #             self.sql_operations.execute_queries(query)
    #
    #         # only level 3 products mcv fields
    #         iterator = iterator.query(' niveau == 3 ').copy()
    #         iterator['mcv'] = ', mode_contact_valide'
    #         iterator['table_input_mcv'] = iterator.apply(lambda row: f"calculsa_N{row.niveau}_{row.period}_page{p}mcv",
    #                                                      axis=1)
    #         iterator['table_output_mcv'] = iterator.apply(
    #             lambda row: f"format_calculsa_N{row.niveau}_{row.period}_page{p}mcv",
    #             axis=1)
    #         iterator['final_table'] = iterator.apply(
    #             lambda row: f"format_calculs_N{row.niveau}_{row.period}_page{p}",
    #             axis=1)
    #         # read query and pass parameters
    #         iterator['query'] = iterator.apply(lambda row: self.sql_operations
    #                                            .read_query_blocks(calculs_queries_path, f'formatage_evol_page6to9mcv.sql',
    #                                                        format=row.to_dict()), axis=1)
    #         # then execute queries
    #         for query in iterator['query'].values.tolist():
    #             self.sql_operations.execute_queries(query)

    def build_format_calculs_page6to9(self) -> None:
        list_item_pct = {
            8: "'item_03700', 'item_03910', 'item_03930', 'item_03940', 'item_03950', 'item_03960', 'item_03970'",
            6: "'item_01300', 'item_01400'",
            7: "'item_01300', 'item_01400'",
            9: "'item_01300', 'item_01400'"
        }
        calculs_queries_path = os.path.join(self.get_path_parameters()['sql_files'], 'calculs_queries')

        for p in range(6, 10):
            print(f'Execute build_format_calculs_page{p} debut...........')

            iterator = self.get_parameters_table(level=2)
            iterator = iterator.assign(
                table_input="calculsa_N" + iterator['niveau'].astype(str) + "_" + iterator['period'] + f"_page{p}",
                table_output="format_calculsa_N" + iterator['niveau'].astype(str) + "_" + iterator[
                    'period'] + f"_page{p}",
                list_item_pct=list_item_pct.get(p, "'item_01300', 'item_01400'"),
                threshold_NI_NS=self.threshold_NI_NS
            )

            # Génération des requêtes SQL
            iterator['query'] = iterator.apply(
                lambda row: self.sql_operations.read_query_blocks(
                    calculs_queries_path, 'formatage_evol_page6to9.sql', format=row.to_dict()
                ), axis=1
            )
            # Exécution multithreadée de toutes les requêtes d'un coup
            self.sql_operations.execute_queries(iterator['query'].tolist())

            # Niveau 3 uniquement
            iterator3 = iterator[iterator['niveau'] == 3].copy()
            iterator3 = iterator3.assign(
                mcv=', mode_contact_valide',
                table_input_mcv="calculsa_N" + iterator3['niveau'].astype(str) + "_" + iterator3['period'] +
                                f"_page{p}mcv",
                table_output_mcv="format_calculsa_N" + iterator3['niveau'].astype(str) + "_" + iterator3['period'] +
                                 f"_page{p}mcv",
                final_table="format_calculs_N" + iterator3['niveau'].astype(str) + "_" + iterator3['period'] +
                            f"_page{p}")

            iterator3['query'] = iterator3.apply(lambda row: self.sql_operations.read_query_blocks(
                calculs_queries_path, 'formatage_evol_page6to9mcv.sql', format=row.to_dict()), axis=1)
            self.sql_operations.execute_queries(iterator3['query'].tolist())

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
        print(f'Execute build_calculs_page2to5 debut...........')
        self.build_calculs_page2to5()
        print('Execute build_calculs_page2to5 fin...........')
        print(f'Execute build_calculs_page6to9 debut...........')
        self.build_calculs_page6to9()
        print('Execute build_calculs_page6to9 fin...........')
        # calculus of KPI and their evolutions
        print('Execute build_format_calculs_page2to5 debut...........')
        self.build_format_calculs_page2to5()
        print('Execute build_format_calculs_page2to5 fin...........')
        # print('Execute build_format_calculs_page6to9 debut...........')
        self.build_format_calculs_page6to9()
        # self.build_format_calculs_page6to9_level5to4()
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
                                                             lambda
                                                                 row: f"format_calculs_n{row.niveau}_{row.period}_page{row.page} as f",
                                                             axis=1),
                                                         df_iterator_nsup.apply(
                                                             lambda
                                                                 row: f"format_calculs_n{row.niveau_inf}_{row.period}_page{row.page} as f",
                                                             axis=1))

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
