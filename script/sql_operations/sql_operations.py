# -*- coding: utf-8 -*-
"""
Classe de gestion de différentes opérations de SQL
Elle hérite de la classe SqlConnection qui fait appel à SQLAlchemy notamment

"""

import os
from sql_connection import SqlConnection
from typing import List
from sqlalchemy import text


class SqlOperations(SqlConnection):

    def __init__(self) -> None:
        """

        :rtype: object
        """
        super().__init__()

    def execute_query(self, query: str):
        """
        Pour exécuter une requête, on lance une transaction de manière explicite
        puis on exécute la requête
        enfin on commit la transaction pour s'assurer que les modif effectuées soient visibles
        pour toutes les autres connexions

        Remarque: on fait appel à la methode text() de sqlalchemy de manière à empêcher python d'interpréter
        des caractères spéciaux pour éviter des exceptions.
        Ex: self.connexion.execute(select * from table where column LIKE 'test%') --> TypeError: dict is not a sequence

        :type query: object
        """
        try:
            transaction = self.transaction()
            self.connexion.execute(text(query))
            self.commit(transaction)
        except Exception as e:
            print(f" Erreur lors de l'exécution de la requête: {query}")
            print(e)

    def execute_queries(self, queries: List[str]):
        for query in queries:
            self.execute_query(text(query))

    @staticmethod
    def read_query(path_to_queries_folder: str, query_name: str, format: dict = None) -> str:
        """

        :param path_to_queries_folder: path vers le folder des requêtes sql à lire ou exécuter
        :param query_name: nom de la requête à lire ou exécuter avec son extension .sql
        :param format: dictionnaire de paramètres à passer à la requête à lire ou exécuter
        """

        if format is None:
            format = dict()
        try:
            with open(os.path.join(path_to_queries_folder, query_name)) as file:
                query = file.read()
            file.close()
            return ' '.join(query.replace('\n', ' ').split()).format(**format)
        except Exception as e:
            print(e)
            print(f'Erreur lors de la lecture de la requête: {query_name}')

    @staticmethod
    def read_queries(path_to_json: str, key_query_name: str, format: dict = None) -> List[str]:
        """

        :param path_to_json: path vers le fichier json des requêtes sql à lire ou exécuter
        :param key_query_name: nom de la requête à lire ou exécuter
        :param format: dictionnaire de paramètres à passer à la requête à lire ou exécuter
        """

        if format is None:
            format = dict()
        try:
            with open(path_to_json, key_query_name) as file:
                # On séparera les requêtes par deux sauts de ligne : \n\n
                list_query = [' '.join(query.replace('\n', ' ').split()) for query in file.read().split("\n\n")]
            file.close()
            return [query.format(**format) for query in list_query]
        except Exception as e:
            print(f'Erreur lors de la lecture de la requête: {key_query_name}')
            print(e)

# if __name__ == '__main__':
#
#     sql_operation = SqlOperations()
