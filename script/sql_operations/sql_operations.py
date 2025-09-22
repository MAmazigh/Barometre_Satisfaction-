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
        super().__init__()  # initialise SqlConnection sans stocker de connexion persistante

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
        conn = self.get_connexion()  # nouvelle connexion à chaque appel
        transaction = conn.begin()
        try:
            conn.execute(query)
            transaction.commit()
        except Exception as e:
            transaction.rollback()
            print(f" Erreur lors de l'exécution de la requête: {query}")
            print(e)
        finally:
            conn.close()

    # def execute_queries(self, queries: List[str]):
    #     for query in queries:
    #         self.execute_query(text(query))
    def execute_queries(self, queries: List[str]):
        conn = self.get_connexion()
        transaction = conn.begin()
        try:
            for query in queries:
                conn.execute(text(query))
            transaction.commit()
        except Exception as e:
            transaction.rollback()
            print("Erreur lors de l'exécution des requêtes multiples :")
            print(e)
        finally:
            conn.close()

    @staticmethod
    def read_query(path_to_queries_folder: str, query_name: str, format: dict = None) -> str:
        """

        :param path_to_queries_folder: path vers le folder des requêtes sql à lire ou exécuter
        :param query_name: nom de la requête à lire ou exécuter avec son extension .sql
        :param format: dictionnaire de paramètres à passer à la requête à lire ou exécuter

        Nettoie la requête SQL en supprimant les retours à la ligne et les espaces superflus
        Cela permet d’avoir une requête sur une seule ligne pour éviter certains bugs lors de l’exécution
        format(**format) pour injecter dynamiquement les paramètres dans la requête SQL
        Avec with open on ferme automatiquement le fichier, plus besoin de file.close()
        Gère les exceptions à la lecture du fichier et affiche un message d’erreur personnalisé
        avec le nom de la requête
        """

        if format is None:
            format = dict()
        try:
            with open(os.path.join(path_to_queries_folder, query_name)) as file:
                query = file.read()
            return ' '.join(query.replace('\n', ' ').split()).format(**format)
        except Exception as e:
            print(e)
            print(f'Erreur lors de la lecture de la requête: {query_name}')
            return ""

    @staticmethod
    def read_queries(path_to_json: str, key_query_name: str, format: dict = None) -> List[str]:
        """

        :param path_to_json: path vers le fichier json des requêtes sql à lire ou exécuter
        :param key_query_name: nom de la requête à lire ou exécuter
        :param format: dictionnaire de paramètres à passer à la requête à lire ou exécuter

        But : Lire plusieurs requêtes SQL stockées dans un fichier
        Séparation : Les requêtes sont séparées par deux sauts de ligne (\n\n).
        Nettoyage : Chaque requête est mise sur une seule ligne (' '.join(query.replace('\n', ' ').split())).
        Formatage : Chaque requête est formatée avec le dictionnaire format (remplacement des variables).
        Retour : Une liste de requêtes SQL prêtes à être exécutées.
        """

        if format is None:
            format = dict()
        try:
            with open(path_to_json, key_query_name) as file:
                # On séparera les requêtes par deux sauts de ligne : \n\n
                list_query = [' '.join(query.replace('\n', ' ').split()) for query in file.read().split("\n\n")]
            return [query.format(**format) for query in list_query]
        except Exception as e:
            print(f'Erreur lors de la lecture de la requête: {key_query_name}')
            print(e)
            return []

    @staticmethod
    def read_query_blocks(path_to_queries_folder: str, query_name: str, format: dict = None) -> List[str]:
        """
        Lit un fichier SQL et retourne une liste de blocs de requêtes séparés par deux sauts de ligne.
        Chaque bloc est nettoyé (retours à la ligne supprimés, espaces superflus retirés) et formaté avec le dictionnaire fourni.
        :param path_to_queries_folder: chemin vers le dossier contenant les fichiers SQL
        :param query_name: nom du fichier SQL à lire
        :param format: dictionnaire de paramètres à injecter dans les requêtes
        :return: liste de chaînes, chaque chaîne étant une requête SQL prête à être exécutée
        """
        if format is None:
            format = dict()
        try:
            with open(os.path.join(path_to_queries_folder, query_name), encoding="utf-8") as file:
                raw = file.read()
            # Séparation par double saut de ligne
            blocks = raw.split("\n\n")
            # Nettoyage et formatage de chaque bloc
            return [
                ' '.join(block.replace('\n', ' ').split()).format(**format)
                for block in blocks if block.strip()
            ]

            # return [block.strip().format(**format) for block in raw.split("\n\n") if block.strip()]


        except Exception as e:
            print(f'Erreur lors de la lecture des blocs SQL : {query_name}')
            print(e)
            return []

# if __name__ == '__main__':
#
#     sql_operation = SqlOperations()
