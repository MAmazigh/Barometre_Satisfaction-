# -*- coding: utf-8 -*-
"""
Classe de connection à une base de donnée PostGreSQL
Elle utilise les librairies:
 dotenv pour récupérer les paramètres de connection
 sqlalchemy pour réaliser la connection
"""

import os
import sqlalchemy as db
# import sys to get more detailed Python exception info
import sys
from dotenv import load_dotenv
# import the error handling libraries for psycopg2
from psycopg2 import OperationalError, errorcodes, errors


class SqlConnection:
    #  variable de classe : pas besoin de self car définie avant toute méthode.

    def __init__(self) -> None:
        """
        self: variable qui fait référence à UNE instance de la classe
        Elle permet d'avoir accès aux attributs et aux méthodes de la classe
        load_dotenv est une librairie importée pour gérer les variables d'environnement
        dans un fichier .env situé à la racine du projet. On y a accès via os.getenv()
        """
        load_dotenv()
        self.engine = self.get_configuration()
        self.connexion = self.get_connexion()
        # path où l'on va importer/exporter les fichiers csv
        self.path_to_database = os.getenv('path_to_db')

    @staticmethod
    def get_configuration() -> db.engine.base.Engine:
        """
        on a rendu la méthode statique: on n'a plus besoin du self en ref à l'instance
        passage des éléments de configuration de la connexion à la Db
        :return: on définit le dialecte sql (postgresql) et le driver utilisé psycopg2
        """
        try:
            username = os.getenv('user_name')
            password = os.getenv('password')
            hostname = os.getenv('hostname')
            port = os.getenv('port')
            database_name = os.getenv('db_name')
            return db.create_engine(f'postgresql+psycopg2://{username}:{password}@{hostname}:{port}/{database_name}')
        except:
            print("problème au niveau configuration")

    # Define a method that handles and parses psycopg2 exceptions
    def get_psycopg2_exception(err):
        # get details about the exception
        err_type, err_obj, traceback = sys.exc_info()
        # get the line number when exception occured
        line_n = traceback.tb_lineno
        # print the connect() error
        print("\npsycopg2 ERROR:", err, "on line number:", line_n)
        print("psycopg2 traceback:", traceback, "-- type:", err_type)
        # psycopg2 extensions.Diagnostics object attribute
        print("\nextensions.Diagnostics:", err.diag)
        # print the pgcode and pgerror exceptions
        print("pgerror:", err.pgerror)
        print("pgcode:", err.pgcode, "\n")

    def get_connexion(self):
        """
        création de la connexion après la configuration
        :return:
        """
        try:
            return self.engine.connect()
        # except:
        #     print("impossible de se connecter à la base")
        except OperationalError as err:
            # passing exception to function
            self.get_psycopg2_exception(err)

    def transaction(self):
        """  Les transactions permettent l'exécution d'un ensemble de requêtes et
        de s'assurer qu'elles sont soit commitées (commit) soit annulées (rollback) """
        try:
            return self.connexion.begin()
        except Exception as e:
            print("erreur de transaction des requêtes")

    def commit(self, transaction):
        try:
            transaction.commit()
        except:
            print("erreur de commit des requêtes")

    def get_deconnexion(self):
        self.connexion.close()

# if __name__ == '__main__':

# from dotenv import load_dotenv
#
# load_dotenv()
#
# print(os.getenv('db_name'))

# sql_connect = SqlConnection()
#
# query = """
# SELECT *
#   FROM public.satisfaction
#   LIMIT 10
# """
#
# import pandas as pd
#
# print(pd.read_sql_query(query, sql_connect.connexion))
