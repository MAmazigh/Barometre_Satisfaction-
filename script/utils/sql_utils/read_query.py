# -*- coding: utf-8 -*-
"""
 Fonction read_query: lecture d'une requête SQL
"""


def read_query(path: str, key: str, format: dict = None) -> str:
    """

    :param path: path vers le fichier json des requêtes sql à lire ou exécuter
    :param key: nom de la requête à lire ou exécuter
    :param format: dictionnaire de paramètres à passer à la requête à lire ou exécuter
    """

    if format is None:
        format = dict()

    try:
        with open(path, key) as file:
            query = file.read()
        file.close()
        return ' '.join(query.replace('\n', ' ').split()).format(**format)
    except Exception:
        # logger.exception(e)
        print('Erreur lors de la lecture de la requête')