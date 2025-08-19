# -*- coding: utf-8 -*-
"""
 Fonction get_path: path vers le fichier recherché
"""

import os
from pathlib import Path
from typing import Union


def get_path(current_path: Path, fic_name: str = None, json_schema: str = None) -> Union[str, bytes]:
    """
    # recherche recursive du fichier json dans les sous dossiers du current_path

    :param json_schema: nom du dossier contenant les fichiers json
    :param current_path: chemin du dossier de script
    :param fic_name: nom de dossier ou de fichier à rechercher
    :return path_to_file: chemin du dossier ou du fichier json
    """

    if json_schema is None:
        json_schema_name = fic_name
    else:
        json_schema_name = json_schema + '/' + fic_name

    to_file = [p for p in current_path.rglob(json_schema_name)][0]
    path_to_file = os.path.join(to_file)

    return path_to_file
