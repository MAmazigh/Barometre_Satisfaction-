import pandas as pd


def load_json(path: str) -> pd.DataFrame:
    """Chargement d'un fichier JSON

    :param path: chemin d'un fichier JSON
    :return: fichier JSON sous forme de dataframe
    """
    try:
        return pd.read_json(path)
    except:
        print("erreur au niveau de l'importation du fichier json")
        