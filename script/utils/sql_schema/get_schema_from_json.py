from load_json import load_json


def get_schema_from_json(path_to_json) -> tuple:
    df_json = load_json(path_to_json)
    transpose_df_json = df_json.T.reset_index()
    schema = [tuple(a) for a in transpose_df_json.to_numpy()]
    columns = list(df_json.columns)

    return schema, columns
