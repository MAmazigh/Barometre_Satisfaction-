DROP TABLE IF EXISTS {table};
CREATE TABLE IF NOT EXISTS {table}
({schema_structure_from_json})
TABLESPACE pg_default;
COPY {table}({columns_from_json})
FROM '{path_to_structure_csv}'
DELIMITER {delimiter}
CSV HEADER;
ALTER TABLE IF EXISTS {table}
    OWNER to postgres;
	