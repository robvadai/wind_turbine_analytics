INSERT INTO
    "connection" ("conn_id", "conn_type", "description", "host", "schema", "login", "password", "port", "is_encrypted", "is_extra_encrypted", "extra")
VALUES
    ('spark_local', 'spark', NULL, 'local[*]', NULL, NULL, NULL, NULL, NULL, NULL, NULL),
    ('postgres_local', 'postgres', NULL, 'postgres', 'wind_turbine_analytics', 'wind_turbine_analytics', 'wind_turbine_analytics', '5432', NULL, NULL, NULL);