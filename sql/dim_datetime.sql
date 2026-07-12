CREATE TABLE IF NOT EXISTS star_schema.dim_datetime (
    datetime_id BIGSERIAL PRIMARY KEY,
    full_timestamp TIMESTAMP NOT NULL UNIQUE,
    date DATE,
    year INT,
    quarter INT,
    month INT,
    week INT,
    day INT,
    day_of_week INT,
    hour INT,
    minute INT
);