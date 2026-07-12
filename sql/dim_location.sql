CREATE TABLE IF NOT EXISTS star_schema.dim_location(
    location_id INT PRIMARY KEY,
    borough VARCHAR(100),
    zone VARCHAR(100),
    service_zone VARCHAR(100)
);