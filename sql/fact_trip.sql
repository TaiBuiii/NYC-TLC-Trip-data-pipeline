CREATE TABLE IF NOT EXISTS star_schema.fact_trip(
    trip_id BIGSERIAL PRIMARY KEY,
    pickup_datetime_id BIGINT  REFERENCES star_schema.dim_datetime(datetime_id) NOT NULL,
    dropoff_datetime_id  BIGINT  REFERENCES star_schema.dim_datetime(datetime_id) NOT NULL,
    pickup_location_id INT REFERENCES star_schema.dim_location(location_id) NOT NULL,
    dropoff_location_id INT REFERENCES star_schema.dim_location(location_id) NOT NULL,
    payment_id INT REFERENCES star_schema.dim_payment(payment_id) NOT NULL,
    trip_distance DOUBLE PRECISION,
    passenger_count INT,
    fare_amount DOUBLE PRECISION,
    tip_amount DOUBLE PRECISION DEFAULT 0,
    extra DOUBLE PRECISION DEFAULT 0,
    total_amount DOUBLE PRECISION,
    trip_duration DOUBLE PRECISION,
    average_speed DOUBLE PRECISION
);