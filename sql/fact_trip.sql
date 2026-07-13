CREATE TABLE IF NOT EXISTS star_schema.fact_trip(
    trip_id BIGSERIAL PRIMARY KEY,
    pickup_location_id INT REFERENCES star_schema.dim_location(location_id) NOT NULL,
    dropoff_location_id INT REFERENCES star_schema.dim_location(location_id) NOT NULL,
    payment_id INT REFERENCES star_schema.dim_payment(payment_id) NOT NULL,
    pickup_datetime TIMESTAMP NOT NULL,
    dropoff_datetime  TIMESTAMP NOT NULL,
    trip_distance DOUBLE PRECISION,
    passenger_count INT,
    fare_amount DOUBLE PRECISION,
    tip_amount DOUBLE PRECISION DEFAULT 0,
    extra DOUBLE PRECISION DEFAULT 0,
    total_amount DOUBLE PRECISION,
    trip_duration DOUBLE PRECISION,
    average_speed DOUBLE PRECISION,
    UNIQUE (pickup_datetime, dropoff_datetime, pickup_location_id, dropoff_location_id, payment_id, trip_distance, passenger_count, fare_amount, tip_amount, extra, total_amount, trip_duration, average_speed)
);