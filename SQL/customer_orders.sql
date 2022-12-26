CREATE TABLE enpal.customer_orders(
    customer_id integer PRIMARY KEY,
    city     TEXT,
    country  TEXT,
    country_code     TEXT,
    state    TEXT,
    latitude     DECIMAL(20,5),
    longitude    DECIMAL(20,5),
    start_date DATE,
    number_of_panels NUMERIC(20),
    sysdatecreated timestamp NULL DEFAULT now(),
    sysdatemodified timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL
)
