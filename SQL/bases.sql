CREATE TABLE enpal.bases(
    team_id team_id NUMERIC(20),
    latitude     DECIMAL(20,5),
    longitude    DECIMAL(20,5),
    sysdatecreated timestamp NULL DEFAULT now(),
    sysdatemodified timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL
)

