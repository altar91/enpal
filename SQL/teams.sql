CREATE TABLE enpal.teams(
    team_id integer PRIMARY KEY,
    availability_from DATE,
    availability_till DATE,
    skill_level NUMERIC(20),
    sysdatecreated timestamp NULL DEFAULT now(),
    sysdatemodified timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL
)

