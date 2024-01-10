CREATE DATABASE info21;

\c info21;


DROP TYPE IF EXISTS check_status;
CREATE TYPE check_status AS ENUM ('Start', 'Success', 'Failure');

CREATE TABLE IF NOT EXISTS peers
(
  nickname VARCHAR(255) PRIMARY KEY,
  birthday DATE
);

CREATE TABLE IF NOT EXISTS tasks
(
  title       VARCHAR(255) PRIMARY KEY,
  parent_task VARCHAR(255),
  max_xp      INT
);

CREATE TABLE IF NOT EXISTS checks
(
  id   SERIAL PRIMARY KEY,
  peer VARCHAR(255),
  task VARCHAR(255),
  date DATE,
  FOREIGN KEY (peer) REFERENCES peers (nickname),
  FOREIGN KEY (task) REFERENCES tasks (title)
);

CREATE TABLE IF NOT EXISTS p2p
(
  id            SERIAL PRIMARY KEY,
  "check"       INT,
  checking_peer VARCHAR(255),
  state         check_status,
  time          TIME,
  FOREIGN KEY ("check") REFERENCES Checks (id),
  FOREIGN KEY (checking_peer) REFERENCES Peers (nickname)
);

CREATE TABLE IF NOT EXISTS verter
(
  id      SERIAL PRIMARY KEY,
  "check" INT,
  state   check_status,
  time    TIME,
  FOREIGN KEY ("check") REFERENCES Checks (id),
  CONSTRAINT fk_verter_check FOREIGN KEY ("check") REFERENCES Checks (id)
);

CREATE TABLE IF NOT EXISTS transferred_points
(
  id            SERIAL PRIMARY KEY,
  checking_peer VARCHAR(255),
  checked_peer  VARCHAR(255),
  points_amount INT,
  FOREIGN KEY (checking_peer) REFERENCES Peers (nickname),
  FOREIGN KEY (checked_peer) REFERENCES Peers (nickname)
);

CREATE TABLE IF NOT EXISTS friends
(
  id    SERIAL PRIMARY KEY,
  peer1 VARCHAR(255),
  peer2 VARCHAR(255),
  FOREIGN KEY (peer1) REFERENCES Peers (nickname),
  FOREIGN KEY (peer2) REFERENCES Peers (nickname)
);

CREATE TABLE IF NOT EXISTS recommendations
(
  id               SERIAL PRIMARY KEY,
  peer             VARCHAR(255),
  recommended_peer VARCHAR(255),
  FOREIGN KEY (peer) REFERENCES Peers (nickname),
  FOREIGN KEY (recommended_peer) REFERENCES Peers (nickname)
);

CREATE TABLE IF NOT EXISTS xp
(
  id        SERIAL PRIMARY KEY,
  "check"   INT,
  xp_amount INT,
  FOREIGN KEY ("check") REFERENCES Checks (id)
);

CREATE TABLE IF NOT EXISTS time_tracking
(
  id    SERIAL PRIMARY KEY,
  peer  VARCHAR(255),
  date  DATE,
  time  TIME,
  state INT,
  FOREIGN KEY (peer) REFERENCES Peers (nickname)
);

CREATE OR REPLACE PROCEDURE import_data_from_csv(
  IN table_name TEXT,
  IN file_path TEXT,
  IN csv_separator TEXT
)
AS
$$
BEGIN
  EXECUTE format('COPY %I FROM %L DELIMITER %L CSV HEADER;',
                 table_name,
                 file_path,
                 csv_separator);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE export_data_to_csv(
  IN table_name TEXT,
  IN file_path TEXT,
  IN csv_separator TEXT
)
AS
$$
BEGIN
  EXECUTE format('COPY %I TO %L DELIMITER %L CSV HEADER;',
                 table_name,
                 file_path,
                 csv_separator);
END;
$$ LANGUAGE plpgsql;