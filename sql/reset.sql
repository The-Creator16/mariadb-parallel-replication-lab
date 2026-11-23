-- Rebuild the workload tables before each run. Row counts come from the plan
DROP DATABASE IF EXISTS lab;
CREATE DATABASE lab;
-- seq_1_to_N tables are virtual and resolve in the current database
USE lab;

CREATE TABLE lab.accounts (
  id          INT UNSIGNED  PRIMARY KEY,
  balance     BIGINT        NOT NULL DEFAULT 0,
  version     INT UNSIGNED  NOT NULL DEFAULT 0,
  updated_at  DATETIME(6)   NOT NULL,
  KEY idx_updated (updated_at)
) ENGINE=InnoDB;

CREATE TABLE lab.ledger (
  id          BIGINT UNSIGNED   AUTO_INCREMENT PRIMARY KEY,
  account_id  INT UNSIGNED      NOT NULL,
  client_id   SMALLINT UNSIGNED NOT NULL,
  amount      INT               NOT NULL,
  created_at  DATETIME(6)       NOT NULL,
  note        VARCHAR(200)      NOT NULL,
  KEY idx_account (account_id),
  KEY idx_client_time (client_id, created_at)
) ENGINE=InnoDB;

CREATE TABLE lab.hot_counters (
  id     TINYINT UNSIGNED PRIMARY KEY,
  value  BIGINT NOT NULL
) ENGINE=InnoDB;

CREATE TABLE lab.archive (
  id       INT UNSIGNED PRIMARY KEY,
  payload  VARCHAR(255) NOT NULL,
  created  DATETIME     NOT NULL
) ENGINE=InnoDB;

INSERT INTO lab.accounts (id, balance, version, updated_at)
  SELECT seq, 1000, 0, NOW(6) FROM seq_1_to_{accounts};

INSERT INTO lab.hot_counters SELECT seq, 0 FROM seq_1_to_255;

INSERT INTO lab.archive (id, payload, created)
  SELECT seq, REPEAT(SHA1(seq), 5), NOW() FROM seq_1_to_{archive_rows};

CALL labctl.create_own_tables({own_tables});
