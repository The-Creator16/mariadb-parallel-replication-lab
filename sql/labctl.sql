
CREATE DATABASE IF NOT EXISTS labctl;

-- The heartbeat tracks domain 0
CREATE TABLE IF NOT EXISTS labctl.heartbeat (
  id   TINYINT UNSIGNED PRIMARY KEY,
  ts   DATETIME(6)      NOT NULL,
  seq  BIGINT UNSIGNED  NOT NULL
) ENGINE=InnoDB;
INSERT IGNORE INTO labctl.heartbeat VALUES (1, NOW(6), 0);

DELIMITER //

DROP PROCEDURE IF EXISTS labctl.wl_independent //
CREATE PROCEDURE labctl.wl_independent(
  IN p_client INT, IN p_clients INT, IN p_tx INT, IN p_rows INT, IN p_accounts INT)
BEGIN
  DECLARE i INT DEFAULT 0;
  DECLARE j INT;
  DECLARE acc INT UNSIGNED;
  DECLARE per_client INT DEFAULT p_accounts DIV p_clients;
  WHILE i < p_tx DO
    START TRANSACTION;
    SET j = 0;
    WHILE j < p_rows DO
      SET acc = FLOOR(RAND() * per_client) * p_clients + p_client + 1;
      UPDATE lab.accounts
         SET balance = balance + 1, version = version + 1, updated_at = NOW(6)
       WHERE id = acc;
      INSERT INTO lab.ledger (account_id, client_id, amount, created_at, note)
      VALUES (acc, p_client, 1, NOW(6), CONCAT('independent c', p_client, ' tx', i, ' r', j));
      SET j = j + 1;
    END WHILE;
    COMMIT;
    SET i = i + 1;
  END WHILE;
END //

DROP PROCEDURE IF EXISTS labctl.wl_hotspot //
CREATE PROCEDURE labctl.wl_hotspot(
  IN p_client INT, IN p_clients INT, IN p_tx INT, IN p_rows INT, IN p_accounts INT,
  IN p_hot INT)
BEGIN
  DECLARE i INT DEFAULT 0;
  DECLARE j INT;
  DECLARE acc INT UNSIGNED;
  DECLARE hot_id INT UNSIGNED;
  DECLARE per_client INT DEFAULT p_accounts DIV p_clients;
  WHILE i < p_tx DO
    START TRANSACTION;
    SET j = 0;
    WHILE j < p_rows DO
      SET acc = FLOOR(RAND() * per_client) * p_clients + p_client + 1;
      UPDATE lab.accounts
         SET balance = balance + 1, version = version + 1, updated_at = NOW(6)
       WHERE id = acc;
      INSERT INTO lab.ledger (account_id, client_id, amount, created_at, note)
      VALUES (acc, p_client, 1, NOW(6), CONCAT('hotspot c', p_client, ' tx', i, ' r', j));
      SET j = j + 1;
    END WHILE;
    -- Pick the counter first. Writing RAND() inside the WHERE clause would
    -- re-evaluate it for every row, force a full scan and lock *every*
    -- counter -- a classic way to serialise a whole application by accident
    SET hot_id = FLOOR(RAND() * p_hot) + 1;
    UPDATE lab.hot_counters SET value = value + 1 WHERE id = hot_id;
    COMMIT;
    SET i = i + 1;
  END WHILE;
END //

DROP PROCEDURE IF EXISTS labctl.wl_own_table //
CREATE PROCEDURE labctl.wl_own_table(
  IN p_client INT, IN p_tx INT, IN p_rows INT, IN p_engine VARCHAR(10))
BEGIN
  DECLARE i INT DEFAULT 0;
  SET @wl_sql = CONCAT(
    'INSERT INTO lab.own_', p_engine, '_', p_client,
    ' (client_id, created_at, note) SELECT ?, NOW(6), CONCAT(''own '', ?, '' r'', seq)',
    ' FROM seq_1_to_', p_rows);
  PREPARE wl_stmt FROM @wl_sql;
  SET @wl_client = p_client;
  WHILE i < p_tx DO
    START TRANSACTION;
    SET @wl_i = i;
    EXECUTE wl_stmt USING @wl_client, @wl_i;
    COMMIT;
    SET i = i + 1;
  END WHILE;
  DEALLOCATE PREPARE wl_stmt;
END //

-- Creates lab.own_innodb_0..n-1 and lab.own_myisam_0..n-1 (called by reset)
DROP PROCEDURE IF EXISTS labctl.create_own_tables //
CREATE PROCEDURE labctl.create_own_tables(IN p_n INT)
BEGIN
  DECLARE c INT DEFAULT 0;
  WHILE c < p_n DO
    SET @ddl = CONCAT('CREATE TABLE lab.own_innodb_', c, ' (',
      'id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, client_id SMALLINT UNSIGNED NOT NULL, ',
      'created_at DATETIME(6) NOT NULL, note VARCHAR(200) NOT NULL, KEY (created_at)) ENGINE=InnoDB');
    PREPARE s FROM @ddl; EXECUTE s; DEALLOCATE PREPARE s;
    SET @ddl = REPLACE(REPLACE(@ddl, 'own_innodb_', 'own_myisam_'), 'ENGINE=InnoDB', 'ENGINE=MyISAM');
    PREPARE s FROM @ddl; EXECUTE s; DEALLOCATE PREPARE s;
    SET c = c + 1;
  END WHILE;
END //

DELIMITER ;
