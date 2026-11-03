-- ============================================================
-- MediSlot migration 001: split consultation_types into two tables
-- ------------------------------------------------------------
-- BEFORE  consultation_types   one row per doctor and type
--                              (doctor_id, name, duration, fee, capacity, is_active)
-- AFTER   consultation_types          catalog of exactly 4 types (ids 1-4)
--         doctor_consultation_types   per-doctor fee, capacity, is_active
--
-- New ids:  1 Quick Consult   2 Standard Consult
--           3 Detailed Consult   4 Health Camp Session
--
-- Run ONCE on an existing database that still has the old layout
-- (MySQL Workbench: open this file and run all). New databases do not
-- need it, schema.sql already creates the new layout.
--
-- What it does
--   1. Refuses to run if the old layout is not found, or if the data
--      cannot be mapped (unknown type name, or a schedule or booking
--      that points at another doctor's consultation type).
--   2. Renames the old table to consultation_types_legacy (kept as a
--      backup; drop it yourself when you are happy).
--   3. Creates the new tables and copies every doctor's fee, capacity
--      and active flag across.
--   4. Re-points availability_patterns and appointments to the new
--      catalog ids and adds foreign keys so a doctor can only schedule
--      or be booked for a type they offer.
--
-- Afterwards: slot ids in DynamoDB contain the consultation type id
-- (doctorId#typeId#startUTC). Run POST /doctors/me/materialize (or wait
-- for the daily job). Unbooked slots under old ids are withdrawn
-- automatically and new ones are created. Slots that already have
-- bookings keep their old id, so existing bookings stay valid.
-- ============================================================

USE medislot;

DROP PROCEDURE IF EXISTS medislot_migrate_consultation_types;

DELIMITER //

CREATE PROCEDURE medislot_migrate_consultation_types()
BEGIN
    DECLARE problems INT DEFAULT 0;

    -- ---- 1. safety checks ------------------------------------------------
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = 'consultation_types'
          AND column_name = 'doctor_id'
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'consultation_types is not in the old layout. Migration already applied or not needed.';
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = DATABASE() AND table_name = 'consultation_types_legacy'
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'consultation_types_legacy already exists. Drop or rename it first.';
    END IF;

    SELECT COUNT(*) INTO problems FROM consultation_types
    WHERE consultation_name NOT IN ('Quick Consult','Standard Consult','Detailed Consult','Health Camp Session');
    IF problems > 0 THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Some consultation_types rows have a name outside the four approved types. Fix them first.';
    END IF;

    SELECT COUNT(*) INTO problems FROM availability_patterns a
    JOIN consultation_types c ON c.consultation_type_id = a.consultation_type_id
    WHERE c.doctor_id <> a.doctor_id;
    IF problems > 0 THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Some availability_patterns point at another doctor''s consultation type. Fix them first.';
    END IF;

    SELECT COUNT(*) INTO problems FROM appointments a
    JOIN consultation_types c ON c.consultation_type_id = a.consultation_type_id
    WHERE c.doctor_id <> a.doctor_id;
    IF problems > 0 THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Some appointments point at another doctor''s consultation type. Fix them first.';
    END IF;

    -- ---- 2. keep the old table as a backup -------------------------------
    RENAME TABLE consultation_types TO consultation_types_legacy;

    ALTER TABLE availability_patterns DROP FOREIGN KEY fk_availability_consultation;
    ALTER TABLE appointments          DROP FOREIGN KEY fk_appointment_consultation;
    -- MySQL keeps the index it created for each dropped foreign key; remove them.
    ALTER TABLE availability_patterns DROP INDEX fk_availability_consultation;
    ALTER TABLE appointments          DROP INDEX fk_appointment_consultation;

    -- ---- 3. new catalog (same definition as schema.sql) ------------------
    CREATE TABLE consultation_types (
        consultation_type_id BIGINT UNSIGNED NOT NULL,
        consultation_name VARCHAR(100) NOT NULL,
        duration_minutes SMALLINT UNSIGNED NOT NULL,
        min_capacity SMALLINT UNSIGNED NOT NULL DEFAULT 1,
        max_capacity SMALLINT UNSIGNED NOT NULL DEFAULT 1,
        created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
        PRIMARY KEY (consultation_type_id),
        UNIQUE KEY uq_consultation_types_name (consultation_name),
        CONSTRAINT chk_consultation_types_id CHECK (consultation_type_id BETWEEN 1 AND 4),
        CONSTRAINT chk_consultation_types_name CHECK (consultation_name IN ('Quick Consult','Standard Consult','Detailed Consult','Health Camp Session')),
        CONSTRAINT chk_consultation_types_duration CHECK (duration_minutes BETWEEN 5 AND 240),
        CONSTRAINT chk_consultation_types_capacity CHECK (min_capacity >= 1 AND max_capacity >= min_capacity AND max_capacity <= 50)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

    INSERT INTO consultation_types (consultation_type_id, consultation_name, duration_minutes, min_capacity, max_capacity) VALUES
        (1, 'Quick Consult', 15, 1, 1),
        (2, 'Standard Consult', 30, 1, 1),
        (3, 'Detailed Consult', 60, 1, 1),
        (4, 'Health Camp Session', 60, 2, 10);

    CREATE TABLE doctor_consultation_types (
        doctor_id BIGINT UNSIGNED NOT NULL,
        consultation_type_id BIGINT UNSIGNED NOT NULL,
        fee DECIMAL(10,2) NOT NULL DEFAULT 0.00,
        slot_capacity SMALLINT UNSIGNED NOT NULL DEFAULT 1,
        is_active BOOLEAN NOT NULL DEFAULT TRUE,
        created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
        PRIMARY KEY (doctor_id, consultation_type_id),
        KEY idx_doctor_consultation_type (consultation_type_id),
        KEY idx_doctor_consultation_active (doctor_id, is_active),
        CONSTRAINT fk_doctor_consultation_doctor FOREIGN KEY (doctor_id)
            REFERENCES doctors (doctor_id) ON UPDATE RESTRICT ON DELETE RESTRICT,
        CONSTRAINT fk_doctor_consultation_type FOREIGN KEY (consultation_type_id)
            REFERENCES consultation_types (consultation_type_id) ON UPDATE RESTRICT ON DELETE RESTRICT,
        CONSTRAINT chk_doctor_consultation_fee CHECK (fee >= 0),
        CONSTRAINT chk_doctor_consultation_capacity CHECK (slot_capacity BETWEEN 1 AND 50)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

    -- ---- 4. copy each doctor's fee / capacity / active flag --------------
    INSERT INTO doctor_consultation_types
        (doctor_id, consultation_type_id, fee, slot_capacity, is_active, created_at, updated_at)
    SELECT l.doctor_id, c.consultation_type_id, l.fee, l.slot_capacity, l.is_active, l.created_at, l.updated_at
    FROM consultation_types_legacy l
    JOIN consultation_types c ON c.consultation_name = l.consultation_name;

    -- ---- 5. re-point schedules and bookings to the catalog ids -----------
    UPDATE availability_patterns a
    JOIN consultation_types_legacy l ON l.consultation_type_id = a.consultation_type_id
    JOIN consultation_types c ON c.consultation_name = l.consultation_name
    SET a.consultation_type_id = c.consultation_type_id;

    UPDATE appointments a
    JOIN consultation_types_legacy l ON l.consultation_type_id = a.consultation_type_id
    JOIN consultation_types c ON c.consultation_name = l.consultation_name
    SET a.consultation_type_id = c.consultation_type_id;

    -- ---- 6. a doctor can only schedule / be booked for a type they offer --
    ALTER TABLE availability_patterns
        ADD KEY idx_availability_doctor_consultation (doctor_id, consultation_type_id),
        ADD CONSTRAINT fk_availability_doctor_consultation FOREIGN KEY (doctor_id, consultation_type_id)
            REFERENCES doctor_consultation_types (doctor_id, consultation_type_id)
            ON UPDATE RESTRICT ON DELETE RESTRICT;

    ALTER TABLE appointments
        ADD KEY idx_appointments_doctor_consultation (doctor_id, consultation_type_id),
        ADD CONSTRAINT fk_appointment_doctor_consultation FOREIGN KEY (doctor_id, consultation_type_id)
            REFERENCES doctor_consultation_types (doctor_id, consultation_type_id)
            ON UPDATE RESTRICT ON DELETE RESTRICT;
END//

DELIMITER ;

CALL medislot_migrate_consultation_types();
DROP PROCEDURE medislot_migrate_consultation_types;

-- ---- check the result ------------------------------------------------------
SELECT * FROM consultation_types ORDER BY consultation_type_id;

SELECT d.doctor_id, d.consultation_type_id, c.consultation_name, c.duration_minutes,
       d.fee, d.slot_capacity, d.is_active
FROM doctor_consultation_types d
JOIN consultation_types c ON c.consultation_type_id = d.consultation_type_id
ORDER BY d.doctor_id, d.consultation_type_id;

-- Row counts must match: every old row became one doctor_consultation_types row.
SELECT (SELECT COUNT(*) FROM consultation_types_legacy) AS old_rows,
       (SELECT COUNT(*) FROM doctor_consultation_types) AS new_rows;

-- When you are happy with the result you can remove the backup:
-- DROP TABLE consultation_types_legacy;
