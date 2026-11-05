-- ============================================================
-- MediSlot migration 002: users.user_id is the only id
-- ------------------------------------------------------------
-- BEFORE  doctors.doctor_id   (own AUTO_INCREMENT primary key) + doctors.user_id
--         patients.patient_id (own AUTO_INCREMENT primary key) + patients.user_id
-- AFTER   doctors.user_id     PRIMARY KEY (and FK to users)
--         patients.user_id    PRIMARY KEY (and FK to users)
--         doctor_id / patient_id columns on the other tables stay, but
--         they now hold the person's users.user_id and reference
--         doctors (user_id) / patients (user_id).
--
-- Run ONCE on an existing database (MySQL Workbench: open this file and
-- run all). New databases do not need it, schema.sql already has the new
-- layout.
--
-- What it does
--   1. Refuses to run if the old layout is not found, or if a doctor
--      whose id will change already has BOOKED appointments (their slot
--      ids live in DynamoDB and cannot be rewritten safely here).
--   2. Drops the foreign keys that point at doctors / patients.
--   3. Rewrites doctor_id / patient_id values in the child tables
--      (availability, blackouts, consultation types, appointments) from
--      the old doctor_id / patient_id to the person's user_id.
--   4. Makes user_id the primary key and drops doctor_id / patient_id.
--   5. Re-creates the foreign keys against user_id.
--
-- Afterwards: DynamoDB slot ids look like doctorId#typeId#startUTC. If any
-- doctor's id changed (the final check below tells you), delete the old
-- items from the slots table (or let them expire) and run
-- POST /doctors/me/materialize, or wait for the daily job, to create the
-- slots under the new ids.
-- ============================================================

USE medislot;

DROP PROCEDURE IF EXISTS medislot_migrate_user_id_keys;

DELIMITER //

CREATE PROCEDURE medislot_migrate_user_id_keys()
BEGIN
    DECLARE problems INT DEFAULT 0;
    -- Larger than any real id: lets us renumber without transient duplicate keys.
    DECLARE shift BIGINT UNSIGNED DEFAULT 4000000000000;

    -- ---- 1. safety checks ------------------------------------------------
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = DATABASE() AND table_name = 'doctors' AND column_name = 'doctor_id'
    ) OR NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = DATABASE() AND table_name = 'patients' AND column_name = 'patient_id'
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'doctors.doctor_id / patients.patient_id not found. Migration already applied or not needed.';
    END IF;

    SELECT COUNT(*) INTO problems
    FROM appointments a
    JOIN doctors d ON d.doctor_id = a.doctor_id
    WHERE d.doctor_id <> d.user_id AND a.status = 'BOOKED';
    IF problems > 0 THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Doctors whose id would change still have BOOKED appointments. Cancel or complete them first.';
    END IF;

    -- ---- 2. drop foreign keys that reference doctors / patients ----------
    ALTER TABLE appointments          DROP FOREIGN KEY fk_appointment_doctor_consultation;
    ALTER TABLE appointments          DROP FOREIGN KEY fk_appointment_doctor;
    ALTER TABLE appointments          DROP FOREIGN KEY fk_appointment_patient;
    ALTER TABLE availability_patterns DROP FOREIGN KEY fk_availability_doctor_consultation;
    ALTER TABLE availability_patterns DROP FOREIGN KEY fk_availability_doctor;
    ALTER TABLE doctor_blackouts      DROP FOREIGN KEY fk_doctor_blackouts_doctor;
    ALTER TABLE doctor_consultation_types DROP FOREIGN KEY fk_doctor_consultation_doctor;

    -- ---- 3. rewrite ids in the child tables ------------------------------
    -- Pass 1 moves every id far away from the real range, pass 2 sets the
    -- final value, so two rows can swap ids without ever colliding.
    UPDATE doctor_consultation_types x JOIN doctors d ON d.doctor_id = x.doctor_id
        SET x.doctor_id = d.user_id + shift;
    UPDATE doctor_consultation_types SET doctor_id = doctor_id - shift WHERE doctor_id >= shift;

    UPDATE availability_patterns x JOIN doctors d ON d.doctor_id = x.doctor_id
        SET x.doctor_id = d.user_id + shift;
    UPDATE availability_patterns SET doctor_id = doctor_id - shift WHERE doctor_id >= shift;

    UPDATE doctor_blackouts x JOIN doctors d ON d.doctor_id = x.doctor_id
        SET x.doctor_id = d.user_id + shift;
    UPDATE doctor_blackouts SET doctor_id = doctor_id - shift WHERE doctor_id >= shift;

    UPDATE appointments x JOIN doctors d ON d.doctor_id = x.doctor_id
        SET x.doctor_id = d.user_id + shift;
    UPDATE appointments x JOIN patients p ON p.patient_id = x.patient_id
        SET x.patient_id = p.user_id + shift;
    -- MySQL evaluates SET assignments left to right, so slot_id must be set
    -- before doctor_id changes.
    UPDATE appointments
        SET slot_id = CONCAT(doctor_id - shift, '#', SUBSTRING_INDEX(slot_id, '#', -2)),
            doctor_id = doctor_id - shift,
            patient_id = patient_id - shift
        WHERE doctor_id >= shift;

    -- ---- 4. user_id becomes the primary key ------------------------------
    ALTER TABLE doctors MODIFY doctor_id BIGINT UNSIGNED NOT NULL;
    ALTER TABLE doctors DROP PRIMARY KEY, ADD PRIMARY KEY (user_id);
    ALTER TABLE doctors DROP INDEX uq_doctors_user_id, DROP COLUMN doctor_id;

    ALTER TABLE patients MODIFY patient_id BIGINT UNSIGNED NOT NULL;
    ALTER TABLE patients DROP PRIMARY KEY, ADD PRIMARY KEY (user_id);
    ALTER TABLE patients DROP INDEX uq_patients_user_id, DROP COLUMN patient_id;

    -- ---- 5. foreign keys against user_id ---------------------------------
    ALTER TABLE doctor_consultation_types
        ADD CONSTRAINT fk_doctor_consultation_doctor FOREIGN KEY (doctor_id)
            REFERENCES doctors (user_id) ON UPDATE RESTRICT ON DELETE RESTRICT;

    ALTER TABLE doctor_blackouts
        ADD CONSTRAINT fk_doctor_blackouts_doctor FOREIGN KEY (doctor_id)
            REFERENCES doctors (user_id) ON UPDATE RESTRICT ON DELETE RESTRICT;

    ALTER TABLE availability_patterns
        ADD CONSTRAINT fk_availability_doctor FOREIGN KEY (doctor_id)
            REFERENCES doctors (user_id) ON UPDATE RESTRICT ON DELETE RESTRICT,
        ADD CONSTRAINT fk_availability_doctor_consultation FOREIGN KEY (doctor_id, consultation_type_id)
            REFERENCES doctor_consultation_types (doctor_id, consultation_type_id)
            ON UPDATE RESTRICT ON DELETE RESTRICT;

    ALTER TABLE appointments
        ADD CONSTRAINT fk_appointment_doctor FOREIGN KEY (doctor_id)
            REFERENCES doctors (user_id) ON UPDATE RESTRICT ON DELETE RESTRICT,
        ADD CONSTRAINT fk_appointment_patient FOREIGN KEY (patient_id)
            REFERENCES patients (user_id) ON UPDATE RESTRICT ON DELETE RESTRICT,
        ADD CONSTRAINT fk_appointment_doctor_consultation FOREIGN KEY (doctor_id, consultation_type_id)
            REFERENCES doctor_consultation_types (doctor_id, consultation_type_id)
            ON UPDATE RESTRICT ON DELETE RESTRICT;
END//

DELIMITER ;

-- Remember which doctors get a new id (needed for the DynamoDB note above).
SELECT COUNT(*) AS doctors_whose_id_will_change
FROM doctors WHERE doctor_id <> user_id;

CALL medislot_migrate_user_id_keys();
DROP PROCEDURE medislot_migrate_user_id_keys;

-- ---- check the result ------------------------------------------------------
SHOW CREATE TABLE doctors;
SHOW CREATE TABLE patients;

-- Every child row must point at an existing doctor / patient (all three return 0).
SELECT COUNT(*) AS orphan_consultation_types FROM doctor_consultation_types x
    LEFT JOIN doctors d ON d.user_id = x.doctor_id WHERE d.user_id IS NULL;
SELECT COUNT(*) AS orphan_availability FROM availability_patterns x
    LEFT JOIN doctors d ON d.user_id = x.doctor_id WHERE d.user_id IS NULL;
SELECT COUNT(*) AS orphan_appointments FROM appointments x
    LEFT JOIN doctors d ON d.user_id = x.doctor_id
    LEFT JOIN patients p ON p.user_id = x.patient_id WHERE d.user_id IS NULL OR p.user_id IS NULL;
