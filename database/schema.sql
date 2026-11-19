-- ============================================================
-- MediSlot: Initial Authentication Database Schema
-- Milestone 2 - Foundation and Authentication
-- Database: Amazon RDS for MySQL
-- ============================================================
--
-- Doctor license number format:
--   Two uppercase letters, a hyphen, and exactly five digits.
--   Example: TS-12345
--
-- Requires MySQL 8.0.16+ for enforced CHECK constraints.
-- ============================================================

CREATE DATABASE IF NOT EXISTS medislot
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_0900_ai_ci;

USE medislot;


-- ============================================================
-- USERS TABLE
-- Stores shared authentication and account information.
-- ============================================================

CREATE TABLE IF NOT EXISTS users (
    user_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,

    email VARCHAR(254) NOT NULL,

    password_hash VARCHAR(255) NOT NULL,

    role VARCHAR(10) NOT NULL,

    account_status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',

    created_at DATETIME(6) NOT NULL
        DEFAULT CURRENT_TIMESTAMP(6),

    updated_at DATETIME(6) NOT NULL
        DEFAULT CURRENT_TIMESTAMP(6)
        ON UPDATE CURRENT_TIMESTAMP(6),

    PRIMARY KEY (user_id),

    UNIQUE KEY uq_users_email (email),

    KEY idx_users_role_status (role, account_status),

    CONSTRAINT chk_users_role
        CHECK (role IN ('DOCTOR', 'PATIENT')),

    CONSTRAINT chk_users_account_status
        CHECK (
            account_status IN (
                'ACTIVE',
                'INACTIVE',
                'SUSPENDED'
            )
        )

) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_0900_ai_ci;


-- ============================================================
-- PATIENTS TABLE
-- Stores patient-specific profile information.
-- ============================================================

CREATE TABLE IF NOT EXISTS patients (
    -- Same value as users.user_id: the account id IS the patient id.
    user_id BIGINT UNSIGNED NOT NULL,

    first_name VARCHAR(50) NOT NULL,

    last_name VARCHAR(50) NOT NULL,

    phone VARCHAR(16) NOT NULL,

    date_of_birth DATE NOT NULL,

    gender VARCHAR(30) NOT NULL,

    address VARCHAR(500) NOT NULL,

    -- Milestone 4: city and emergency contact. NULL only for accounts created
    -- before Milestone 4; new registrations always supply them.
    city VARCHAR(100) NULL,

    emergency_contact_name VARCHAR(100) NULL,

    emergency_contact_phone VARCHAR(16) NULL,

    created_at DATETIME(6) NOT NULL
        DEFAULT CURRENT_TIMESTAMP(6),

    updated_at DATETIME(6) NOT NULL
        DEFAULT CURRENT_TIMESTAMP(6)
        ON UPDATE CURRENT_TIMESTAMP(6),

    -- One patient profile per account (primary key = users.user_id)
    PRIMARY KEY (user_id),

    KEY idx_patients_name (last_name, first_name),

    KEY idx_patients_phone (phone),

    CONSTRAINT fk_patients_user
        FOREIGN KEY (user_id)
        REFERENCES users (user_id)
        ON UPDATE RESTRICT
        ON DELETE RESTRICT,

    CONSTRAINT chk_patients_gender
        CHECK (
            gender IN (
                'FEMALE',
                'MALE',
                'OTHER',
                'PREFER_NOT_TO_SAY'
            )
        )

) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_0900_ai_ci;


-- ============================================================
-- HOSPITALS
-- Reference list shown as a dropdown on the doctor dashboard.
-- doctors.hospital_id stores the id from this table.
-- The rows below are sample data: add, edit or hide (is_active = 0) as needed.
-- ============================================================

CREATE TABLE IF NOT EXISTS hospitals (
    hospital_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,

    hospital_name VARCHAR(150) NOT NULL,

    city VARCHAR(100) NOT NULL,

    state VARCHAR(100) NOT NULL,

    is_active BOOLEAN NOT NULL DEFAULT TRUE,

    created_at DATETIME(6) NOT NULL
        DEFAULT CURRENT_TIMESTAMP(6),

    updated_at DATETIME(6) NOT NULL
        DEFAULT CURRENT_TIMESTAMP(6)
        ON UPDATE CURRENT_TIMESTAMP(6),

    PRIMARY KEY (hospital_id),

    UNIQUE KEY uq_hospitals_name_city (hospital_name, city),

    KEY idx_hospitals_active_name (is_active, hospital_name),

    -- Milestone 4 doctor search: the city filter resolves hospital ids here first.
    KEY idx_hospitals_city (city, hospital_name)

) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_0900_ai_ci;

INSERT IGNORE INTO hospitals (hospital_id, hospital_name, city, state) VALUES
    (1, 'Apollo Hospitals, Jubilee Hills', 'Hyderabad', 'Telangana'),
    (2, 'Yashoda Hospitals, Secunderabad', 'Hyderabad', 'Telangana'),
    (3, 'KIMS Hospitals, Secunderabad', 'Hyderabad', 'Telangana'),
    (4, 'CARE Hospitals, Banjara Hills', 'Hyderabad', 'Telangana'),
    (5, 'Continental Hospitals, Gachibowli', 'Hyderabad', 'Telangana'),
    (6, 'AIG Hospitals, Gachibowli', 'Hyderabad', 'Telangana'),
    (7, 'Medicover Hospitals, HITEC City', 'Hyderabad', 'Telangana'),
    (8, 'Sunshine Hospitals, Secunderabad', 'Hyderabad', 'Telangana'),
    (9, 'Rainbow Children''s Hospital, Banjara Hills', 'Hyderabad', 'Telangana'),
    (10, 'Osmania General Hospital', 'Hyderabad', 'Telangana'),
    (11, 'Apollo Hospitals, Greams Road', 'Chennai', 'Tamil Nadu'),
    (12, 'Manipal Hospital, Old Airport Road', 'Bengaluru', 'Karnataka'),
    (13, 'Fortis Hospital, Bannerghatta Road', 'Bengaluru', 'Karnataka'),
    (14, 'Max Super Speciality Hospital, Saket', 'New Delhi', 'Delhi');


-- ============================================================
-- SPECIALIZATIONS
-- Reference list shown as a dropdown on the doctor dashboard.
-- doctors.specialization_id stores the id from this table.
-- The rows below are sample data: add, edit or hide (is_active = 0) as needed.
-- ============================================================

CREATE TABLE IF NOT EXISTS specializations (
    specialization_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,

    specialization_name VARCHAR(100) NOT NULL,

    is_active BOOLEAN NOT NULL DEFAULT TRUE,

    created_at DATETIME(6) NOT NULL
        DEFAULT CURRENT_TIMESTAMP(6),

    updated_at DATETIME(6) NOT NULL
        DEFAULT CURRENT_TIMESTAMP(6)
        ON UPDATE CURRENT_TIMESTAMP(6),

    PRIMARY KEY (specialization_id),

    UNIQUE KEY uq_specializations_name (specialization_name)

) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_0900_ai_ci;

INSERT IGNORE INTO specializations (specialization_id, specialization_name) VALUES
    (1, 'Cardiology'),
    (2, 'Dentistry'),
    (3, 'Dermatology'),
    (4, 'Endocrinology'),
    (5, 'ENT (Otolaryngology)'),
    (6, 'Gastroenterology'),
    (7, 'General Medicine'),
    (8, 'General Surgery'),
    (9, 'Gynecology & Obstetrics'),
    (10, 'Nephrology'),
    (11, 'Neurology'),
    (12, 'Oncology'),
    (13, 'Ophthalmology'),
    (14, 'Orthopedics'),
    (15, 'Pediatrics'),
    (16, 'Psychiatry'),
    (17, 'Pulmonology'),
    (18, 'Urology');


-- ============================================================
-- DOCTORS TABLE
-- Stores personal and professional doctor information.
-- ============================================================

CREATE TABLE IF NOT EXISTS doctors (
    -- Same value as users.user_id: the account id IS the doctor id.
    user_id BIGINT UNSIGNED NOT NULL,

    first_name VARCHAR(50) NOT NULL,

    last_name VARCHAR(50) NOT NULL,

    phone VARCHAR(16) NOT NULL,

    date_of_birth DATE NOT NULL,

    gender VARCHAR(30) NOT NULL,

    address VARCHAR(500) NOT NULL,

    specialization_id BIGINT UNSIGNED NOT NULL,

    hospital_id BIGINT UNSIGNED NOT NULL,

    license_number VARCHAR(8) NOT NULL,

    years_of_experience TINYINT UNSIGNED NOT NULL,

    clinic_address VARCHAR(500) NOT NULL,

    -- S3 object key of the profile photo (NULL until one is uploaded)
    photo_key VARCHAR(512) NULL,

    created_at DATETIME(6) NOT NULL
        DEFAULT CURRENT_TIMESTAMP(6),

    updated_at DATETIME(6) NOT NULL
        DEFAULT CURRENT_TIMESTAMP(6)
        ON UPDATE CURRENT_TIMESTAMP(6),

    -- One doctor profile per account (primary key = users.user_id)
    PRIMARY KEY (user_id),

    -- Every doctor must have a unique license number
    UNIQUE KEY uq_doctors_license_number (license_number),

    KEY idx_doctors_specialization (specialization_id),

    KEY idx_doctors_hospital (hospital_id),

    KEY idx_doctors_name (last_name, first_name),

    -- Milestone 4 doctor search (see docs/patient-domain.md for the reasoning):
    -- first-name prefix matches (idx_doctors_name only helps the last name), and
    -- the combined specialisation + hospital filter.
    KEY idx_doctors_first_name (first_name),

    KEY idx_doctors_spec_hospital (specialization_id, hospital_id),

    KEY idx_doctors_phone (phone),

    CONSTRAINT fk_doctors_user
        FOREIGN KEY (user_id)
        REFERENCES users (user_id)
        ON UPDATE RESTRICT
        ON DELETE RESTRICT,

    CONSTRAINT fk_doctors_specialization
        FOREIGN KEY (specialization_id)
        REFERENCES specializations (specialization_id)
        ON UPDATE RESTRICT
        ON DELETE RESTRICT,

    CONSTRAINT fk_doctors_hospital
        FOREIGN KEY (hospital_id)
        REFERENCES hospitals (hospital_id)
        ON UPDATE RESTRICT
        ON DELETE RESTRICT,

    CONSTRAINT chk_doctors_gender
        CHECK (
            gender IN (
                'FEMALE',
                'MALE',
                'OTHER',
                'PREFER_NOT_TO_SAY'
            )
        ),

    CONSTRAINT chk_doctors_years_experience
        CHECK (
            years_of_experience BETWEEN 0 AND 60
        ),

    -- Exactly two uppercase letters, a hyphen, and five digits
    CONSTRAINT chk_doctors_license_format
        CHECK (
            license_number REGEXP '^[A-Z]{2}-[0-9]{5}$'
        )

) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_0900_ai_ci;


-- ============================================================
-- END OF INITIAL AUTHENTICATION SCHEMA
-- ============================================================
-- ============================================================
-- MILESTONE 3: DOCTOR DOMAIN (additive; preserves Milestone 2)
-- ============================================================

-- ------------------------------------------------------------
-- CONSULTATION TYPES (catalog)
-- The four consultation types MediSlot supports, and nothing else.
-- Each type has a fixed id, name and duration. Per-doctor details
-- (fee, capacity, active flag) live in doctor_consultation_types.
--   1 Quick Consult        15 min, capacity 1
--   2 Standard Consult     30 min, capacity 1
--   3 Detailed Consult     60 min, capacity 1
--   4 Health Camp Session  60 min, capacity 2-10 (chosen per doctor)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS consultation_types (
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

INSERT IGNORE INTO consultation_types (consultation_type_id, consultation_name, duration_minutes, min_capacity, max_capacity) VALUES
    (1, 'Quick Consult', 15, 1, 1),
    (2, 'Standard Consult', 30, 1, 1),
    (3, 'Detailed Consult', 60, 1, 1),
    (4, 'Health Camp Session', 60, 2, 10);

-- ------------------------------------------------------------
-- DOCTOR CONSULTATION TYPES
-- Which catalog consultation types a doctor offers, with the
-- doctor's own fee, slot capacity and active flag.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS doctor_consultation_types (
    -- doctor_id holds the doctor's users.user_id
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
        REFERENCES doctors (user_id) ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT fk_doctor_consultation_type FOREIGN KEY (consultation_type_id)
        REFERENCES consultation_types (consultation_type_id) ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT chk_doctor_consultation_fee CHECK (fee >= 0),
    CONSTRAINT chk_doctor_consultation_capacity CHECK (slot_capacity BETWEEN 1 AND 50)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- A consultation type cannot be removed while availability exists for it:
-- fk_availability_doctor_consultation is RESTRICT, and the API refuses to
-- deactivate a type that still has active availability (remove that first).
CREATE TABLE IF NOT EXISTS availability_patterns (
    availability_pattern_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    doctor_id BIGINT UNSIGNED NOT NULL,
    consultation_type_id BIGINT UNSIGNED NULL,
    weekday TINYINT UNSIGNED NOT NULL COMMENT 'Monday=0 through Sunday=6',
    start_time TIME NOT NULL,
    end_time TIME NOT NULL,
    timezone_name VARCHAR(64) NOT NULL COMMENT 'IANA timezone, e.g. America/New_York',
    effective_from DATE NOT NULL,
    effective_to DATE NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (availability_pattern_id),
    KEY idx_availability_doctor_weekday (doctor_id, weekday, is_active),
    KEY idx_availability_doctor_consultation (doctor_id, consultation_type_id),
    CONSTRAINT fk_availability_doctor FOREIGN KEY (doctor_id)
        REFERENCES doctors (user_id) ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT fk_availability_doctor_consultation FOREIGN KEY (doctor_id, consultation_type_id)
        REFERENCES doctor_consultation_types (doctor_id, consultation_type_id) ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT chk_availability_weekday CHECK (weekday BETWEEN 0 AND 6),
    CONSTRAINT chk_availability_time CHECK (start_time < end_time),
    CONSTRAINT chk_availability_dates CHECK (effective_to IS NULL OR effective_to >= effective_from)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS doctor_blackouts (
    blackout_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    doctor_id BIGINT UNSIGNED NOT NULL,
    blackout_date DATE NOT NULL,
    reason VARCHAR(255) NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (blackout_id),
    UNIQUE KEY uq_doctor_blackout_date (doctor_id, blackout_date),
    KEY idx_blackout_date (blackout_date),
    CONSTRAINT fk_doctor_blackouts_doctor FOREIGN KEY (doctor_id)
        REFERENCES doctors (user_id) ON UPDATE RESTRICT ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ============================================================
-- SLOTS (Milestone 5): the bookable time slots, kept in RDS.
-- Appointments live in DynamoDB (table "appointments"); this table holds the
-- seat counter they compete for.
--
-- booked_count and version implement optimistic concurrency control. A booking reads
-- the row, then runs ONE conditional UPDATE:
--   SET booked_count = booked_count + 1, version = version + 1
--   WHERE slot_id = ? AND status = 'AVAILABLE' AND booked_count < capacity AND version = ?
-- MySQL checks the WHERE clause atomically, so two requests can never take the last
-- seat. One affected row means the seat is yours; zero means somebody else changed
-- the slot first and the request re-reads and retries (bounded).
--
-- slot_id is deterministic (doctor#consultation_type#UTC start), so refreshing the
-- slots is idempotent. slot_date is the doctor's local calendar date.
-- status: AVAILABLE | CANCELLED (doctor cancelled it) | REMOVED | BLACKED_OUT
-- ============================================================

CREATE TABLE IF NOT EXISTS slots (
    slot_id VARCHAR(64) NOT NULL,
    doctor_id BIGINT UNSIGNED NOT NULL,
    consultation_type_id BIGINT UNSIGNED NOT NULL,
    availability_pattern_id BIGINT UNSIGNED NULL,
    slot_date DATE NOT NULL,
    start_time_utc DATETIME(6) NOT NULL,
    end_time_utc DATETIME(6) NOT NULL,
    timezone_name VARCHAR(64) NOT NULL,
    local_start DATETIME NOT NULL,
    duration_minutes SMALLINT UNSIGNED NOT NULL,
    fee DECIMAL(10,2) NOT NULL,
    capacity SMALLINT UNSIGNED NOT NULL,
    booked_count SMALLINT UNSIGNED NOT NULL DEFAULT 0,
    version INT UNSIGNED NOT NULL DEFAULT 0,
    status VARCHAR(12) NOT NULL DEFAULT 'AVAILABLE',
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (slot_id),
    KEY idx_slots_doctor_date (doctor_id, slot_date, start_time_utc),
    KEY idx_slots_doctor_start (doctor_id, start_time_utc),
    KEY idx_slots_pattern (availability_pattern_id),
    CONSTRAINT fk_slots_doctor FOREIGN KEY (doctor_id)
        REFERENCES doctors (user_id) ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT fk_slots_doctor_consultation FOREIGN KEY (doctor_id, consultation_type_id)
        REFERENCES doctor_consultation_types (doctor_id, consultation_type_id) ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT fk_slots_pattern FOREIGN KEY (availability_pattern_id)
        REFERENCES availability_patterns (availability_pattern_id) ON UPDATE RESTRICT ON DELETE SET NULL,
    CONSTRAINT chk_slots_status CHECK (status IN ('AVAILABLE','CANCELLED','REMOVED','BLACKED_OUT')),
    CONSTRAINT chk_slots_capacity CHECK (capacity >= 1),
    CONSTRAINT chk_slots_booked CHECK (booked_count <= capacity),
    CONSTRAINT chk_slots_times CHECK (start_time_utc < end_time_utc)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ============================================================
-- REFUND LEDGER (Milestone 5)
-- One row per cancelled or no-show appointment. Refunds are RECORDED here only;
-- no money moves (there is no payment gateway). The appointment itself lives in
-- DynamoDB, so appointment_id is a plain number (the DynamoDB appointment id) with no foreign key; the
-- slot and start time are copied here so the patient's refund list needs no lookup.
-- UNIQUE (appointment_id) makes the insert idempotent: nobody is refunded twice.
-- ============================================================

CREATE TABLE IF NOT EXISTS refund_ledger (
    refund_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    appointment_id BIGINT UNSIGNED NOT NULL,
    patient_id BIGINT UNSIGNED NOT NULL,
    doctor_id BIGINT UNSIGNED NOT NULL,
    slot_id VARCHAR(64) NOT NULL,
    scheduled_start_utc DATETIME(6) NOT NULL,
    trigger_type VARCHAR(24) NOT NULL,
    tier VARCHAR(40) NOT NULL,
    refund_percent DECIMAL(5,2) NOT NULL,
    fee_amount DECIMAL(10,2) NOT NULL,
    refund_amount DECIMAL(10,2) NOT NULL,
    deduction_amount DECIMAL(10,2) NOT NULL,
    hours_before_start DECIMAL(10,2) NULL,
    currency CHAR(3) NOT NULL DEFAULT 'INR',
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (refund_id),
    UNIQUE KEY uq_refund_appointment (appointment_id),
    KEY idx_refund_patient_created (patient_id, created_at),
    KEY idx_refund_doctor_created (doctor_id, created_at),
    CONSTRAINT fk_refund_patient FOREIGN KEY (patient_id)
        REFERENCES patients (user_id) ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT fk_refund_doctor FOREIGN KEY (doctor_id)
        REFERENCES doctors (user_id) ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT chk_refund_trigger CHECK (trigger_type IN ('PATIENT_CANCELLATION','DOCTOR_CANCELLATION','NO_SHOW')),
    CONSTRAINT chk_refund_percent CHECK (refund_percent BETWEEN 0 AND 100),
    CONSTRAINT chk_refund_amounts CHECK (refund_amount >= 0 AND deduction_amount >= 0 AND refund_amount <= fee_amount)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- ============================================================
-- SAMPLE DATA FOR REVIEW / DEMO (dev only)
-- 15 doctors (ids 101-115), 6 patients (ids 201-206), the consultation types
-- each doctor offers, weekly availability and a few days off.
-- Every account below uses the password: MediSlot@2026
-- Doctor logins:  doctor1@gmail.com ... doctor15@gmail.com (ids 101-115)
-- Patient logins: patient1@gmail.com ... patient6@gmail.com (ids 201-206)
-- All statements are INSERT IGNORE with fixed ids, so re-running this file never
-- duplicates rows.
-- No slots and no appointments are stored here. Slots are created in the RDS `slots`
-- table by the slot materializer from availability_patterns (the deploy pipeline runs it
-- once, then it runs daily) for the next 30 days. Appointments are created by booking
-- and live in DynamoDB.
-- Remove this whole section for a production database.
-- ============================================================

-- Accounts (users)
INSERT IGNORE INTO users (user_id, email, password_hash, role, account_status) VALUES
    (101, 'doctor1@gmail.com', '$2b$12$BOXwUAX9sZCOaBye14D5SuWiz9gIzGa2xM/aRe0cAfeCoP2KfwzBK', 'DOCTOR', 'ACTIVE'),
    (102, 'doctor2@gmail.com', '$2b$12$BOXwUAX9sZCOaBye14D5SuWiz9gIzGa2xM/aRe0cAfeCoP2KfwzBK', 'DOCTOR', 'ACTIVE'),
    (103, 'doctor3@gmail.com', '$2b$12$BOXwUAX9sZCOaBye14D5SuWiz9gIzGa2xM/aRe0cAfeCoP2KfwzBK', 'DOCTOR', 'ACTIVE'),
    (104, 'doctor4@gmail.com', '$2b$12$BOXwUAX9sZCOaBye14D5SuWiz9gIzGa2xM/aRe0cAfeCoP2KfwzBK', 'DOCTOR', 'ACTIVE'),
    (105, 'doctor5@gmail.com', '$2b$12$BOXwUAX9sZCOaBye14D5SuWiz9gIzGa2xM/aRe0cAfeCoP2KfwzBK', 'DOCTOR', 'ACTIVE'),
    (106, 'doctor6@gmail.com', '$2b$12$BOXwUAX9sZCOaBye14D5SuWiz9gIzGa2xM/aRe0cAfeCoP2KfwzBK', 'DOCTOR', 'ACTIVE'),
    (107, 'doctor7@gmail.com', '$2b$12$BOXwUAX9sZCOaBye14D5SuWiz9gIzGa2xM/aRe0cAfeCoP2KfwzBK', 'DOCTOR', 'ACTIVE'),
    (108, 'doctor8@gmail.com', '$2b$12$BOXwUAX9sZCOaBye14D5SuWiz9gIzGa2xM/aRe0cAfeCoP2KfwzBK', 'DOCTOR', 'ACTIVE'),
    (109, 'doctor9@gmail.com', '$2b$12$BOXwUAX9sZCOaBye14D5SuWiz9gIzGa2xM/aRe0cAfeCoP2KfwzBK', 'DOCTOR', 'ACTIVE'),
    (110, 'doctor10@gmail.com', '$2b$12$BOXwUAX9sZCOaBye14D5SuWiz9gIzGa2xM/aRe0cAfeCoP2KfwzBK', 'DOCTOR', 'ACTIVE'),
    (111, 'doctor11@gmail.com', '$2b$12$BOXwUAX9sZCOaBye14D5SuWiz9gIzGa2xM/aRe0cAfeCoP2KfwzBK', 'DOCTOR', 'ACTIVE'),
    (112, 'doctor12@gmail.com', '$2b$12$BOXwUAX9sZCOaBye14D5SuWiz9gIzGa2xM/aRe0cAfeCoP2KfwzBK', 'DOCTOR', 'ACTIVE'),
    (113, 'doctor13@gmail.com', '$2b$12$BOXwUAX9sZCOaBye14D5SuWiz9gIzGa2xM/aRe0cAfeCoP2KfwzBK', 'DOCTOR', 'ACTIVE'),
    (114, 'doctor14@gmail.com', '$2b$12$BOXwUAX9sZCOaBye14D5SuWiz9gIzGa2xM/aRe0cAfeCoP2KfwzBK', 'DOCTOR', 'ACTIVE'),
    (115, 'doctor15@gmail.com', '$2b$12$BOXwUAX9sZCOaBye14D5SuWiz9gIzGa2xM/aRe0cAfeCoP2KfwzBK', 'DOCTOR', 'ACTIVE'),
    (201, 'patient1@gmail.com', '$2b$12$BOXwUAX9sZCOaBye14D5SuWiz9gIzGa2xM/aRe0cAfeCoP2KfwzBK', 'PATIENT', 'ACTIVE'),
    (202, 'patient2@gmail.com', '$2b$12$BOXwUAX9sZCOaBye14D5SuWiz9gIzGa2xM/aRe0cAfeCoP2KfwzBK', 'PATIENT', 'ACTIVE'),
    (203, 'patient3@gmail.com', '$2b$12$BOXwUAX9sZCOaBye14D5SuWiz9gIzGa2xM/aRe0cAfeCoP2KfwzBK', 'PATIENT', 'ACTIVE'),
    (204, 'patient4@gmail.com', '$2b$12$BOXwUAX9sZCOaBye14D5SuWiz9gIzGa2xM/aRe0cAfeCoP2KfwzBK', 'PATIENT', 'ACTIVE'),
    (205, 'patient5@gmail.com', '$2b$12$BOXwUAX9sZCOaBye14D5SuWiz9gIzGa2xM/aRe0cAfeCoP2KfwzBK', 'PATIENT', 'ACTIVE'),
    (206, 'patient6@gmail.com', '$2b$12$BOXwUAX9sZCOaBye14D5SuWiz9gIzGa2xM/aRe0cAfeCoP2KfwzBK', 'PATIENT', 'ACTIVE');

-- Doctor profiles
INSERT IGNORE INTO doctors (user_id, first_name, last_name, phone, date_of_birth, gender, address, specialization_id, hospital_id, license_number, years_of_experience, clinic_address) VALUES
    (101, 'Ananya', 'Reddy', '9876500101', '1982-03-14', 'FEMALE', 'Plot 21, Road 12, Banjara Hills, Hyderabad', 1, 1, 'TS-10101', 14, 'Cardiology OPD, Apollo Hospitals, Jubilee Hills, Hyderabad'),
    (102, 'Rohit', 'Sharma', '9876500102', '1988-07-02', 'MALE', 'Flat 302, Madhura Nagar, Hyderabad', 2, 4, 'TS-10102', 8, 'Dental Clinic, CARE Hospitals, Banjara Hills, Hyderabad'),
    (103, 'Meera', 'Iyer', '9876500103', '1985-11-21', 'FEMALE', 'Flat 8B, Gachibowli, Hyderabad', 3, 5, 'TS-10103', 11, 'Skin Clinic, Continental Hospitals, Gachibowli, Hyderabad'),
    (104, 'Karthik', 'Rao', '9876500104', '1980-01-30', 'MALE', 'House 14, Manikonda, Hyderabad', 4, 6, 'TS-10104', 16, 'Endocrine Wing, AIG Hospitals, Gachibowli, Hyderabad'),
    (105, 'Sneha', 'Kulkarni', '9876500105', '1987-05-09', 'FEMALE', 'Flat 5, Begumpet, Hyderabad', 5, 2, 'TS-10105', 9, 'ENT Clinic, Yashoda Hospitals, Secunderabad, Hyderabad'),
    (106, 'Vikram', 'Singh', '9876500106', '1978-09-17', 'MALE', 'Villa 9, Tarnaka, Hyderabad', 6, 3, 'TS-10106', 18, 'Gastro Centre, KIMS Hospitals, Secunderabad, Hyderabad'),
    (107, 'Farah', 'Khan', '9876500107', '1990-12-03', 'FEMALE', 'Flat 12, Madhapur, Hyderabad', 7, 7, 'TS-10107', 6, 'General Medicine OPD, Medicover Hospitals, HITEC City, Hyderabad'),
    (108, 'Arjun', 'Nair', '9876500108', '1984-04-26', 'MALE', 'Flat 77, Film Nagar, Hyderabad', 14, 1, 'TS-10108', 12, 'Ortho Clinic, Apollo Hospitals, Jubilee Hills, Hyderabad'),
    (109, 'Divya', 'Menon', '9876500109', '1986-08-15', 'FEMALE', 'Flat 4A, Road 3, Banjara Hills, Hyderabad', 15, 9, 'TS-10109', 10, 'Paediatric OPD, Rainbow Children''s Hospital, Banjara Hills, Hyderabad'),
    (110, 'Suresh', 'Babu', '9876500110', '1975-02-11', 'MALE', 'House 6, Kapra, Hyderabad', 11, 8, 'TS-10110', 20, 'Neuro Clinic, Sunshine Hospitals, Secunderabad, Hyderabad'),
    (111, 'Lakshmi', 'Prasad', '9876500111', '1981-06-28', 'FEMALE', 'Flat 19, Kukatpally, Hyderabad', 9, 3, 'TS-10111', 15, 'Women''s Clinic, KIMS Hospitals, Secunderabad, Hyderabad'),
    (112, 'Imran', 'Qureshi', '9876500112', '1989-10-05', 'MALE', 'Flat 3C, Greams Road, Chennai', 17, 11, 'TS-10112', 7, 'Chest Clinic, Apollo Hospitals, Greams Road, Chennai'),
    (113, 'Pooja', 'Verma', '9876500113', '1991-01-19', 'FEMALE', 'Flat 21, Nanakramguda, Hyderabad', 13, 5, 'TS-10113', 5, 'Eye Clinic, Continental Hospitals, Gachibowli, Hyderabad'),
    (114, 'Rajesh', 'Gupta', '9876500114', '1979-03-08', 'MALE', 'B-45, Saket, New Delhi', 18, 14, 'TS-10114', 17, 'Urology OPD, Max Super Speciality Hospital, Saket, New Delhi'),
    (115, 'Nandini', 'Krishnan', '9876500115', '1983-07-24', 'FEMALE', 'Flat 10, Indiranagar, Bengaluru', 16, 12, 'TS-10115', 13, 'Mind Care Clinic, Manipal Hospital, Old Airport Road, Bengaluru');

-- Patient profiles
INSERT IGNORE INTO patients (user_id, first_name, last_name, phone, date_of_birth, gender, address, city, emergency_contact_name, emergency_contact_phone) VALUES
    (201, 'Aarav', 'Mehta', '9123400201', '1995-04-12', 'MALE', 'Flat 14, Ameerpet, Hyderabad', 'Hyderabad', 'Kavita Mehta', '9123400301'),
    (202, 'Ishita', 'Das', '9123400202', '1992-09-30', 'FEMALE', 'House 2, Kondapur, Hyderabad', 'Hyderabad', 'Subir Das', '9123400302'),
    (203, 'Rahul', 'Joshi', '9123400203', '1988-01-17', 'MALE', 'Flat 6D, Miyapur, Hyderabad', 'Hyderabad', 'Sunita Joshi', '9123400303'),
    (204, 'Sana', 'Fatima', '9123400204', '2000-06-05', 'FEMALE', 'Flat 31, Tolichowki, Hyderabad', 'Hyderabad', 'Yusuf Ali', '9123400304'),
    (205, 'Tarun', 'Chowdary', '9123400205', '1976-11-23', 'MALE', 'Villa 11, Jubilee Hills, Hyderabad', 'Hyderabad', 'Madhavi Chowdary', '9123400305'),
    (206, 'Keerthi', 'Suresh', '9123400206', '1998-02-14', 'FEMALE', 'Flat 9, Velachery, Chennai', 'Chennai', 'Suresh Kumar', '9123400306');

-- Consultation types each doctor offers (fee in INR, seats per slot)
INSERT IGNORE INTO doctor_consultation_types (doctor_id, consultation_type_id, fee, slot_capacity, is_active) VALUES
    (101, 2, 1200.00, 1, 1),
    (101, 3, 2000.00, 1, 1),
    (102, 1, 500.00, 1, 1),
    (102, 2, 800.00, 1, 1),
    (102, 4, 300.00, 8, 1),
    (103, 1, 600.00, 1, 1),
    (103, 2, 900.00, 1, 1),
    (104, 2, 1000.00, 1, 1),
    (104, 3, 1800.00, 1, 1),
    (105, 1, 400.00, 1, 1),
    (105, 2, 700.00, 1, 1),
    (106, 2, 1100.00, 1, 1),
    (106, 3, 2200.00, 1, 1),
    (107, 1, 300.00, 1, 1),
    (107, 2, 500.00, 1, 1),
    (107, 4, 150.00, 10, 1),
    (108, 2, 900.00, 1, 1),
    (108, 3, 1500.00, 1, 1),
    (109, 1, 350.00, 1, 1),
    (109, 2, 600.00, 1, 1),
    (109, 4, 200.00, 6, 1),
    (110, 2, 1500.00, 1, 1),
    (110, 3, 2500.00, 1, 1),
    (111, 2, 1000.00, 1, 1),
    (111, 3, 1700.00, 1, 1),
    (112, 1, 450.00, 1, 1),
    (112, 2, 750.00, 1, 1),
    (113, 1, 400.00, 1, 1),
    (113, 2, 650.00, 1, 1),
    (114, 2, 1300.00, 1, 1),
    (114, 3, 2100.00, 1, 1),
    (115, 2, 1400.00, 1, 1),
    (115, 3, 2400.00, 1, 1);

-- Weekly availability (weekday: Monday=0 ... Sunday=6, times in Asia/Kolkata)
INSERT IGNORE INTO availability_patterns (availability_pattern_id, doctor_id, consultation_type_id, weekday, start_time, end_time, timezone_name, effective_from, effective_to, is_active) VALUES
    (1, 101, 2, 0, '09:00:00', '12:00:00', 'Asia/Kolkata', '2026-01-01', NULL, 1),
    (2, 101, 2, 2, '09:00:00', '12:00:00', 'Asia/Kolkata', '2026-01-01', NULL, 1),
    (3, 101, 3, 4, '14:00:00', '17:00:00', 'Asia/Kolkata', '2026-01-01', NULL, 1),
    (4, 102, 1, 0, '09:00:00', '10:00:00', 'Asia/Kolkata', '2026-01-01', NULL, 1),
    (5, 102, 2, 2, '09:00:00', '10:00:00', 'Asia/Kolkata', '2026-01-01', NULL, 1),
    (6, 102, 4, 5, '10:00:00', '12:00:00', 'Asia/Kolkata', '2026-01-01', NULL, 1),
    (7, 103, 2, 1, '10:00:00', '13:00:00', 'Asia/Kolkata', '2026-01-01', NULL, 1),
    (8, 103, 2, 3, '10:00:00', '13:00:00', 'Asia/Kolkata', '2026-01-01', NULL, 1),
    (9, 103, 1, 5, '10:00:00', '11:00:00', 'Asia/Kolkata', '2026-01-01', NULL, 1),
    (10, 104, 2, 0, '15:00:00', '18:00:00', 'Asia/Kolkata', '2026-01-01', NULL, 1),
    (11, 104, 3, 3, '15:00:00', '17:00:00', 'Asia/Kolkata', '2026-01-01', NULL, 1),
    (12, 105, 1, 1, '09:00:00', '11:00:00', 'Asia/Kolkata', '2026-01-01', NULL, 1),
    (13, 105, 2, 4, '09:00:00', '12:00:00', 'Asia/Kolkata', '2026-01-01', NULL, 1),
    (14, 105, 1, 5, '09:00:00', '11:00:00', 'Asia/Kolkata', '2026-01-01', NULL, 1),
    (15, 106, 3, 0, '10:00:00', '12:00:00', 'Asia/Kolkata', '2026-01-01', NULL, 1),
    (16, 106, 3, 2, '10:00:00', '12:00:00', 'Asia/Kolkata', '2026-01-01', NULL, 1),
    (17, 106, 2, 4, '16:00:00', '18:00:00', 'Asia/Kolkata', '2026-01-01', NULL, 1),
    (18, 107, 1, 0, '09:00:00', '11:00:00', 'Asia/Kolkata', '2026-01-01', NULL, 1),
    (19, 107, 1, 1, '09:00:00', '11:00:00', 'Asia/Kolkata', '2026-01-01', NULL, 1),
    (20, 107, 1, 2, '09:00:00', '11:00:00', 'Asia/Kolkata', '2026-01-01', NULL, 1),
    (21, 107, 1, 3, '09:00:00', '11:00:00', 'Asia/Kolkata', '2026-01-01', NULL, 1),
    (22, 107, 1, 4, '09:00:00', '11:00:00', 'Asia/Kolkata', '2026-01-01', NULL, 1),
    (23, 107, 4, 6, '10:00:00', '12:00:00', 'Asia/Kolkata', '2026-01-01', NULL, 1),
    (24, 108, 2, 1, '11:00:00', '14:00:00', 'Asia/Kolkata', '2026-01-01', NULL, 1),
    (25, 108, 2, 3, '11:00:00', '14:00:00', 'Asia/Kolkata', '2026-01-01', NULL, 1),
    (26, 109, 2, 0, '10:00:00', '12:00:00', 'Asia/Kolkata', '2026-01-01', NULL, 1),
    (27, 109, 2, 2, '10:00:00', '12:00:00', 'Asia/Kolkata', '2026-01-01', NULL, 1),
    (28, 109, 1, 5, '09:00:00', '11:00:00', 'Asia/Kolkata', '2026-01-01', NULL, 1),
    (29, 109, 4, 6, '11:00:00', '13:00:00', 'Asia/Kolkata', '2026-01-01', NULL, 1),
    (30, 110, 2, 1, '14:00:00', '17:00:00', 'Asia/Kolkata', '2026-01-01', NULL, 1),
    (31, 110, 3, 4, '14:00:00', '16:00:00', 'Asia/Kolkata', '2026-01-01', NULL, 1),
    (32, 111, 2, 0, '11:00:00', '13:00:00', 'Asia/Kolkata', '2026-01-01', NULL, 1),
    (33, 111, 2, 3, '11:00:00', '13:00:00', 'Asia/Kolkata', '2026-01-01', NULL, 1),
    (34, 111, 3, 5, '11:00:00', '13:00:00', 'Asia/Kolkata', '2026-01-01', NULL, 1),
    (35, 112, 1, 2, '09:00:00', '11:00:00', 'Asia/Kolkata', '2026-01-01', NULL, 1),
    (36, 112, 2, 4, '09:00:00', '11:00:00', 'Asia/Kolkata', '2026-01-01', NULL, 1),
    (37, 113, 2, 1, '09:00:00', '12:00:00', 'Asia/Kolkata', '2026-01-01', NULL, 1),
    (38, 113, 2, 3, '09:00:00', '12:00:00', 'Asia/Kolkata', '2026-01-01', NULL, 1),
    (39, 113, 1, 5, '09:00:00', '10:00:00', 'Asia/Kolkata', '2026-01-01', NULL, 1),
    (40, 114, 2, 0, '16:00:00', '18:00:00', 'Asia/Kolkata', '2026-01-01', NULL, 1),
    (41, 114, 2, 2, '16:00:00', '18:00:00', 'Asia/Kolkata', '2026-01-01', NULL, 1),
    (42, 115, 3, 1, '10:00:00', '13:00:00', 'Asia/Kolkata', '2026-01-01', NULL, 1),
    (43, 115, 2, 3, '10:00:00', '12:00:00', 'Asia/Kolkata', '2026-01-01', NULL, 1),
    (44, 115, 3, 6, '10:00:00', '12:00:00', 'Asia/Kolkata', '2026-01-01', NULL, 1);

-- Days off
INSERT IGNORE INTO doctor_blackouts (blackout_id, doctor_id, blackout_date, reason) VALUES
    (1, 101, '2026-11-25', 'Medical conference'),
    (2, 109, '2026-11-30', 'Personal leave'),
    (3, 107, '2026-12-25', 'Christmas Day');

-- ============================================================
-- END OF SAMPLE DATA
-- ============================================================
