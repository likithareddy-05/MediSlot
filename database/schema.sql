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

    KEY idx_hospitals_active_name (is_active, hospital_name)

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
-- MILESTONE 3: APPOINTMENTS (additive)
-- Minimal appointment shape used by the doctor day-view and outcome APIs.
-- The patient booking workflow populates these rows in a later milestone.
-- ============================================================

CREATE TABLE IF NOT EXISTS appointments (
    appointment_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    -- doctor_id / patient_id hold the two people's users.user_id values
    doctor_id BIGINT UNSIGNED NOT NULL,
    patient_id BIGINT UNSIGNED NOT NULL,
    slot_id VARCHAR(128) NOT NULL,
    consultation_type_id BIGINT UNSIGNED NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'BOOKED',
    outcome VARCHAR(20) NULL,
    scheduled_start_utc DATETIME(6) NOT NULL,
    scheduled_end_utc DATETIME(6) NOT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (appointment_id),
    UNIQUE KEY uq_appointment_slot_patient (slot_id, patient_id),
    KEY idx_appointments_doctor_start (doctor_id, scheduled_start_utc),
    KEY idx_appointments_doctor_status (doctor_id, status),
    KEY idx_appointments_doctor_consultation (doctor_id, consultation_type_id),
    CONSTRAINT fk_appointment_doctor FOREIGN KEY (doctor_id)
        REFERENCES doctors (user_id) ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT fk_appointment_patient FOREIGN KEY (patient_id)
        REFERENCES patients (user_id) ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT fk_appointment_doctor_consultation FOREIGN KEY (doctor_id, consultation_type_id)
        REFERENCES doctor_consultation_types (doctor_id, consultation_type_id) ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT chk_appointment_status CHECK (status IN ('BOOKED','CANCELLED','COMPLETED','NO_SHOW')),
    CONSTRAINT chk_appointment_outcome CHECK (outcome IS NULL OR outcome IN ('COMPLETED','NO_SHOW'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
