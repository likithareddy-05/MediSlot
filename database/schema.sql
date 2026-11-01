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
    patient_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,

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

    PRIMARY KEY (patient_id),

    -- One patient profile per account
    UNIQUE KEY uq_patients_user_id (user_id),

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
-- DOCTORS TABLE
-- Stores personal and professional doctor information.
-- ============================================================

CREATE TABLE IF NOT EXISTS doctors (
    doctor_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,

    user_id BIGINT UNSIGNED NOT NULL,

    first_name VARCHAR(50) NOT NULL,

    last_name VARCHAR(50) NOT NULL,

    phone VARCHAR(16) NOT NULL,

    date_of_birth DATE NOT NULL,

    gender VARCHAR(30) NOT NULL,

    address VARCHAR(500) NOT NULL,

    specialization VARCHAR(100) NOT NULL,

    license_number VARCHAR(8) NOT NULL,

    years_of_experience TINYINT UNSIGNED NOT NULL,

    clinic_address VARCHAR(500) NOT NULL,

    created_at DATETIME(6) NOT NULL
        DEFAULT CURRENT_TIMESTAMP(6),

    updated_at DATETIME(6) NOT NULL
        DEFAULT CURRENT_TIMESTAMP(6)
        ON UPDATE CURRENT_TIMESTAMP(6),

    PRIMARY KEY (doctor_id),

    -- One doctor profile per account
    UNIQUE KEY uq_doctors_user_id (user_id),

    -- Every doctor must have a unique license number
    UNIQUE KEY uq_doctors_license_number (license_number),

    KEY idx_doctors_specialization (specialization),

    KEY idx_doctors_name (last_name, first_name),

    KEY idx_doctors_phone (phone),

    CONSTRAINT fk_doctors_user
        FOREIGN KEY (user_id)
        REFERENCES users (user_id)
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

CREATE TABLE IF NOT EXISTS doctor_profile_assets (
    doctor_id BIGINT UNSIGNED NOT NULL,
    photo_key VARCHAR(512) NOT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (doctor_id),
    CONSTRAINT fk_doctor_profile_assets_doctor FOREIGN KEY (doctor_id)
        REFERENCES doctors (doctor_id) ON UPDATE RESTRICT ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS consultation_types (
    consultation_type_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    doctor_id BIGINT UNSIGNED NOT NULL,
    consultation_name VARCHAR(100) NOT NULL,
    duration_minutes SMALLINT UNSIGNED NOT NULL,
    fee DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    slot_capacity SMALLINT UNSIGNED NOT NULL DEFAULT 1,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (consultation_type_id),
    UNIQUE KEY uq_consultation_doctor_name (doctor_id, consultation_name),
    KEY idx_consultation_doctor_active (doctor_id, is_active),
    CONSTRAINT fk_consultation_types_doctor FOREIGN KEY (doctor_id)
        REFERENCES doctors (doctor_id) ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT chk_consultation_duration CHECK (duration_minutes BETWEEN 5 AND 240),
    CONSTRAINT chk_consultation_fee CHECK (fee >= 0),
    CONSTRAINT chk_consultation_capacity CHECK (slot_capacity BETWEEN 1 AND 50)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

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
    CONSTRAINT fk_availability_doctor FOREIGN KEY (doctor_id)
        REFERENCES doctors (doctor_id) ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT fk_availability_consultation FOREIGN KEY (consultation_type_id)
        REFERENCES consultation_types (consultation_type_id) ON UPDATE RESTRICT ON DELETE RESTRICT,
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
        REFERENCES doctors (doctor_id) ON UPDATE RESTRICT ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ============================================================
-- MILESTONE 3: APPOINTMENTS AND REFUND LEDGER (additive)
-- Minimal appointment shape used by the doctor day-view and outcome APIs.
-- The patient booking workflow populates these rows in a later milestone.
-- ============================================================

CREATE TABLE IF NOT EXISTS appointments (
    appointment_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
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
    CONSTRAINT fk_appointment_doctor FOREIGN KEY (doctor_id)
        REFERENCES doctors (doctor_id) ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT fk_appointment_patient FOREIGN KEY (patient_id)
        REFERENCES patients (patient_id) ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT fk_appointment_consultation FOREIGN KEY (consultation_type_id)
        REFERENCES consultation_types (consultation_type_id) ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT chk_appointment_status CHECK (status IN ('BOOKED','CANCELLED','COMPLETED','NO_SHOW')),
    CONSTRAINT chk_appointment_outcome CHECK (outcome IS NULL OR outcome IN ('COMPLETED','NO_SHOW'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS refund_ledger (
    refund_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    appointment_id BIGINT UNSIGNED NOT NULL,
    patient_id BIGINT UNSIGNED NOT NULL,
    doctor_id BIGINT UNSIGNED NOT NULL,
    refund_amount DECIMAL(10,2) NOT NULL,
    refund_status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    reason VARCHAR(255) NOT NULL DEFAULT 'DOCTOR_SLOT_CANCELLED',
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (refund_id),
    UNIQUE KEY uq_refund_appointment (appointment_id),
    CONSTRAINT fk_refund_appointment FOREIGN KEY (appointment_id)
        REFERENCES appointments (appointment_id) ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT fk_refund_patient FOREIGN KEY (patient_id)
        REFERENCES patients (patient_id) ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT fk_refund_doctor FOREIGN KEY (doctor_id)
        REFERENCES doctors (doctor_id) ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT chk_refund_amount CHECK (refund_amount >= 0),
    CONSTRAINT chk_refund_status CHECK (refund_status IN ('PENDING','PROCESSING','REFUNDED','FAILED'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
