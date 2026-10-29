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