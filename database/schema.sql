
-- ============================================================
-- MediSlot: Initial Authentication Database Schema
-- Milestone 2 - Foundation and Authentication
-- Database: Amazon RDS for MySQL
-- ============================================================

CREATE DATABASE IF NOT EXISTS medislot;

USE medislot;

-- ------------------------------------------------------------
-- USERS TABLE
-- Stores shared authentication and account information
-- for both doctors and patients.
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS users (
    user_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,

    email VARCHAR(254) NOT NULL,

    password_hash VARCHAR(255) NOT NULL,

    role VARCHAR(10) NOT NULL,

    account_status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',

    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),

    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
        ON UPDATE CURRENT_TIMESTAMP(6),

    -- Primary key
    PRIMARY KEY (user_id),

    -- Email must be unique across doctors and patients
    UNIQUE KEY uq_users_email (email),

    -- Supports administrative role/status queries
    KEY idx_users_role_status (role, account_status),

    -- Only the two defined account roles are permitted
    CONSTRAINT chk_users_role
        CHECK (role IN ('DOCTOR', 'PATIENT'))

) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_0900_ai_ci;
