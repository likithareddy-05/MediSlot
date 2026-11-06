
import json
import logging
import os
from pathlib import Path

import boto3
import pymysql

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ssm = boto3.client("ssm")
secrets = boto3.client("secretsmanager")


# Additive, idempotent changes for databases created before a milestone.
# CREATE TABLE IF NOT EXISTS never alters an existing table, so columns and
# indexes added later are applied here only when they are missing. Fresh
# databases already get them from schema.sql, so every step is a no-op there.
ADDITIVE_COLUMNS = [
    # (table, column, DDL fragment)
    ("patients", "city", "ADD COLUMN city VARCHAR(100) NULL AFTER address"),
    ("patients", "emergency_contact_name", "ADD COLUMN emergency_contact_name VARCHAR(100) NULL AFTER city"),
    ("patients", "emergency_contact_phone", "ADD COLUMN emergency_contact_phone VARCHAR(16) NULL AFTER emergency_contact_name"),
]
ADDITIVE_INDEXES = [
    # (table, index, DDL fragment)
    ("hospitals", "idx_hospitals_city", "ADD KEY idx_hospitals_city (city, hospital_name)"),
    ("doctors", "idx_doctors_first_name", "ADD KEY idx_doctors_first_name (first_name)"),
    ("doctors", "idx_doctors_spec_hospital", "ADD KEY idx_doctors_spec_hospital (specialization_id, hospital_id)"),
]


def apply_additive_migrations(connection, database):
    """Add the Milestone 4 columns and indexes to an older database. Returns what was applied."""
    applied = []
    with connection.cursor() as cursor:
        for table, column, ddl in ADDITIVE_COLUMNS:
            cursor.execute(
                """
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s AND column_name = %s
                """,
                (database, table, column),
            )
            if not cursor.fetchone():
                cursor.execute(f"ALTER TABLE {table} {ddl}")
                applied.append(f"{table}.{column}")
        for table, index, ddl in ADDITIVE_INDEXES:
            cursor.execute(
                """
                SELECT 1 FROM information_schema.statistics
                WHERE table_schema = %s AND table_name = %s AND index_name = %s
                """,
                (database, table, index),
            )
            if not cursor.fetchone():
                cursor.execute(f"ALTER TABLE {table} {ddl}")
                applied.append(f"{table}.{index}")
    return applied


def lambda_handler(event, context):
    if event.get("confirm") != "INITIALIZE_MEDISLOT_SCHEMA":
        return {
            "statusCode": 400,
            "body": json.dumps({
                "message": "Explicit confirmation is required."
            }),
        }

    environment = os.environ["ENVIRONMENT_NAME"]

    def get_parameter(suffix):
        name = f"/medislot/{environment}/database/{suffix}"
        return ssm.get_parameter(Name=name)["Parameter"]["Value"]

    host = get_parameter("host")
    port = int(get_parameter("port"))
    database = get_parameter("name")

    secret_arn = os.environ["DB_SECRET_ARN"]
    secret_response = secrets.get_secret_value(SecretId=secret_arn)
    credentials = json.loads(secret_response["SecretString"])

    schema_path = Path(__file__).with_name("schema.sql")
    schema = schema_path.read_text(encoding="utf-8")

    # Remove full-line SQL comments from the supplied schema.
    schema_lines = [
        line for line in schema.splitlines()
        if not line.strip().startswith("--")
    ]
    schema = "\n".join(schema_lines)

    statements = [
        statement.strip()
        for statement in schema.split(";")
        if statement.strip()
    ]

    connection = None

    try:
        connection = pymysql.connect(
            host=host,
            port=port,
            user=credentials["username"],
            password=credentials["password"],
            charset="utf8mb4",
            connect_timeout=10,
            read_timeout=30,
            write_timeout=30,
            autocommit=True,
        )

        # schema.sql is the only definition of the database. CREATE TABLE IF NOT EXISTS
        # never alters a table that already exists, so a database still on an older
        # layout must be reset (see README, "Resetting a dev database") instead of
        # having the new schema run on top of it and fail half-way.
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = %s
                  AND table_name = 'consultation_types'
                  AND column_name = 'doctor_id'
                """,
                (database,),
            )
            if cursor.fetchone():
                raise RuntimeError(
                    "The database still has the old consultation_types layout. "
                    "Drop the MediSlot tables and re-run (README: Resetting a dev database)."
                )

        # doctors / patients used to have their own doctor_id / patient_id key. The user_id
        # is now the key; CREATE TABLE IF NOT EXISTS would silently keep the old tables.
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = %s
                  AND ((table_name = 'doctors' AND column_name = 'doctor_id')
                    OR (table_name = 'patients' AND column_name = 'patient_id'))
                """,
                (database,),
            )
            if cursor.fetchone():
                raise RuntimeError(
                    "doctors / patients still have their own doctor_id / patient_id key. "
                    "Drop the MediSlot tables and re-run (README: Resetting a dev database)."
                )

        # An existing doctors table without specialization_id / hospital_id / photo_key
        # would be left untouched by CREATE TABLE IF NOT EXISTS: stop with a clear message.
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                  (SELECT COUNT(*) FROM information_schema.tables
                    WHERE table_schema = %s AND table_name = 'doctors') AS has_table,
                  (SELECT COUNT(*) FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = 'doctors'
                      AND column_name IN ('specialization_id', 'hospital_id', 'photo_key')) AS new_columns
                """,
                (database, database),
            )
            has_table, new_columns = cursor.fetchone()
            if has_table and new_columns != 3:
                raise RuntimeError(
                    "The existing doctors table is missing specialization_id, hospital_id or "
                    "photo_key. Drop the MediSlot tables and re-run (README: Resetting a dev database)."
                )

        with connection.cursor() as cursor:
            for index, statement in enumerate(statements, start=1):
                logger.info("Executing schema statement %s", index)
                cursor.execute(statement)

        migrated = apply_additive_migrations(connection, database)
        if migrated:
            logger.info("Applied additive migrations: %s", migrated)

        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT TABLE_NAME
                FROM information_schema.tables
                WHERE TABLE_SCHEMA = %s
                  AND TABLE_NAME IN ('users', 'patients', 'hospitals', 'specializations', 'doctors', 'consultation_types', 'doctor_consultation_types', 'availability_patterns', 'doctor_blackouts', 'appointments')
                ORDER BY TABLE_NAME
                """,
                (database,),
            )
            tables = [row[0] for row in cursor.fetchall()]

        expected = {"users", "patients", "hospitals", "specializations", "doctors", "consultation_types", "doctor_consultation_types", "availability_patterns", "doctor_blackouts", "appointments"}
        missing = sorted(expected - set(tables))

        if missing:
            raise RuntimeError(
                f"Schema verification failed. Missing tables: {missing}"
            )

        with connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM consultation_types")
            catalog_rows = cursor.fetchone()[0]
        if catalog_rows != 4:
            raise RuntimeError(
                f"consultation_types must hold exactly the 4 approved types, found {catalog_rows}"
            )

        logger.info("Schema initialized and verified: %s", tables)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Schema initialized and verified.",
                "database": database,
                "tables": tables,
            }),
        }

    except Exception:
        logger.exception("Schema initialization failed")
        raise

    finally:
        if connection is not None:
            connection.close()
