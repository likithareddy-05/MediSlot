
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

        with connection.cursor() as cursor:
            for index, statement in enumerate(statements, start=1):
                logger.info("Executing schema statement %s", index)
                cursor.execute(statement)

        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT TABLE_NAME
                FROM information_schema.tables
                WHERE TABLE_SCHEMA = %s
                  AND TABLE_NAME IN ('users', 'patients', 'doctors')
                ORDER BY TABLE_NAME
                """,
                (database,),
            )
            tables = [row[0] for row in cursor.fetchall()]

        expected = {"users", "patients", "doctors"}
        missing = sorted(expected - set(tables))

        if missing:
            raise RuntimeError(
                f"Schema verification failed. Missing tables: {missing}"
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
