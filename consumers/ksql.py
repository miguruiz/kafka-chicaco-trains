"""Configures KSQL to combine station and turnstile data"""
import json
import logging

import requests

import topic_check


logger = logging.getLogger(__name__)


KSQL_URL = "http://localhost:8088"#"http://ksql:8088"



KSQL_STATEMENT = """
CREATE TABLE TURNSTILE (
    station_id VARCHAR,
    station_name VARCHAR,
    line VARCHAR
) WITH (
    KAFKA_TOPIC='chicago.rail.all_station.turnstile',
    VALUE_FORMAT='AVRO',
    KEY='station_id'
);
"""

KSQL_STATEMENT_2 = """CREATE TABLE TURNSTILE_SUMMARY
WITH (VALUE_FORMAT='json') AS
SELECT station_id, COUNT(station_id) AS COUNT
FROM TURNSTILE
GROUP BY station_id;"""


def execute_statement(KSQL_STATEMENT = KSQL_STATEMENT):
    """Executes the KSQL statement against the KSQL API"""
    if topic_check.topic_exists("TURNSTILE_SUMMARY") is True:
        return

    logging.debug("executing ksql statement...")

    resp = requests.post(
        f"{KSQL_URL}/ksql",
        headers={"Content-Type": "application/vnd.ksql.v1+json"},
        data=json.dumps(
            {
                "ksql": KSQL_STATEMENT,
                "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"},
            }
        ),
    )

    # Ensure that a 2XX status code was returned
    print(resp.json())
    resp.raise_for_status()


if __name__ == "__main__":
    execute_statement(KSQL_STATEMENT)
    execute_statement(KSQL_STATEMENT_2)
