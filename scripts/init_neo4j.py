"""
Initialises the Neo4J schema for Cosh 2.0.
Run once after Neo4J is running: python scripts/init_neo4j.py
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from neo4j import GraphDatabase
from app.config import settings


def init_schema():
    driver = GraphDatabase.driver(
        settings.neo4j_uri,
        auth=(settings.neo4j_username, settings.neo4j_password)
    )

    with driver.session() as session:
        print("Creating uniqueness constraint on CoreDataItem.id ...")
        session.run("""
            CREATE CONSTRAINT cdi_unique_id IF NOT EXISTS
            FOR (n:CoreDataItem) REQUIRE n.id IS UNIQUE
        """)

        print("Creating index on CoreDataItem.core_id ...")
        session.run("""
            CREATE INDEX cdi_core IF NOT EXISTS
            FOR (n:CoreDataItem) ON (n.core_id)
        """)

        print("Creating index on CoreDataItem.status ...")
        session.run("""
            CREATE INDEX cdi_status IF NOT EXISTS
            FOR (n:CoreDataItem) ON (n.status)
        """)

        print("Creating full-text index for similarity search ...")
        session.run("""
            CREATE FULLTEXT INDEX cdi_fulltext IF NOT EXISTS
            FOR (n:CoreDataItem) ON EACH [n.english_value]
        """)

        print("Verifying indexes ...")
        result = session.run("SHOW INDEXES")
        for record in result:
            print(f"  ✓ {record['name']} ({record['type']})")

    driver.close()
    print("\nNeo4J schema initialised successfully.")


if __name__ == "__main__":
    init_schema()
