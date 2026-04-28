from neo4j import GraphDatabase
from app.config import settings

driver = GraphDatabase.driver(
    settings.neo4j_uri,
    auth=(settings.neo4j_username, settings.neo4j_password)
)


def get_neo4j():
    with driver.session() as session:
        yield session


def close_driver():
    driver.close()
