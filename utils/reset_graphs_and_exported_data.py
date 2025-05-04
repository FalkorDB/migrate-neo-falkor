import os
import glob
from dotenv import load_dotenv
from neo4j import GraphDatabase
from falkordb import FalkorDB

load_dotenv()
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_CREDS_USERNAME = os.getenv("NEO4J_CREDS_USERNAME", "neo4j")
NEO4J_CREDS_PASSWORD = os.getenv("NEO4J_CREDS_PASSWORD", "test1234")
NEO4J_DATA_FOLDER = os.getenv("NEO4J_DATA_FOLDER", "data/neo4j_data/")
FALKOR_DB_HOST = os.getenv("FALKOR_DB_HOST", "localhost")
FALKOR_DB_PORT = os.getenv("FALKOR_DB_PORT", "6379")
FALKOR_DB_GRAPH_NAME = os.getenv("FALKOR_DB_GRAPH_NAME", "SocialGraph")


def main():
    # === 1. Clear neo4j_data/ folder ===
    csv_files = glob.glob(os.path.join(NEO4J_DATA_FOLDER, "*.csv"))
    print(f"Found {len(csv_files)} CSV files in '{NEO4J_DATA_FOLDER}' to delete.")
    for file in csv_files:
        print(f"Deleting file: {file}")
        os.remove(file)

    # === 2. Reset Neo4j graph ===
    neo4j_driver = GraphDatabase.driver(
        NEO4J_URI, auth=(NEO4J_CREDS_USERNAME, NEO4J_CREDS_PASSWORD)
    )
    with neo4j_driver.session() as session:
        print("Deleting all nodes and relationships from Neo4j...")
        session.run("MATCH (n) DETACH DELETE n")

        print("Dropping Neo4j constraints...")
        constraints = session.run("SHOW CONSTRAINTS")
        for record in constraints:
            print(f"Dropping constraint: {record['name']}")
            session.run(f"DROP CONSTRAINT {record['name']} IF EXISTS")
    neo4j_driver.close()

    # === 3. Reset Falkor graph ===
    falkordb_client = FalkorDB(host=FALKOR_DB_HOST, port=FALKOR_DB_PORT)
    graph = falkordb_client.select_graph(FALKOR_DB_GRAPH_NAME)

    print("Deleting all nodes and relationships from FalkorDB...")
    graph.query("MATCH (n) DETACH DELETE n")

    # Drop unique constraints
    for label in ["User", "Post"]:
        try:
            print(f"Dropping Falkor constraint on {label}(name)...")
            graph.drop_node_unique_constraint(label, *["name"])
        except Exception as e:
            if "no such constraint" not in str(e).lower():
                raise


if __name__ == "__main__":
    main()
