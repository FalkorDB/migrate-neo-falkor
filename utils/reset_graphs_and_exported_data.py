import os
import glob
from neo4j import GraphDatabase


def main():
    # === 1. Clear neo_data/ folder ===
    neo_data_dir = "data/neo_data"
    csv_files = glob.glob(os.path.join(neo_data_dir, "*.csv"))
    print(f"Found {len(csv_files)} CSV files in '{neo_data_dir}' to delete.")
    for file in csv_files:
        print(f"Deleting file: {file}")
        os.remove(file)

    # === 2. Reset Neo4j graph ===
    neo4j_driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "test1234"))
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
    from falkordb import FalkorDB

    falkor_client = FalkorDB(host="localhost", port=6379)
    graph = falkor_client.select_graph("SocialGraph")

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
