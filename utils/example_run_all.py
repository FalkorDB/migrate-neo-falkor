import sys
import os
from dotenv import load_dotenv
from falkordb import FalkorDB
from neo4j import GraphDatabase

load_dotenv()
FALKOR_DB_HOST = os.getenv("FALKOR_DB_HOST", "localhost")
FALKOR_DB_PORT = os.getenv("FALKOR_DB_PORT", "6379")
FALKOR_DB_GRAPH_NAME = os.getenv("FALKOR_DB_GRAPH_NAME", "SocialGraph")

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_CREDS_USERNAME = os.getenv("NEO4J_CREDS_USERNAME", "neo4j")
NEO4J_CREDS_PASSWORD = os.getenv("NEO4J_CREDS_PASSWORD", "test1234")
NEO4J_DATA_FOLDER = os.getenv("NEO4J_DATA_FOLDER", "data/neo4j_data")


# Sanity check on the Neo grpah after creation
def check_neo4j_node_count():
    driver = GraphDatabase.driver(
        NEO4J_URI, auth=(NEO4J_CREDS_USERNAME, NEO4J_CREDS_PASSWORD)
    )
    with driver.session() as session:
        count = session.run("MATCH (n) RETURN count(n)").single()[0]
        print(f"Neo4j node count: {count}")
        if count == 0:
            raise ValueError("Neo4j sanity check failed: no nodes")
    driver.close()


# Sanity check on the Neo data after exporting
def check_export_output():
    import os

    expected_files = [
        "users.csv",
        "posts.csv",
        "friends_with.csv",
        "created.csv",
        "constraints.csv",
    ]
    missing = [
        f for f in expected_files if not os.path.exists(f"{NEO4J_DATA_FOLDER}/{f}")
    ]
    if missing:
        raise ValueError(f"Export check failed: missing files {missing}")
    print("👍 Export output verified: All expected CSV files are present.")


# Sanity check on the falkor grpah after creation
def check_falkordb_graph_created():
    client = FalkorDB(host=FALKOR_DB_HOST, port=FALKOR_DB_PORT)
    graph = client.select_graph(FALKOR_DB_GRAPH_NAME)
    result = graph.query("MATCH (n) RETURN count(n)")
    count = result.result_set[0][0]
    if count < 1:
        raise ValueError("Falkordb graph creation check failed: no nodes found.")
    print(f"👍 Falkordb graph creation verified: {count} nodes found.")


# Helper to confirm continuation
def confirm_or_exit():
    proceed = (
        input("Continue to next stage? either press Enter or [y/N]: ").strip().lower()
    )
    if proceed not in ("", "y", "yes"):
        print("Aborting pipeline.")
        sys.exit(0)


# Run a script stage with optional check, with error handling and environment reset
def run_stage(name, func, check=None):
    print(f"\n--- Running {name} ---")
    try:
        func()
        print(f"✅ Stage '{name}' completed.")
        confirm_or_exit()
        if check:
            check()
            confirm_or_exit()
    except Exception as e:
        print(f"❌ Error during stage '{name}': {e}")
        print("⚠️  Running reset_environment to clean up...")
        reset_environment()
        print("Environment reset. Exiting.")
        sys.exit(1)


def main():
    # === RUN MIGRATION STEPS ===
    STAGES = {
        "Stage - Reset Environment": (reset_environment, None),
        "Stage - Create Neo4j Graph": (create_neo4j_graph, check_neo4j_node_count),
        "Stage - Export from Neo4j": (export_data_from_neo4j, check_export_output),
        "Stage - Create Falkordb Graph": (
            create_falkordb_graph,
            check_falkordb_graph_created,
        ),
        "Stage - Compare Graphs": (compare_graphs, None),
        "Stage - Clean Falkordb Graph": (clean_falkordb, None),
    }

    proceed = (
        input(
            "⚠️  This will reset the environment (delete data on both graphs including constraints and will empty the exported data folder). Continue? [y/N]: "
        )
        .strip()
        .lower()
    )
    if proceed not in ("y", "yes"):
        print("Aborting pipeline before reset.")
        sys.exit(0)

    for stage_name, (func, check) in STAGES.items():
        run_stage(stage_name, func, check)

    print("\n✅✅ Migration pipeline completed successfully")


if __name__ == "__main__":
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
    from utils.reset_graphs_and_exported_data import main as reset_environment
    from utils.create_neo4j_graph import main as create_neo4j_graph
    from migrate.export_from_neo4j import main as export_data_from_neo4j
    from migrate.create_falkordb_graph import main as create_falkordb_graph
    from migrate.compare_graphs import main as compare_graphs
    from migrate.clean import main as clean_falkordb

    main()
