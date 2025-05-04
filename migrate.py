import sys
import os
from dotenv import load_dotenv
from falkordb import FalkorDB
from utils.reset_graphs_and_exported_data import main as reset_environment
from migrate.export_from_neo4j import main as export_data_from_neo4j
from migrate.create_falkordb_graph import main as create_falkordb_graph
from migrate.compare_graphs import main as compare_graphs
from migrate.clean import main as clean_falkordb

load_dotenv()
FALKOR_DB_HOST = os.getenv("FALKOR_DB_HOST", "localhost")
FALKOR_DB_PORT = os.getenv("FALKOR_DB_PORT", "6379")
FALKOR_DB_GRAPH_NAME = os.getenv("FALKOR_DB_GRAPH_NAME", "SocialGraph")
NEO4J_DATA_FOLDER = os.getenv("NEO4J_DATA_FOLDER", "data/neo4j_data")


EXPECTED_FILES = ["users.csv", "posts.csv", "friends_with.csv", "created.csv", "constraints.csv"]


# Sanity check on the exported csv files in data/neo4j_data
def check_export_output():
    missing = [f for f in EXPECTED_FILES if not os.path.exists(f"{NEO4J_DATA_FOLDER}/{f}")]
    if missing:
        raise ValueError(f"Export check failed: missing files {missing}")
    print("Exported files present.")


# Sanity check on the Falkor grpah after creation
def check_falkor_graph_created():
    client = FalkorDB(host=FALKOR_DB_HOST, port=FALKOR_DB_PORT)
    graph = client.select_graph(FALKOR_DB_GRAPH_NAME)
    result = graph.query("MATCH (n) RETURN count(n)")
    count = result.result_set[0][0]
    if count < 1:
        raise ValueError("Falkordb graph creation check failed: no nodes found.")
    print(f"Falkordb graph node count: {count}")


# Helper to confirm continuation
def confirm_or_exit():
    proceed = input("Continue to next stage? [Y/n]: ").strip().lower()
    if proceed not in ("", "y", "yes"):
        print("Aborting pipeline.")
        sys.exit(0)


# Run a script stage with optional check, with error handling and environment reset
def run_stage(name, func, check=None):
    print(f"\n--- Running {name} ---")
    try:
        func()
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
        "Stage - Export from Neo4j": (export_data_from_neo4j, check_export_output),
        "Stage - Create Falkor Graph": (create_falkordb_graph, check_falkor_graph_created),
        "Stage - Compare Graphs": (compare_graphs, None),
        "Stage - Clean Falkor Graph": (clean_falkordb, None)
    }

    for stage_name, (func, check) in STAGES.items():
        run_stage(stage_name, func, check)
    print("\n ✅✅ Migration pipeline completed successfully")


if __name__ == "__main__":
    main()
