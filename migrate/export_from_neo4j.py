import csv
import os
import pandas as pd
import time
from dotenv import load_dotenv
from neo4j import GraphDatabase

load_dotenv()
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_CREDS_USERNAME = os.getenv("NEO4J_CREDS_USERNAME", "neo4j")
NEO4J_CREDS_PASSWORD = os.getenv("NEO4J_CREDS_PASSWORD", "test1234")
NEO4J_MOUNTED_DIR = os.getenv("NEO4J_MOUNTED_DIR", "/import/neo4j_data")
NEO4J_DATA_FOLDER = os.getenv("NEO4J_DATA_FOLDER", "data/neo4j_data")


def get_export_path():
    cwd = os.getcwd()
    default_path = os.path.join(cwd, NEO4J_DATA_FOLDER)
    response = (
        input(f"Use default local export path '{default_path}'? [Y/n]: ")
        .strip()
        .lower()
    )
    if response in ("", "y", "yes"):
        os.makedirs(default_path, exist_ok=True)
        return default_path

    s3 = input("Export to S3? [y/N]: ").strip().lower()
    if s3 in ("y", "yes"):
        raise NotImplementedError("S3 export is not yet implemented.")

    custom_path = input("Enter custom local export path: ").strip()
    os.makedirs(custom_path, exist_ok=True)
    return custom_path


def convert_created_timestamp_to_epoch(path):
    file_path = os.path.join(path, "created.csv")

    for _ in range(10):
        if os.path.exists(file_path):
            break
        time.sleep(0.5)
    else:
        raise FileNotFoundError(f"File not found after waiting: {file_path}")

    df = pd.read_csv(file_path)
    df["timestamp"] = (
        pd.to_datetime(df["timestamp"], errors="coerce").astype("int64") // 1_000_000
    )
    df.to_csv(file_path, index=False)


def convert_firends_with_since_to_epoch(path):
    file_path = os.path.join(path, "friends_with.csv")

    for _ in range(10):
        if os.path.exists(file_path):
            break
        time.sleep(0.5)
    else:
        raise FileNotFoundError(f"File not found after waiting: {file_path}")

    df = pd.read_csv(file_path)
    df["since"] = (
        pd.to_datetime(df["since"], errors="coerce").astype("int64") // 1_000_000
    )
    df.to_csv(file_path, index=False)


def get_neo4j_credentials():
    uri = input(f"Enter Neo4j URI (default: {NEO4J_URI}): ").strip() or NEO4J_URI
    user = (
        input(f"Enter Neo4j username (default: {NEO4J_CREDS_USERNAME}): ").strip()
        or NEO4J_CREDS_USERNAME
    )
    password = (
        input(f"Enter Neo4j password (default: {NEO4J_CREDS_PASSWORD}): ").strip()
        or NEO4J_CREDS_PASSWORD
    )
    return uri, user, password


def main():
    uri, user, password = get_neo4j_credentials()
    export_path = get_export_path()
    os.makedirs(export_path, exist_ok=True)

    driver = GraphDatabase.driver(uri, auth=(user, password))
    with driver.session() as session:
        # Nodes
        result = session.run(
            "CALL apoc.export.csv.query($query, $file, {})",
            {
                "query": "MATCH (u:User) RETURN elementId(u) AS element_id, u.name AS name, u.age AS age, u.city AS city, u.email AS email",
                "file": f"{NEO4J_MOUNTED_DIR}/users.csv",
            },
        )
        result.consume()
        print(f"[✓] Exported to: {NEO4J_DATA_FOLDER}/users.csv")
        result = session.run(
            "CALL apoc.export.csv.query($query, $file, {})",
            {
                "query": "MATCH (p:Post) RETURN elementId(p) AS element_id, p.name AS name, p.likes AS likes, p.category AS category, p.image_url AS image_url",
                "file": f"{NEO4J_MOUNTED_DIR}/posts.csv",
            },
        )
        result.consume()
        print(f"[✓] Exported to: {NEO4J_DATA_FOLDER}/posts.csv")

        # Rels
        result = session.run(
            "CALL apoc.export.csv.query($query, $file, {})",
            {
                "query": "MATCH (u1:User)-[r:FRIENDS_WITH]->(u2:User) RETURN elementId(r) AS element_id, elementId(u1) AS start_id, elementId(u2) AS end_id, r.since AS since",
                "file": f"{NEO4J_MOUNTED_DIR}/friends_with.csv",
            },
        )
        result.consume()
        print(f"[✓] Exported to: {NEO4J_DATA_FOLDER}/friends_with.csv")

        result = session.run(
            "CALL apoc.export.csv.query($query, $file, {})",
            {
                "query": "MATCH (u:User)-[r:CREATED]->(p:Post) RETURN elementId(r) AS element_id, elementId(u) AS start_id, elementId(p) AS end_id, r.timestamp AS timestamp",
                "file": f"{NEO4J_MOUNTED_DIR}/created.csv",
            },
        )
        result.consume()
        print(f"[✓] Exported to: {NEO4J_DATA_FOLDER}/created.csv")

        # Transformations
        convert_created_timestamp_to_epoch(export_path)
        convert_firends_with_since_to_epoch(export_path)

        # Constraints
        constraints_result = session.run("SHOW CONSTRAINTS")
        headers = [key for key in constraints_result.keys()]
        rows = [record.values() for record in constraints_result]

        constraints_path = os.path.join(export_path, "constraints.csv")
        with open(constraints_path, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(headers)
            writer.writerows(rows)

    print(f"[✓] Export complete. Files written to: {export_path}")


if __name__ == "__main__":
    main()
