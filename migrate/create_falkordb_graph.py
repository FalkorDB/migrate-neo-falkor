import csv
import os
from dotenv import load_dotenv
from falkordb import FalkorDB

load_dotenv()
FALKOR_DB_HOST = os.getenv("FALKOR_DB_HOST", "localhost")
FALKOR_DB_PORT = os.getenv("FALKOR_DB_PORT", "6379")
FALKOR_DB_IMPORT_DIR = os.getenv("FALKOR_DB_IMPORT_DIR", "file://")
FALKOR_DB_GRAPH_NAME = os.getenv("FALKOR_DB_GRAPH_NAME", "SocialGraph")
FALKOR_DB_DATA_FOLDER = os.getenv("FALKOR_DB_DATA_FOLDER", "import/neo4j_data/")


def create_constraints_from_csv(graph):
    constraints_path = os.path.abspath(
        os.path.join(
            os.path.dirname(__file__), "..", FALKOR_DB_DATA_FOLDER, "constraints.csv"
        )
    )
    with open(constraints_path, newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if row["type"] == "UNIQUENESS" and row["entityType"] == "NODE":
                labels = eval(row["labelsOrTypes"])
                props = eval(row["properties"])
                if labels and props:
                    label = labels[0]
                    prop = props[0]
                    print(f"Creating UNIQUE constraint on :{label}({prop})")
                    graph.create_node_unique_constraint(label, prop)


def load_csv_and_create(graph, filename, create_clause, label_desc):
    result = graph.query(
        f'LOAD CSV WITH HEADERS FROM "{FALKOR_DB_IMPORT_DIR}/{filename}" AS row '
        f"{create_clause}"
    )
    print(
        f"Created {result.nodes_created} nodes and {int(result.relationships_created)} relationships for {label_desc}"
    )


def main():
    client = FalkorDB(host=FALKOR_DB_HOST, port=FALKOR_DB_PORT)
    graph = client.select_graph(FALKOR_DB_GRAPH_NAME)

    load_csv_and_create(
        graph,
        "users.csv",
        "CREATE (:User {element_id: row.element_id, name: row.name, age: toInteger(row.age), city: row.city, email: row.email})",
        "Users",
    )
    load_csv_and_create(
        graph,
        "posts.csv",
        "CREATE (:Post {element_id: row.element_id, name: row.name, likes: toInteger(row.likes), category: row.category, image_url: row.image_url})",
        "Posts",
    )
    load_csv_and_create(
        graph,
        "friends_with.csv",
        "MATCH (u1:User {element_id: row.start_id}), (u2:User {element_id: row.end_id}) "
        "CREATE (u1)-[:FRIENDS_WITH {since: row.since, element_id: row.element_id}]->(u2)",
        "FRIENDS_WITH relationships",
    )
    load_csv_and_create(
        graph,
        "created.csv",
        "MATCH (u:User {element_id: row.start_id}), (p:Post {element_id: row.end_id}) "
        "CREATE (u)-[:CREATED {timestamp: toInteger(row.timestamp), element_id: row.element_id}]->(p)",
        "CREATED relationships",
    )

    create_constraints_from_csv(graph)


if __name__ == "__main__":
    main()
