import os
import pandas as pd
from dotenv import load_dotenv
from neo4j import GraphDatabase
from falkordb import FalkorDB

load_dotenv()
FALKOR_DB_HOST = os.getenv("FALKOR_DB_HOST", "localhost")
FALKOR_DB_PORT = os.getenv("FALKOR_DB_PORT", "6379")
FALKOR_DB_GRAPH_NAME = os.getenv("FALKOR_DB_GRAPH_NAME", "SocialGraph")

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_CREDS_USERNAME = os.getenv("NEO4J_CREDS_USERNAME", "neo4j")
NEO4J_CREDS_PASSWORD = os.getenv("NEO4J_CREDS_PASSWORD", "test1234")


# Queries to compare
comparison_queries = {
    "node_count": "MATCH (n) RETURN count(n) AS count",
    "rel_count": "MATCH ()-[r]->() RETURN count(r) AS count",
    "user_sample": "MATCH (u:User) RETURN u.name, u.age, u.city, u.email ORDER BY u.name",
    "post_sample": "MATCH (p:Post) RETURN p.name, p.likes, p.category, p.image_url ORDER BY p.name",
    "created_rels": {
        "neo4j": (
            "MATCH (u:User)-[r:CREATED]->(p:Post) "
            "RETURN elementId(r), datetime(r.timestamp).epochMillis ORDER BY elementId(r)"
        ),
        "falkordb": (
            "MATCH (u:User)-[r:CREATED]->(p:Post) "
            "RETURN r.element_id, r.timestamp ORDER BY r.element_id"
        ),
    },
    "friends_with_rels": {
        "neo4j": (
            "MATCH (u1:User)-[r:FRIENDS_WITH]->(u2:User) "
            "RETURN elementId(r), datetime({date: r.since}).epochMillis ORDER BY elementId(r)"
        ),
        "falkordb": (
            "MATCH (u1:User)-[r:FRIENDS_WITH]->(u2:User) "
            "RETURN r.element_id, toInteger(r.since) ORDER BY r.element_id"
        ),
    },
}


def query_neo4j(query):
    driver = GraphDatabase.driver(
        NEO4J_URI, auth=(NEO4J_CREDS_USERNAME, NEO4J_CREDS_PASSWORD)
    )
    with driver.session() as session:
        result = session.run(query)
        return [record.data() for record in result]


def query_falkordb(query):
    client = FalkorDB(host=FALKOR_DB_HOST, port=FALKOR_DB_PORT)
    graph = client.select_graph(FALKOR_DB_GRAPH_NAME)
    result = graph.query(query)
    return result.result_set


def compare_results(name, neo4j_result, falkordb_result):
    def normalize_neo(rows):
        normalized = []
        for row in rows:
            values = list(row.values())
            for i, v in enumerate(values):
                if isinstance(v, pd.Timestamp) or isinstance(v, str) and "T" in v:
                    try:
                        values[i] = int(
                            pd.to_datetime(v, errors="coerce").timestamp() * 1_000_000
                        )
                    except Exception:
                        pass
                elif pd.isna(v):
                    values[i] = None
            normalized.append(values)
        return sorted(normalized)

    def normalize_falkor(rows):
        return sorted(rows)

    neo4j_norm = normalize_neo(neo4j_result)
    falkordb_norm = normalize_falkor(falkordb_result)

    match = neo4j_norm == falkordb_norm
    print(f"\n--- Comparing {name} ---")
    print("Neo4j:", neo4j_norm)
    print("FalkorDB:", falkordb_norm)
    emoji = "✅" if match else "❌"
    print(f"{emoji} {name}: {len(neo4j_norm)} Neo4j vs {len(falkordb_norm)} FalkorDB")


def main():
    for name, query in comparison_queries.items():
        print("name:")
        print(name)
        neo4j_query = query
        falkordb_query = query
        if name in ["created_rels", "friends_with_rels"]:
            neo4j_query = query["neo4j"]
            falkordb_query = query["falkordb"]
        print("querying neo4j...")
        neo4j_result = query_neo4j(neo4j_query)
        print("querying falkordb...")
        falkordb_result = query_falkordb(falkordb_query)
        compare_results(name, neo4j_result, falkordb_result)


if __name__ == "__main__":
    main()
