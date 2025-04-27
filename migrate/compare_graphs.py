from neo4j import GraphDatabase
from falkordb import FalkorDB
import pandas as pd

# Neo4j settings
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASS = "test1234"

# FalkorDB settings
FALKOR_HOST = "localhost"
FALKOR_PORT = 6379
FALKOR_GRAPH = "SocialGraph"

# Queries to compare
comparison_queries = {
    "node_count": "MATCH (n) RETURN count(n) AS count",
    "rel_count": "MATCH ()-[r]->() RETURN count(r) AS count",
    "user_sample": "MATCH (u:User) RETURN u.name, u.age, u.city, u.email ORDER BY u.name",
    "post_sample": "MATCH (p:Post) RETURN p.name, p.likes, p.category, p.image_url ORDER BY p.name",
    "created_rels": {
        "neo": (
            "MATCH (u:User)-[r:CREATED]->(p:Post) "
            "RETURN elementId(r), datetime(r.timestamp).epochMillis ORDER BY elementId(r)"
            ),
        "falkor": ("MATCH (u:User)-[r:CREATED]->(p:Post) "
            "RETURN r.element_id, r.timestamp ORDER BY r.element_id")
    },
    "friends_with_rels": {
        "neo": (
            "MATCH (u1:User)-[r:FRIENDS_WITH]->(u2:User) "
            "RETURN elementId(r), datetime({date: r.since}).epochMillis ORDER BY elementId(r)"
            ),
        "falkor": (
            "MATCH (u1:User)-[r:FRIENDS_WITH]->(u2:User) "
            "RETURN r.element_id, toInteger(r.since) ORDER BY r.element_id"
            )
    }
}

def query_neo4j(query):
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
    with driver.session() as session:
        result = session.run(query)
        return [record.data() for record in result]

def query_falkor(query):
    client = FalkorDB(host=FALKOR_HOST, port=FALKOR_PORT)
    graph = client.select_graph(FALKOR_GRAPH)
    result = graph.query(query)
    return result.result_set

def compare_results(name, neo_result, falkor_result):
    def normalize_neo(rows):
        normalized = []
        for row in rows:
            values = list(row.values())
            for i, v in enumerate(values):
                if isinstance(v, pd.Timestamp) or isinstance(v, str) and "T" in v:
                    try:
                        values[i] = int(pd.to_datetime(v, errors="coerce").timestamp() * 1_000_000)
                    except Exception:
                        pass
                elif pd.isna(v):
                    values[i] = None
            normalized.append(values)
        return sorted(normalized)

    def normalize_falkor(rows):
        return sorted(rows)

    neo_norm = normalize_neo(neo_result)
    falkor_norm = normalize_falkor(falkor_result)

    match = neo_norm == falkor_norm
    print(f"\n--- Comparing {name} ---")
    print("Neo4j:", neo_norm)
    print("Falkor:", falkor_norm)
    emoji = "✅" if match else "❌"
    print(f"{emoji} {name}: {len(neo_norm)} Neo4j vs {len(falkor_norm)} Falkor")

def main():
    for name, query in comparison_queries.items():
        print("name:")
        print(name)
        neo_query = query
        falkor_query = query
        if name in ['created_rels', 'friends_with_rels']:
                neo_query = query["neo"]
                falkor_query = query["falkor"]
        print("querying neo...")
        neo_result = query_neo4j(neo_query)
        print("querying falkor...")
        falkor_result = query_falkor(falkor_query)
        compare_results(name, neo_result, falkor_result)

if __name__ == "__main__":
    main()