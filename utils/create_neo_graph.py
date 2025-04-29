from neo4j import GraphDatabase

# Neo4j connection details
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASS = "test1234"

# Path to CSVs relative to Neo4j’s import folder
CSV_PATH = "file:///sample_data/"

cypher_commands = [
    f"""
    LOAD CSV WITH HEADERS FROM '{CSV_PATH}users.csv' AS row
    CREATE (:User {{
        name: row.name,
        age: toInteger(row.age),
        email: CASE WHEN row.email = "" THEN NULL ELSE row.email END,
        city: CASE WHEN row.city = "" THEN NULL ELSE row.city END
    }});
    """,
    f"""
    LOAD CSV WITH HEADERS FROM '{CSV_PATH}posts.csv' AS row
    CREATE (:Post {{
        name: row.name,
        likes: toInteger(row.likes),
        category: CASE WHEN row.category = "" THEN NULL ELSE row.category END,
        image_url: CASE WHEN row.image_url = "" THEN NULL ELSE row.image_url END
    }});
    """,
    f"""
    LOAD CSV WITH HEADERS FROM '{CSV_PATH}friends_with.csv' AS row
    MATCH (u1:User {{name: row.start_username}}), (u2:User {{name: row.end_username}})
    CREATE (u1)-[:FRIENDS_WITH {{since: date(row.since)}}]->(u2);
    """,
    f"""
    LOAD CSV WITH HEADERS FROM '{CSV_PATH}created.csv' AS row
    MATCH (u:User {{name: row.username}}), (p:Post {{name: row.postname}})
    CREATE (u)-[:CREATED {{timestamp: datetime(row.timestamp)}}]->(p);
    """,
    "CREATE CONSTRAINT user_name_constraint IF NOT EXISTS FOR (u:User) REQUIRE u.name IS UNIQUE;",
    "CREATE CONSTRAINT post_title_constraint IF NOT EXISTS FOR (p:Post) REQUIRE p.name IS UNIQUE;"
]


def main():
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
    with driver.session() as session:
        for query in cypher_commands:
            result = session.run(query)
            summary = result.consume()
            print("Created:\n", summary.counters)


if __name__ == "__main__":
    main()
