import os
from falkordb import FalkorDB
from dotenv import load_dotenv

load_dotenv()
FALKOR_DB_HOST = os.getenv("FALKOR_DB_HOST", "localhost")
FALKOR_DB_PORT = os.getenv("FALKOR_DB_PORT", "6379")
FALKOR_DB_GRAPH_NAME = os.getenv("FALKOR_DB_GRAPH_NAME", "SocialGraph")


def main():
    client = FalkorDB(host=FALKOR_DB_HOST, port=FALKOR_DB_PORT)
    graph = client.select_graph(FALKOR_DB_GRAPH_NAME)
    # Remove the internal neo4j ids (used dring the migration)
    print("Removing element_id from :User nodes...")
    graph.query("MATCH (n:User) REMOVE n.element_id")

    print("Removing element_id from :Post nodes...")
    graph.query("MATCH (n:Post) REMOVE n.element_id")

    print("Removing element_id from all relationships...")
    graph.query("MATCH (n)-[r]->(m) REMOVE r.element_id")

    print("✅ element_id removal complete.")


if __name__ == "__main__":
    main()
