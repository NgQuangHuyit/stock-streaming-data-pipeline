import argparse
import logging
import os
from contextlib import contextmanager

from cassandra.cluster import Cluster, Session
from cassandra.auth import PlainTextAuthProvider

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


class CassandraClient:
    def __init__(self, cluster: list[str], username: str, password: str):
        self._auth_provider = PlainTextAuthProvider(username=username, password=password)
        self.cluster = Cluster(cluster, port=9042, auth_provider=self._auth_provider)
        self.session = None
        self.logger = logging.getLogger(__name__)

    @contextmanager
    def get_session(self, keyspace: str=None) -> Session:
        if keyspace is not None:
            self.session = self.cluster.connect(keyspace)
        else:
            self.session = self.cluster.connect()
        try:
            yield self.session
        finally:
            self.session.shutdown()
            self.session = None

def cassandra_initalization(cluster, cql_file, username, password):
    queries = open(cql_file, "r").read()
    queries = queries.split(";")
    cassandra_client = CassandraClient(cluster=cluster.split(","), username=username, password=password)
    with cassandra_client.get_session() as session:
        for query in queries:
            if query.strip():
                session.execute(query)
        cassandra_client.logger.info("Cassandra initialization complete")

if __name__ == "__main__":
    cassandra_initalization("localhost", "ddl.cql", "cassandra", "password123")