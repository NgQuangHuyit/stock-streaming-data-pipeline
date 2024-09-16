from contextlib import contextmanager

from cassandra.cluster import Cluster, Session
from cassandra.auth import PlainTextAuthProvider


class CassandraClient:
    def __init__(self, cluster: list[str], username: str, password: str):
        self._auth_provider = PlainTextAuthProvider(username=username, password=password)
        self.cluster = Cluster(cluster, port=9042, auth_provider=self._auth_provider)
        self.session = None

    @contextmanager
    def get_session(self, keyspace: str) -> Session:
        self.session = self.cluster.connect(keyspace)
        try:
            yield self.session
        finally:
            self.session.shutdown()
            self.session = None


