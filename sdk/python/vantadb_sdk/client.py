"""VantaDB Python Client SDK.

Usage:
    from vantadb_sdk import VantaClient

    client = VantaClient("localhost:5432")
    client.authenticate("root", "password")
    doc_id = client.insert("mydb", "users", {"name": "Alice", "age": 30})
    print(f"Inserted: {doc_id}")
"""

import json
import grpc

# Generated stubs — run scripts/gen-python-sdk.sh to regenerate
from . import vantadb_pb2
from . import vantadb_pb2_grpc


class VantaClient:
    """Ergonomic VantaDB client with auto token management."""

    def __init__(self, address, *, tls=False, ca_cert=None, client_cert=None, client_key=None):
        """Connect to a VantaDB server.

        Args:
            address: Host:port string (e.g. "localhost:5432")
            tls: Enable TLS
            ca_cert: CA certificate bytes for server verification
            client_cert: Client certificate bytes for mTLS
            client_key: Client key bytes for mTLS
        """
        if tls:
            credentials = grpc.ssl_channel_credentials(
                root_certificates=ca_cert,
                private_key=client_key,
                certificate_chain=client_cert,
            )
            self._channel = grpc.secure_channel(address, credentials)
        else:
            self._channel = grpc.insecure_channel(address)

        self._auth_stub = vantadb_pb2_grpc.VantaAuthStub(self._channel)
        self._db_stub = vantadb_pb2_grpc.VantaDbStub(self._channel)
        self._token = None

    def close(self):
        """Close the gRPC channel."""
        self._channel.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def _metadata(self):
        if self._token:
            return [("authorization", f"Bearer {self._token}")]
        return []

    # ---- Authentication --------------------------------------

    def authenticate(self, username, password):
        """Authenticate and store JWT token. Returns the user's role."""
        resp = self._auth_stub.Authenticate(
            vantadb_pb2.AuthRequest(username=username, password=password)
        )
        if not resp.success:
            raise Exception(f"Authentication failed: {resp.error}")
        self._token = resp.token
        return resp.role

    # ---- Database operations ---------------------------------

    def create_database(self, name):
        self._db_stub.CreateDatabase(
            vantadb_pb2.DatabaseRequest(database=name),
            metadata=self._metadata(),
        )

    def drop_database(self, name):
        self._db_stub.DropDatabase(
            vantadb_pb2.DatabaseRequest(database=name),
            metadata=self._metadata(),
        )

    def list_databases(self):
        resp = self._db_stub.ListDatabases(
            vantadb_pb2.Empty(), metadata=self._metadata()
        )
        return list(resp.databases)

    # ---- Collection operations -------------------------------

    def create_collection(self, db, collection):
        self._db_stub.CreateCollection(
            vantadb_pb2.CollectionRequest(database=db, collection=collection),
            metadata=self._metadata(),
        )

    def drop_collection(self, db, collection):
        self._db_stub.DropCollection(
            vantadb_pb2.CollectionRequest(database=db, collection=collection),
            metadata=self._metadata(),
        )

    def list_collections(self, db):
        resp = self._db_stub.ListCollections(
            vantadb_pb2.DatabaseRequest(database=db),
            metadata=self._metadata(),
        )
        return list(resp.collections)

    # ---- Document operations ---------------------------------

    def insert(self, db, collection, document):
        """Insert a document. Returns the assigned ID."""
        resp = self._db_stub.Insert(
            vantadb_pb2.InsertRequest(
                database=db,
                collection=collection,
                document_json=json.dumps(document),
            ),
            metadata=self._metadata(),
        )
        if not resp.success:
            raise Exception(f"Insert failed: {resp.error}")
        return resp.id

    def find_by_id(self, db, collection, doc_id):
        """Find a document by ID. Returns dict or None."""
        resp = self._db_stub.FindById(
            vantadb_pb2.FindByIdRequest(database=db, collection=collection, id=doc_id),
            metadata=self._metadata(),
        )
        if resp.found:
            return json.loads(resp.document_json)
        return None

    def find_all(self, db, collection):
        """Find all documents in a collection."""
        resp = self._db_stub.FindAll(
            vantadb_pb2.FindAllRequest(database=db, collection=collection),
            metadata=self._metadata(),
        )
        return [json.loads(d) for d in resp.documents_json]

    def query(self, db, collection, filter_dict):
        """Query with a MongoDB-style filter dict."""
        resp = self._db_stub.Query(
            vantadb_pb2.QueryRequest(
                database=db,
                collection=collection,
                filter_json=json.dumps(filter_dict),
            ),
            metadata=self._metadata(),
        )
        return [json.loads(d) for d in resp.documents_json]

    def delete(self, db, collection, doc_id):
        """Delete a document by ID. Returns True if found."""
        resp = self._db_stub.DeleteById(
            vantadb_pb2.DeleteByIdRequest(database=db, collection=collection, id=doc_id),
            metadata=self._metadata(),
        )
        return resp.found

    def update(self, db, collection, doc_id, patch):
        """Update a document by ID. Returns True if modified."""
        resp = self._db_stub.Update(
            vantadb_pb2.UpdateRequest(
                database=db,
                collection=collection,
                id=doc_id,
                patch_json=json.dumps(patch),
            ),
            metadata=self._metadata(),
        )
        return resp.modified_count > 0

    def count(self, db, collection):
        """Count documents in a collection."""
        resp = self._db_stub.Count(
            vantadb_pb2.CountRequest(database=db, collection=collection),
            metadata=self._metadata(),
        )
        return resp.count

    # ---- Health (no auth) ------------------------------------

    def health(self):
        """Health check. Returns dict with status, uptime, etc."""
        resp = self._auth_stub.HealthCheck(vantadb_pb2.Empty())
        return {
            "status": resp.status,
            "uptime_seconds": resp.uptime_seconds,
            "database_count": resp.database_count,
            "version": resp.version,
        }
