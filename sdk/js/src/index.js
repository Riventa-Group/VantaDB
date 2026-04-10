const grpc = require("@grpc/grpc-js");
const protoLoader = require("@grpc/proto-loader");
const path = require("path");

const PROTO_PATH = path.join(__dirname, "..", "proto", "vantadb.proto");

const packageDef = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});

const vantadb = grpc.loadPackageDefinition(packageDef).vantadb;

/**
 * VantaDB client SDK for Node.js.
 *
 * @example
 * const { VantaClient } = require("vantadb-sdk");
 * const client = new VantaClient("localhost:5432");
 * await client.authenticate("root", "password");
 * const id = await client.insert("mydb", "users", { name: "Alice" });
 */
class VantaClient {
  /**
   * @param {string} address - Host:port (e.g. "localhost:5432")
   * @param {object} [opts] - Options
   * @param {Buffer} [opts.caCert] - CA certificate for TLS
   * @param {Buffer} [opts.clientCert] - Client certificate for mTLS
   * @param {Buffer} [opts.clientKey] - Client key for mTLS
   */
  constructor(address, opts = {}) {
    let credentials;
    if (opts.caCert) {
      credentials = grpc.credentials.createSsl(
        opts.caCert,
        opts.clientKey || null,
        opts.clientCert || null
      );
    } else {
      credentials = grpc.credentials.createInsecure();
    }

    this._auth = new vantadb.VantaAuth(address, credentials);
    this._db = new vantadb.VantaDb(address, credentials);
    this._token = null;
  }

  close() {
    grpc.closeClient(this._auth);
    grpc.closeClient(this._db);
  }

  _metadata() {
    const meta = new grpc.Metadata();
    if (this._token) {
      meta.set("authorization", `Bearer ${this._token}`);
    }
    return meta;
  }

  _promisify(client, method, request) {
    return new Promise((resolve, reject) => {
      client[method](request, this._metadata(), (err, response) => {
        if (err) reject(err);
        else resolve(response);
      });
    });
  }

  _promisifyNoAuth(client, method, request) {
    return new Promise((resolve, reject) => {
      client[method](request, (err, response) => {
        if (err) reject(err);
        else resolve(response);
      });
    });
  }

  // ---- Authentication ------------------------------------

  async authenticate(username, password) {
    const resp = await this._promisifyNoAuth(this._auth, "Authenticate", {
      username,
      password,
    });
    if (!resp.success) throw new Error(`Auth failed: ${resp.error}`);
    this._token = resp.token;
    return resp.role;
  }

  // ---- Database operations --------------------------------

  async createDatabase(name) {
    await this._promisify(this._db, "CreateDatabase", { database: name });
  }

  async dropDatabase(name) {
    await this._promisify(this._db, "DropDatabase", { database: name });
  }

  async listDatabases() {
    const resp = await this._promisify(this._db, "ListDatabases", {});
    return resp.databases;
  }

  // ---- Collection operations ------------------------------

  async createCollection(db, collection) {
    await this._promisify(this._db, "CreateCollection", {
      database: db,
      collection,
    });
  }

  async dropCollection(db, collection) {
    await this._promisify(this._db, "DropCollection", {
      database: db,
      collection,
    });
  }

  async listCollections(db) {
    const resp = await this._promisify(this._db, "ListCollections", {
      database: db,
    });
    return resp.collections;
  }

  // ---- Document operations --------------------------------

  async insert(db, collection, document) {
    const resp = await this._promisify(this._db, "Insert", {
      database: db,
      collection,
      document_json: JSON.stringify(document),
    });
    if (!resp.success) throw new Error(`Insert failed: ${resp.error}`);
    return resp.id;
  }

  async findById(db, collection, id) {
    const resp = await this._promisify(this._db, "FindById", {
      database: db,
      collection,
      id,
    });
    return resp.found ? JSON.parse(resp.document_json) : null;
  }

  async findAll(db, collection) {
    const resp = await this._promisify(this._db, "FindAll", {
      database: db,
      collection,
    });
    return (resp.documents_json || []).map((d) => JSON.parse(d));
  }

  async query(db, collection, filter) {
    const resp = await this._promisify(this._db, "Query", {
      database: db,
      collection,
      filter_json: JSON.stringify(filter),
    });
    return (resp.documents_json || []).map((d) => JSON.parse(d));
  }

  async delete(db, collection, id) {
    const resp = await this._promisify(this._db, "DeleteById", {
      database: db,
      collection,
      id,
    });
    return resp.found;
  }

  async update(db, collection, id, patch) {
    const resp = await this._promisify(this._db, "Update", {
      database: db,
      collection,
      id,
      patch_json: JSON.stringify(patch),
    });
    return resp.modified_count > 0;
  }

  async count(db, collection) {
    const resp = await this._promisify(this._db, "Count", {
      database: db,
      collection,
    });
    return Number(resp.count);
  }

  // ---- Health (no auth) ----------------------------------

  async health() {
    const resp = await this._promisifyNoAuth(this._auth, "HealthCheck", {});
    return {
      status: resp.status,
      uptimeSeconds: Number(resp.uptime_seconds),
      databaseCount: Number(resp.database_count),
      version: resp.version,
    };
  }
}

module.exports = { VantaClient };
