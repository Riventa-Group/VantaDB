#!/usr/bin/env node

/**
 * VantaDB vs PostgreSQL Benchmark
 *
 * Prerequisites:
 *   - VantaDB server running:  vantadb --serve --no-tls
 *   - PostgreSQL running:      docker run -d -p 5433:5432 -e POSTGRES_PASSWORD=bench postgres:16
 *   - Install deps:            cd scripts/benchmark && npm install
 *
 * Usage:
 *   cd scripts/benchmark && npm run bench -- --count 5000
 */

const { VantaClient } = require("../../sdk/js/src/index");

let pg;
try {
  pg = require("pg");
} catch {
  pg = null;
}

// ── Config ──────────────────────────────────────

const args = process.argv.slice(2);
function getArg(name, def) {
  const idx = args.indexOf(name);
  return idx !== -1 && args[idx + 1] ? args[idx + 1] : def;
}

const VANTA_PORT = parseInt(getArg("--vanta-port", "5432"));
const PG_PORT = parseInt(getArg("--pg-port", "5433"));
const COUNT = parseInt(getArg("--count", "5000"));
const VANTA_USER = getArg("--user", "root");
const VANTA_PASS = getArg("--password", "");

// ── Helpers ─────────────────────────────────────

function randomDoc(i) {
  return {
    _id: `user_${i}`,
    name: `user_${i}`,
    email: `user${i}@bench.test`,
    age: 20 + (i % 50),
    score: Math.random() * 100,
    active: i % 2 === 0,
    tags: ["bench", `group_${i % 10}`],
  };
}

async function timedOp(label, fn) {
  const start = process.hrtime.bigint();
  const result = await fn();
  const elapsed = Number(process.hrtime.bigint() - start) / 1e6; // ms
  return { label, elapsed, result };
}

function fmtMs(ms) {
  return ms < 1000 ? `${ms.toFixed(1)}ms` : `${(ms / 1000).toFixed(2)}s`;
}

function fmtOps(count, ms) {
  return `${Math.round((count / ms) * 1000).toLocaleString()} ops/sec`;
}

function percentile(sorted, p) {
  const idx = Math.ceil((p / 100) * sorted.length) - 1;
  return sorted[Math.max(0, idx)];
}

function printResult(label, count, ms) {
  console.log(
    `  ${label.padEnd(35)} ${fmtMs(ms).padStart(10)}  ${fmtOps(count, ms).padStart(15)}`
  );
}

// ── VantaDB Benchmark ───────────────────────────

async function benchVanta() {
  console.log("\n  ╔═══════════════════════════════════════════════════════╗");
  console.log("  ║  VantaDB Benchmark                                    ║");
  console.log("  ╚═══════════════════════════════════════════════════════╝\n");

  const client = new VantaClient(`localhost:${VANTA_PORT}`);

  if (VANTA_PASS) {
    await client.authenticate(VANTA_USER, VANTA_PASS);
  } else {
    await client.authenticate(VANTA_USER, VANTA_PASS);
  }

  // Setup
  try { await client.dropDatabase("bench_db"); } catch {}
  await client.createDatabase("bench_db");
  await client.createCollection("bench_db", "users");

  // 1. Sequential inserts
  const ins = await timedOp("Insert (sequential)", async () => {
    const latencies = [];
    for (let i = 0; i < COUNT; i++) {
      const s = process.hrtime.bigint();
      await client.insert("bench_db", "users", randomDoc(i));
      latencies.push(Number(process.hrtime.bigint() - s) / 1e6);
    }
    latencies.sort((a, b) => a - b);
    return { p50: percentile(latencies, 50), p99: percentile(latencies, 99) };
  });
  printResult("Insert (sequential)", COUNT, ins.elapsed);
  if (ins.result && ins.result.p50) {
    console.log(`    p50: ${fmtMs(ins.result.p50)}  p99: ${fmtMs(ins.result.p99)}`);
  }

  // 2. Point reads (by known pattern)
  const reads = await timedOp("FindAll", async () => {
    await client.findAll("bench_db", "users");
  });
  printResult("FindAll (full scan)", COUNT, reads.elapsed);

  // 3. Query with filter
  const q = await timedOp("Query ($gt filter)", async () => {
    const results = await client.query("bench_db", "users", {
      age: { $gt: 50 },
    });
    return results.length;
  });
  printResult(`Query age>50 (${q.result} hits)`, q.result, q.elapsed);

  // 4. Count
  const cnt = await timedOp("Count", async () => {
    return await client.count("bench_db", "users");
  });
  printResult("Count", 1, cnt.elapsed);

  // 5. Update
  const upd = await timedOp("Update (1000 docs)", async () => {
    for (let i = 0; i < Math.min(1000, COUNT); i++) {
      await client.update("bench_db", "users", `user_${i}`, {
        $set: { score: 99.9 },
      });
    }
  });
  printResult("Update ($set)", Math.min(1000, COUNT), upd.elapsed);

  // 6. Delete
  const del = await timedOp("Delete (500 docs)", async () => {
    for (let i = 0; i < Math.min(500, COUNT); i++) {
      try {
        await client.delete("bench_db", "users", `user_${i}`);
      } catch {}
    }
  });
  printResult("Delete", Math.min(500, COUNT), del.elapsed);

  // Cleanup
  await client.dropDatabase("bench_db");
  client.close();

  return {
    insert: ins.elapsed,
    findAll: reads.elapsed,
    query: q.elapsed,
    count: cnt.elapsed,
    update: upd.elapsed,
    delete: del.elapsed,
  };
}

// ── PostgreSQL Benchmark ────────────────────────

async function benchPostgres() {
  if (!pg) {
    console.log("\n  ⚠ pg module not installed — skipping PostgreSQL benchmark");
    console.log("    Run: npm install pg\n");
    return null;
  }

  console.log("\n  ╔═══════════════════════════════════════════════════════╗");
  console.log("  ║  PostgreSQL Benchmark                                 ║");
  console.log("  ╚═══════════════════════════════════════════════════════╝\n");

  const pool = new pg.Pool({
    host: "localhost",
    port: PG_PORT,
    user: "postgres",
    password: "bench",
    database: "postgres",
    max: 1,
  });

  try {
    // Setup
    await pool.query("DROP TABLE IF EXISTS bench_users");
    await pool.query(`
      CREATE TABLE bench_users (
        id SERIAL PRIMARY KEY,
        name TEXT,
        email TEXT,
        age INT,
        score DOUBLE PRECISION,
        active BOOLEAN,
        tags TEXT[]
      )
    `);

    await pool.query("CREATE INDEX idx_bench_name ON bench_users(name)");

    // 1. Sequential inserts
    const ins = await timedOp("Insert (sequential)", async () => {
      for (let i = 0; i < COUNT; i++) {
        const d = randomDoc(i);
        await pool.query(
          "INSERT INTO bench_users (name, email, age, score, active, tags) VALUES ($1,$2,$3,$4,$5,$6)",
          [d.name, d.email, d.age, d.score, d.active, d.tags]
        );
      }
    });
    printResult("Insert (sequential)", COUNT, ins.elapsed);

    // 2. Full scan
    const reads = await timedOp("SELECT * (full scan)", async () => {
      await pool.query("SELECT * FROM bench_users");
    });
    printResult("SELECT * (full scan)", COUNT, reads.elapsed);

    // 3. Query with filter
    const q = await timedOp("Query (WHERE age > 50)", async () => {
      const res = await pool.query("SELECT * FROM bench_users WHERE age > 50");
      return res.rowCount;
    });
    printResult(`WHERE age>50 (${q.result} hits)`, q.result, q.elapsed);

    // 4. Count
    const cnt = await timedOp("COUNT(*)", async () => {
      const res = await pool.query("SELECT COUNT(*) FROM bench_users");
      return parseInt(res.rows[0].count);
    });
    printResult("COUNT(*)", 1, cnt.elapsed);

    // 5. Update
    const upd = await timedOp("Update (1000 rows)", async () => {
      for (let i = 0; i < Math.min(1000, COUNT); i++) {
        await pool.query(
          "UPDATE bench_users SET score = 99.9 WHERE name = $1",
          [`user_${i}`]
        );
      }
    });
    printResult("UPDATE (SET)", Math.min(1000, COUNT), upd.elapsed);

    // 6. Delete
    const del = await timedOp("Delete (500 rows)", async () => {
      for (let i = 0; i < Math.min(500, COUNT); i++) {
        await pool.query("DELETE FROM bench_users WHERE name = $1", [
          `user_${i}`,
        ]);
      }
    });
    printResult("DELETE", Math.min(500, COUNT), del.elapsed);

    // Cleanup
    await pool.query("DROP TABLE bench_users");
    await pool.end();

    return {
      insert: ins.elapsed,
      findAll: reads.elapsed,
      query: q.elapsed,
      count: cnt.elapsed,
      update: upd.elapsed,
      delete: del.elapsed,
    };
  } catch (e) {
    console.log(`  ✗ PostgreSQL error: ${e.message}`);
    console.log("    Make sure PostgreSQL is running on port " + PG_PORT);
    console.log(
      "    docker run -d -p 5433:5432 -e POSTGRES_PASSWORD=bench postgres:16\n"
    );
    await pool.end();
    return null;
  }
}

// ── Comparison ──────────────────────────────────

function printComparison(vanta, postgres) {
  console.log("\n  ╔═══════════════════════════════════════════════════════╗");
  console.log("  ║  Head-to-Head Comparison                              ║");
  console.log("  ╚═══════════════════════════════════════════════════════╝\n");

  const ops = ["insert", "findAll", "query", "count", "update", "delete"];
  const labels = [
    "Insert (sequential)",
    "Full scan (all docs)",
    "Filter query",
    "Count",
    "Update",
    "Delete",
  ];

  console.log(
    `  ${"Operation".padEnd(25)} ${"VantaDB".padStart(10)} ${"Postgres".padStart(10)} ${"Winner".padStart(10)} ${"Factor".padStart(10)}`
  );
  console.log("  " + "─".repeat(67));

  let vantaWins = 0;
  let pgWins = 0;

  for (let i = 0; i < ops.length; i++) {
    const v = vanta[ops[i]];
    const p = postgres[ops[i]];
    const winner = v < p ? "VantaDB" : "Postgres";
    const factor =
      v < p ? `${(p / v).toFixed(1)}x` : `${(v / p).toFixed(1)}x`;

    if (v < p) vantaWins++;
    else pgWins++;

    const winColor = v < p ? "\x1b[32m" : "\x1b[33m";
    console.log(
      `  ${labels[i].padEnd(25)} ${fmtMs(v).padStart(10)} ${fmtMs(p).padStart(10)} ${winColor}${winner.padStart(10)}\x1b[0m ${factor.padStart(10)}`
    );
  }

  console.log("  " + "─".repeat(67));
  console.log(
    `\n  Score: VantaDB ${vantaWins} — ${pgWins} PostgreSQL\n`
  );

  if (vantaWins > pgWins) {
    console.log("  \x1b[32m✓ VantaDB wins overall!\x1b[0m\n");
  } else if (pgWins > vantaWins) {
    console.log("  \x1b[33mPostgreSQL wins overall\x1b[0m\n");
  } else {
    console.log("  It's a tie!\n");
  }
}

// ── Main ────────────────────────────────────────

async function main() {
  console.log(`\n  VantaDB vs PostgreSQL Benchmark`);
  console.log(`  Documents/rows: ${COUNT.toLocaleString()}`);
  console.log(`  VantaDB port: ${VANTA_PORT} | PostgreSQL port: ${PG_PORT}`);

  let vantaResults, pgResults;

  try {
    vantaResults = await benchVanta();
  } catch (e) {
    console.log(`\n  ✗ VantaDB error: ${e.message}`);
    console.log("    Make sure VantaDB is running: vantadb --serve --no-tls\n");
    process.exit(1);
  }

  pgResults = await benchPostgres();

  if (vantaResults && pgResults) {
    printComparison(vantaResults, pgResults);
  } else if (vantaResults) {
    console.log("\n  ℹ Run with PostgreSQL for comparison\n");
  }
}

main().catch((e) => {
  console.error("Fatal:", e);
  process.exit(1);
});
