#!/usr/bin/env node
const { Command } = require('commander');
const sqlite3 = require('sqlite3').verbose();
const { exec } = require('child_process');
const path = require('path');
const fs = require('fs');
const { v4: uuidv4 } = require('uuid');

const DB_PATH = path.resolve(process.cwd(), 'queuectl.sqlite3');
const DEFAULT_MAX_RETRIES = 3;
const DEFAULT_BACKOFF_BASE = 2;
const WORKER_POLL_INTERVAL_MS = 1000;

const program = new Command();
program.name('queuectl').description('QueueCTL - CLI job queue (sqlite3)').version('1.0.0');

let db;
let dbHelpers;

/* DB helpers */
function openDb() {
  return new Promise((resolve, reject) => {
    const exists = fs.existsSync(DB_PATH);
    const d = new sqlite3.Database(DB_PATH, sqlite3.OPEN_READWRITE | sqlite3.OPEN_CREATE, (err) => {
      if (err) return reject(err);
      d.exec('PRAGMA journal_mode = WAL;', (err2) => {
        if (err2) return reject(err2);
        resolve({ db: d, existed: exists });
      });
    });
  });
}

function makeDbHelpers(d) {
  const runAsync = (sql, params = []) =>
    new Promise((res, rej) => d.run(sql, params, function (err) {
      if (err) return rej(err);
      return res({ lastID: this.lastID, changes: this.changes });
    }));

  const getAsync = (sql, params = []) =>
    new Promise((res, rej) => d.get(sql, params, (err, row) => (err ? rej(err) : res(row))));

  const allAsync = (sql, params = []) =>
    new Promise((res, rej) => d.all(sql, params, (err, rows) => (err ? rej(err) : res(rows))));

  const execAsync = (sql) =>
    new Promise((res, rej) => d.exec(sql, (err) => (err ? rej(err) : res())));

  return { runAsync, getAsync, allAsync, execAsync };
}

/* Init DB */
async function initDb() {
  if (db) return;
  const opened = await openDb();
  db = opened.db;
  dbHelpers = makeDbHelpers(db);

  const sql = `
CREATE TABLE IF NOT EXISTS jobs (
  id TEXT PRIMARY KEY,
  command TEXT NOT NULL,
  state TEXT NOT NULL DEFAULT 'pending',
  attempts INTEGER NOT NULL DEFAULT 0,
  max_retries INTEGER NOT NULL DEFAULT ${DEFAULT_MAX_RETRIES},
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  next_run INTEGER NOT NULL DEFAULT (strftime('%s','now')),
  last_error TEXT,
  worker TEXT
);
CREATE TABLE IF NOT EXISTS config (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
INSERT OR IGNORE INTO config(key, value) VALUES ('backoff_base', '${DEFAULT_BACKOFF_BASE}');
INSERT OR IGNORE INTO config(key, value) VALUES ('default_max_retries', '${DEFAULT_MAX_RETRIES}');
`;
  await dbHelpers.execAsync(sql);
}

/* Helpers */
function isoNow() { return new Date().toISOString(); }
function epochNow() { return Math.floor(Date.now() / 1000); }

async function getConfig(key, fallback = null) {
  await initDb();
  const row = await dbHelpers.getAsync('SELECT value FROM config WHERE key = ?', [key]);
  if (!row) return fallback;
  return row.value;
}

async function setConfig(key, value) {
  await initDb();
  await dbHelpers.runAsync('INSERT INTO config(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value', [key, String(value)]);
}

/* CLI: enqueue */
program
  .command('enqueue <payload>')
  .description('Enqueue a job. Payload can be JSON job or a simple command string.')
  .option('--max-retries <n>', 'max retries for this job', parseInt)
  .action(async (payload, opts) => {
    await initDb();
    let job;
    try {
      if (payload.trim().startsWith('{')) job = JSON.parse(payload);
      else job = { command: payload };
    } catch (e) {
      console.error('Invalid JSON payload:', e.message);
      process.exit(1);
    }
    const id = job.id || uuidv4();
    const command = job.command;
    if (!command) {
      console.error('Job must include a "command" field or provide a command string.');
      process.exit(1);
    }
    const now = isoNow();
    const maxRetries = opts.maxRetries ?? job.max_retries ?? Number(await getConfig('default_max_retries', DEFAULT_MAX_RETRIES));
    try {
      await dbHelpers.runAsync(
        `INSERT INTO jobs(id, command, state, attempts, max_retries, created_at, updated_at, next_run, last_error, worker)
         VALUES(?,?,?,?,?,?,?,?,?,?)`,
         [id, command, 'pending', 0, maxRetries, now, now, epochNow(), null, null]
      );
      console.log(`Enqueued job ${id}`);
    } catch (e) {
      console.error('Failed to enqueue:', e.message);
      process.exit(1);
    }
  });

/* CLI: list */
program
  .command('list')
  .description('List jobs. Use --state to filter')
  .option('--state <state>')
  .action(async (opts) => {
    await initDb();
    let rows;
    if (opts.state) rows = await dbHelpers.allAsync('SELECT * FROM jobs WHERE state = ? ORDER BY created_at', [opts.state]);
    else rows = await dbHelpers.allAsync('SELECT * FROM jobs ORDER BY created_at');
    if (!rows || rows.length === 0) {
      console.log('(no jobs)');
      return;
    }
    for (const r of rows) {
      console.log(`${r.id} | ${r.state} | attempts=${r.attempts}/${r.max_retries} | next_run=${new Date(r.next_run*1000).toISOString()} | ${r.command}`);
      if (r.last_error) console.log(`   last_error: ${r.last_error}`);
    }
  });

/* CLI: status */
program
  .command('status')
  .description('Show summary counts of job states')
  .action(async () => {
    await initDb();
    const rows = await dbHelpers.allAsync(`SELECT state, COUNT(*) as cnt FROM jobs GROUP BY state`);
    const map = {};
    rows.forEach(r => map[r.state] = r.cnt);
    console.log('Job counts:');
    console.log(`  pending:   ${map.pending || 0}`);
    console.log(`  processing:${map.processing || 0}`);
    console.log(`  completed: ${map.completed || 0}`);
    console.log(`  dead:      ${map.dead || 0}`);
    console.log(`  failed:    ${map.failed || 0}`);
  });

/* CLI: DLQ */
program
  .command('dlq:list')
  .description('List DLQ (dead) jobs')
  .action(async () => {
    await initDb();
    const rows = await dbHelpers.allAsync('SELECT * FROM jobs WHERE state = ? ORDER BY updated_at DESC', ['dead']);
    if (!rows || rows.length === 0) {
      console.log('(no dead jobs)');
      return;
    }
    for (const r of rows) {
      console.log(`${r.id} | attempts=${r.attempts}/${r.max_retries} | updated_at=${r.updated_at} | ${r.command}`);
      if (r.last_error) console.log(`   last_error: ${r.last_error}`);
    }
  });

program
  .command('dlq:retry <jobId>')
  .description('Retry a dead job (move back to pending, reset attempts)')
  .action(async (jobId) => {
    await initDb();
    const job = await dbHelpers.getAsync('SELECT * FROM jobs WHERE id = ?', [jobId]);
    if (!job) {
      console.error('Job not found:', jobId);
      process.exit(1);
    }
    if (job.state !== 'dead') {
      console.error('Job is not in dead state; only dead jobs should be retried via dlq:retry. Current state:', job.state);
      process.exit(1);
    }
    const now = isoNow();
    await dbHelpers.runAsync(`UPDATE jobs SET state = 'pending', attempts = 0, next_run = ?, updated_at = ?, last_error = NULL, worker = NULL WHERE id = ?`, [epochNow(), now, jobId]);
    console.log(`Job ${jobId} moved to pending and reset attempts.`);
  });

/* CLI: config */
program
  .command('config:set <key> <value>')
  .description('Set config key')
  .action(async (key, value) => {
    await initDb();
    await setConfig(key, value);
    console.log(`Config ${key} set to ${value}`);
  });

program
  .command('config:get <key>')
  .description('Get config key')
  .action(async (key) => {
    await initDb();
    console.log(await getConfig(key, null));
  });

/* Worker control */
let workerController = { running: false, stopRequested: false, activeWorkers: 0 };

program
  .command('worker:start')
  .description('Start workers (runs in current process). Use --count to start multiple worker loops.')
  .option('--count <n>', 'number of worker loops to start', parseInt, 1)
  .action(async (opts) => {
    await initDb();
    const count = opts.count || 1;
    console.log(`Starting ${count} worker(s) in this process. CTRL+C to stop gracefully.`);
    workerController.running = true;
    workerController.stopRequested = false;

    for (let i = 0; i < count; i++) {
      runWorkerLoop(`worker-${i+1}`).catch(err => {
        console.error('Worker loop crashed:', err);
      });
    }

    function shutdownHandler() {
      if (workerController.stopRequested) return;
      console.log('Shutdown requested. Will finish current job(s) then exit.');
      workerController.stopRequested = true;
    }
    process.on('SIGINT', shutdownHandler);
    process.on('SIGTERM', shutdownHandler);
  });

program
  .command('worker:stop')
  .description('Request workers to stop (if running in same process).')
  .action(() => {
    if (!workerController.running) {
      console.log('No workers running in this process.');
      return;
    }
    console.log('Stop requested.');
    workerController.stopRequested = true;
  });

/* Worker loop */
async function runWorkerLoop(name) {
  workerController.activeWorkers++;
  try {
    while (!workerController.stopRequested) {
      const nowEpoch = epochNow();
      const candidate = await dbHelpers.getAsync(`
        SELECT id FROM jobs
        WHERE state = 'pending' AND next_run <= ?
        ORDER BY created_at
        LIMIT 1
      `, [nowEpoch]);

      if (!candidate) {
        await sleep(WORKER_POLL_INTERVAL_MS);
        continue;
      }

      const candidateId = candidate.id;
      const nowIso = isoNow();

      const claim = await dbHelpers.runAsync(`
        UPDATE jobs
        SET state = 'processing', worker = ?, updated_at = ?
        WHERE id = ? AND state = 'pending'
      `, [name, nowIso, candidateId]);

      if (claim.changes !== 1) {
        continue;
      }

      const job = await dbHelpers.getAsync('SELECT * FROM jobs WHERE id = ?', [candidateId]);
      if (!job) continue;

      console.log(`[${name}] picked job ${job.id}: ${job.command}`);

      const exit = await executeCommand(job.command);
      const finishedAtIso = isoNow();

      if (exit.success) {
        await dbHelpers.runAsync(`UPDATE jobs SET state = 'completed', updated_at = ?, last_error = NULL WHERE id = ?`, [finishedAtIso, job.id]);
        console.log(`[${name}] job ${job.id} completed`);
      } else {
        const attempts = job.attempts + 1;
        const maxRetries = job.max_retries;
        const backoffBase = Number(await getConfig('backoff_base', DEFAULT_BACKOFF_BASE)) || DEFAULT_BACKOFF_BASE;
        const nextDelaySecs = Math.pow(backoffBase, attempts);
        const nextRunEpoch = epochNow() + Math.ceil(nextDelaySecs);

        if (attempts > maxRetries) {
          await dbHelpers.runAsync(`UPDATE jobs SET state = 'dead', attempts = ?, updated_at = ?, last_error = ? WHERE id = ?`, [attempts, finishedAtIso, exit.errorMessage, job.id]);
          console.log(`[${name}] job ${job.id} failed (attempts=${attempts}) -> moved to dead`);
        } else {
          await dbHelpers.runAsync(`UPDATE jobs SET state = 'pending', attempts = ?, updated_at = ?, next_run = ?, last_error = ?, worker = NULL WHERE id = ?`, [attempts, finishedAtIso, nextRunEpoch, exit.errorMessage, job.id]);
          console.log(`[${name}] job ${job.id} failed (attempts=${attempts}). Retrying in ${Math.ceil(nextDelaySecs)}s (next_run=${new Date(nextRunEpoch*1000).toISOString()})`);
        }
      }

      if (workerController.stopRequested) break;
    }

    console.log(`[${name}] worker exiting.`);
  } finally {
    workerController.activeWorkers--;
    if (workerController.activeWorkers === 0 && workerController.stopRequested) {
      console.log('All workers stopped. Exiting process.');
      process.exit(0);
    }
  }
}

/* Execute command */
function executeCommand(cmd) {
  return new Promise((resolve) => {
    const shell = process.platform === 'win32' ? 'cmd.exe' : '/bin/bash';
    exec(cmd, { shell }, (error, stdout, stderr) => {
      let success = !error;
      let errMsg = null;
      if (error) {
        errMsg = `exit=${error.code || 'ERR'} stdout=${trim(stdout)} stderr=${trim(stderr)} msg=${error.message}`;
      } else {
        errMsg = null;
      }
      if (stdout && stdout.trim()) console.log(`  stdout: ${trim(stdout)}`);
      if (stderr && stderr.trim()) console.log(`  stderr: ${trim(stderr)}`);
      resolve({ success, errorMessage: errMsg });
    });
  });
}

function trim(s, n = 1000) {
  if (!s) return '';
  const out = String(s).trim();
  if (out.length > n) return out.slice(0, n) + '...';
  return out;
}

function sleep(ms) { return new Promise(res => setTimeout(res, ms)); }

/* Demo/help */
program
  .command('demo:help')
  .description('Show sample commands and expected output')
  .action(() => {
    console.log(`
Examples:

$ node queuectl.js enqueue "echo hello"
$ node queuectl.js worker:start --count 1
$ node queuectl.js list --state pending
$ node queuectl.js dlq:list
$ node queuectl.js dlq:retry <jobId>
$ node queuectl.js config:set backoff_base 3
`);
  });

program.parse(process.argv);
if (!process.argv.slice(2).length) program.outputHelp();
