# **QueueCTL – CLI-Based Background Job Queue**

`QueueCTL` is a lightweight, CLI-driven background job queue system built for the Backend Developer Internship Assignment. It supports job scheduling, parallel workers, retries with exponential backoff, Dead Letter Queue (DLQ) handling, and persistent storage using SQLite.

This project is intentionally simple, easy to run locally, and demonstrates real-world background processing concepts.

---

## 🔧 **Tech Stack**

* **Node.js** – CLI + worker execution
* **SQLite3** – persistent storage
* **Shell Execution** – executes commands using OS shell (`bash` or `cmd.exe`)

---

## 🚀 **Features**

* Enqueue and manage asynchronous jobs
* Parallel worker processes (`--count N`)
* Atomic job claiming (no duplicate processing)
* Automatic retries with exponential backoff
* Dead Letter Queue for permanently failed jobs
* Persistent job storage across restarts
* Graceful worker shutdown
* Configurable backoff base & retry limits
* Optional per-job workspace & log files

---

# 📦 **Setup Instructions**

### 1. Clone the repository

```bash
git clone: https://github.com/A-man56/queuectl
cd queuectl
```

### 2. Install dependencies

```bash
npm install
```

### 3. Run help

```bash
node queuectl.js --help
```

### 4. (Optional) Reset database

```bash
rm -f queuectl.sqlite3
```

---

# 🧭 **Usage Examples**

### **Enqueue a job**

```bash
node queuectl.js enqueue "echo Hello World"
```

### **Start workers**

```bash
node queuectl.js worker:start --count 3
```

### **Stop workers (gracefully)**

```bash
node queuectl.js worker:stop
```

### **Check status**

```bash
node queuectl.js status
```

### **List jobs**

```bash
node queuectl.js list
node queuectl.js list --state pending
```

### **Dead Letter Queue**

```bash
node queuectl.js dlq:list
node queuectl.js dlq:retry <jobId>
```

### **Configuration**

```bash
node queuectl.js config:set backoff_base 3
node queuectl.js config:get backoff_base
```
---

# 🧩 **How It Works (Architecture Overview)**

### **1. Storage**

SQLite stores all job metadata:

* Job ID
* Command
* State
* Retry counts
* Error messages
* Next scheduled run time

SQLite WAL mode is enabled for concurrency.

---

### **2. Workers**

Workers:

* Poll for pending jobs every second
* Atomically claim jobs via SQL `UPDATE … WHERE state='pending'`
* Prevent double processing
* Execute commands with the OS shell
* Update job state (`completed`, `pending`, `dead`)
* Respect graceful shutdown

Multiple workers can run in parallel using:

```bash
node queuectl.js worker:start --count N
```

---

### **3. Retry Mechanism**

On failure:

```
next_delay = base ^ attempts
```

Where:

* `base` = configurable backoff base
* `attempts` = how many times job has failed

Once `attempts > max_retries`
➡ job moves to **dead** state.

---

### **4. Dead Letter Queue**

Dead jobs are listed via:

```bash
node queuectl.js dlq:list
```

And recovered using:

```bash
node queuectl.js dlq:retry <id>
```

---

# 🧪 **Testing Instructions**

This project includes a **test script** (`test.sh`) that exercises core flows:

### **Run automated test**

```bash
bash test.sh
```

It verifies:

1. Successful job execution
2. Failing job retries
3. Jobs move into DLQ
4. Worker processes behave as expected

If you're on Windows and want PowerShell tests, use:

```bash
powershell -ExecutionPolicy Bypass -File test.ps1
```

(Ask me if you want a pre-generated PowerShell version.)

---

# 📝 **Assumptions & Trade-offs**

* SQLite chosen for simplicity, zero-config persistence, and reliable ACID behavior.
* Workers run in the same process for simplicity; for production you might run them as separate processes.
* No job timeouts (optional bonus feature).
* No job priorities — FIFO based on `created_at`.
* Commands are executed directly — sandboxing or Docker is recommended for untrusted commands.

---

# 🌟 **Optional Enhancements (Supported but not required)**

* Per-job workspace folder with stdout/stderr logs
* Worker crash recovery logic
* Job timeout mechanism
* Job priority queues
* Minimal web dashboard
* REST API wrapper

(Ask if you'd like these added.)

---

# 📹 **Demo Recording**

Add your required CLI demo here:

👉 **Demo URL:** *[https://drive.google.com/](https://drive.google.com/)...*
*(Replace with your uploaded video link)*

**QueueCTL – Backend Developer Assignment**
Built with Node.js + SQLite.
