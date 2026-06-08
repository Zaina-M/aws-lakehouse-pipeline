# E-Commerce Lakehouse Pipeline — A Beginner's Walkthrough

A complete plain-English guide to what this project does, why it's built the way it is, and how to run it from scratch. No prior AWS or data engineering experience is assumed.

---

## Table of Contents

1. [The 30-second pitch](#the-30-second-pitch)
2. [The story behind the project](#the-story-behind-the-project)
3. [The warehouse analogy: the whole system in one picture](#the-warehouse-analogy-the-whole-system-in-one-picture)
4. [The architecture, piece by piece](#the-architecture-piece-by-piece)
5. [The ordering problem: why order_items always goes last](#the-ordering-problem-why-order_items-always-goes-last)
6. [The upsert pattern: running twice should be safe](#the-upsert-pattern-running-twice-should-be-safe)
7. [Data quality: cast, deduplicate, validate, reject](#data-quality-cast-deduplicate-validate-reject)
8. [Failure handling and alerting](#failure-handling-and-alerting)
9. [Quality: tests and what they actually prove](#quality-tests-and-what-they-actually-prove)
10. [Why these specific choices?](#why-these-specific-choices)
11. [What you need to know to work with this](#what-you-need-to-know-to-work-with-this)
12. [How to deploy it (step by step)](#how-to-deploy-it-step-by-step)
13. [How to query the data in Athena](#how-to-query-the-data-in-athena)
14. [Performance and timing expectations](#performance-and-timing-expectations)
15. [What success looks like](#what-success-looks-like)
16. [Troubleshooting common issues](#troubleshooting-common-issues)
17. [How to explain this to a non-technical person](#how-to-explain-this-to-a-non-technical-person)
18. [Future improvements](#future-improvements)
19. [Glossary: jargon translated](#glossary-jargon-translated)

---

## The 30-second pitch

> Imagine an online shop. Every month, the operations team exports three files: a product catalogue listing every item sold, a list of customer orders, and a breakdown of the individual items inside each order. These files arrive as raw spreadsheets — messy, sometimes inconsistent, occasionally containing errors.
>
> The business wants to query this data with SQL. "Which department's products were ordered most in April?" "What was our total revenue and average order value?" "Which order items reference a product or order that doesn't actually exist?" Right now, someone is doing this manually.
>
> This project is the automated system that takes those raw files, checks them for errors, organises them into a reliable database, and makes them instantly queryable — without anyone clicking a button. Bad rows are captured and explained, not silently discarded. If anything breaks, an email goes out. Everything is reproducible from code.

**You drop files in. Minutes later, they are queryable in SQL. Every time.**

---

## The story behind the project

An e-commerce company receives sales data once a month. Three files arrive together:

```
products.csv                  — the product catalogue: product_id, department_id, department, product_name
orders_apr_2025.xlsx          — one row per order: order_num, order_id, user_id, order_timestamp, total_amount, date
order_items_apr_2025.xlsx     — one row per item in an order: id, order_id, user_id,
                                days_since_prior_order, product_id, add_to_cart_order, reordered,
                                order_timestamp, date
```

These files on their own are hard to use. The orders file has a `total_amount` and a `user_id` but no product detail. The order items file references `order_id` and `product_id` but doesn't say what those products are or which department they belong to — it just records *that* a product was added to an order, in what position (`add_to_cart_order`), and whether it was a repeat purchase (`reordered`). To answer any real business question — like "which department gets reordered most?" — all three files need to be brought together.

More importantly, the files have relationships that must be respected:

- An order item **must** reference an order that actually exists. An item saying "this belongs to order #999" when order #999 doesn't exist in the orders file is a data error — an orphan.
- An order item **must** reference a product that actually exists. Same principle.

These integrity rules mean the pipeline can't process the three files in any random order. Products and orders have to be loaded and validated *before* order items, because order items need to check their foreign keys against both of them.

The goal: take these files, clean them, validate them, store them in a reliable queryable format, and capture any bad rows for investigation — automatically, every time, in the right order.

**That entire journey — from raw spreadsheets to SQL-queryable tables — is what this project automates.**

---

## The warehouse analogy: the whole system in one picture

To picture the pipeline, imagine a warehouse that receives and processes incoming stock:

| Warehouse role | What it represents in our system |
|---|---|
| **The receiving dock** where deliveries arrive | S3 `raw` bucket — where the uploaded files land |
| **The product catalogue binder** | `products.csv` — the reference list for what exists |
| **The sales ledger** | `orders_apr_2025.xlsx` — customer purchase records |
| **Individual line items on receipts** | `order_items_apr_2025.xlsx` — what was in each order |
| **The quality control inspector** | Glue job validation — checks each row before it enters the system |
| **The "doesn't belong here" bin** | S3 `rejects` bucket — rows that failed inspection |
| **The warehouse manager** who assigns tasks in order | Step Functions — orchestrates who does what and when |
| **The organised shelves** where stock is filed | Delta tables in S3 `dwh` bucket — the clean, queryable data |
| **The off-site archive** where originals are stored | S3 `archive` bucket — processed source files move here |
| **The inventory lookup terminal** | Athena — run SQL queries directly against the shelves |
| **The smoke alarm** | CloudWatch + SNS — alerts you when anything breaks |

When a delivery arrives, the warehouse manager assigns work in the correct order: first catalogue items and orders (so the reference data is available), then order line items (which cross-check against both). Each step passes quality inspection. Rejected items go to the bin. Good items go to the shelves. The originals are moved to off-site storage. If anything breaks, the alarm sounds.

That's the whole system. Everything below is the detailed explanation.

---

## The architecture, piece by piece

### 1. S3 — the five storage zones

**What S3 is:** Amazon's file storage service. Think of it as Google Drive but designed for code, not humans. Files live in **buckets** (top-level containers). You access them by their path rather than browsing them visually.

This project uses **five** buckets, each with a distinct purpose:

```
lakehouse-dev-raw        ← incoming files land here (the "inbox")
lakehouse-dev-scripts    ← the Glue ETL code lives here
lakehouse-dev-dwh        ← processed Delta tables (what analysts query)
lakehouse-dev-archive    ← raw files are moved here after successful processing
lakehouse-dev-rejects    ← rows that failed validation are written here as CSV
```

**Why five buckets instead of one with subfolders?**

Three concrete reasons:

- **Security.** IAM permission policies work at the bucket level. With five separate buckets, the Glue job can have delete access on `raw` (needed to move files to archive) without having delete access on `dwh` (the Delta tables, which must never be deleted). One bucket with subfolders would require complex policy conditions to express the same separation.

- **Different lifecycle rules.** Archive files should automatically move to Glacier storage (cheaper cold storage) after 90 days. Delta tables should not. A lifecycle rule on a whole bucket is clean. A prefix-filtered rule inside a shared bucket is fragile.

- **Operational visibility.** If a file is in `raw`, it has not been processed. If it's absent from `raw` and present in `archive`, processing succeeded. You can read pipeline state just by checking which bucket holds the file.

---

### 2. AWS Glue — the processing engine

**What Glue is:** A managed service for running data-processing code. You write a Python script that says *"read this file, clean it, write the results here,"* and Glue spins up a temporary computing cluster to run it, then shuts the cluster down when done. You only pay during the run. Glue uses Apache Spark — a framework built for processing large datasets across many machines simultaneously.

This project has **one Glue job** that accepts a `--DATASET_TYPE` parameter (`products`, `orders`, or `order_items`). Step Functions calls the same job three times, passing a different dataset type each time.

Why one job instead of three? When the shared utility code changes — the validation engine, the Delta write logic, the S3 archive operation — you upload one zip file. Three jobs would require three uploads, three separate Terraform resources, and three places to forget to update. Each dataset still has its own handler file (`products.py`, `orders.py`, `order_items.py`) so a bug in one can't affect the others.

---

### 3. Delta Lake — the smart storage layer

**What Delta Lake is:** A file format that makes S3 behave like a reliable database. Plain files in S3 are good for storage but dangerous for updates — if a write is interrupted halfway through, readers see corrupted partial data. Delta Lake adds:

- **A transaction log** — a record of every change ever made. Readers always see the last complete, consistent state. Half-written files are invisible.
- **MERGE (upsert)** — update matching rows and insert new ones in a single operation, without rewriting the entire table.
- **Schema enforcement** — rejects writes whose columns don't match the expected structure.
- **Time travel** — the ability to query what the data looked like at any previous point.

Think of it as the difference between saving a Word document (overwrite the whole file every time) and using Track Changes (atomic edits, history preserved, nothing ever lost).

---

### 4. Step Functions — the warehouse manager

**What Step Functions is:** A "state machine" — a flowchart that AWS actually executes for you. You define the steps, the order, and the rules ("if step 2 fails, go to step 5 and alert"), and AWS runs them, remembering exactly where it is even if something crashes.

The pipeline state machine looks like this:

```
IngestDimensions (Parallel — both branches run at the same time)
├── Branch 1: IngestProducts
└── Branch 2: IngestOrders
        ↓  (Step Functions waits here until BOTH complete)
IngestOrderItems
        ↓
StartCrawler
        ↓
WaitForCrawler → GetCrawlerStatus → CheckCrawlerStatus
                                    ├── RUNNING → back to WaitForCrawler
                                    └── READY   → PipelineSucceeded
```

Any failure at any step routes to `NotifyFailure → PipelineFailed`.

Products and orders run **simultaneously** because they don't depend on each other — this cuts pipeline time roughly in half. Order items runs **after** both complete, because it needs to check its foreign keys against the tables they just created. This ordering is the most important design constraint in the whole system — see the next section.

---

### 5. Athena — the query tool

**What Athena is:** A query service that runs SQL directly against files in S3. Once data is stored as Delta tables and registered in the AWS Glue Data Catalog, you can open Athena, type a SQL query, and get results — without loading data into a separate database or managing any servers. You pay only for the data scanned per query.

The Glue crawler at the end of the pipeline keeps the catalog's partition metadata fresh, so Athena always sees the latest data.

---

### 6. IAM — the permissions layer

**What IAM is:** How AWS controls what each service is allowed to do. Every service gets an **IAM role** — a named identity with an explicit list of permitted actions.

| Role | What it can do |
|---|---|
| **Glue job role** | Read/write all 5 S3 buckets; read/write the Glue Data Catalog; write to CloudWatch Logs |
| **Step Functions role** | Start and monitor Glue jobs; start and check the Glue crawler; publish to the SNS alert topic; write to CloudWatch Logs |

The Step Functions role **cannot touch S3 at all.** All data work is done by Glue. If a bug in the state machine definition accidentally caused a wrong step to execute, it physically could not corrupt the Delta tables — it doesn't have permission. This is called least-privilege and it limits the blast radius of any mistake.

---

## The ordering problem: why order_items always goes last

This is the most important design constraint in the system. If you understand this section, you understand why the pipeline is structured the way it is.

### The problem

An `order_items` row looks like this:

```
id=12, order_id=10002, user_id=7864, product_id=460, add_to_cart_order=2, reordered=0
```

Before this row can be accepted, we must verify:
- **Does order 10002 exist?** If not, this item is an orphan — it belongs to a non-existent order.
- **Does product 460 exist?** If not, this item references a product that isn't in the catalogue.

To do this verification, the `orders` and `products` tables must already be loaded and populated. If `order_items` tried to run first, there would be nothing to check against — the validation would be meaningless.

### How it's enforced

Step Functions handles this through its `Parallel` state. A `Parallel` state runs multiple branches **simultaneously** and only exits when every branch has completed successfully. The state machine is designed so that `IngestOrderItems` is the next state after the `Parallel` state — not inside it.

```
IngestDimensions (Parallel)
├── IngestProducts ──┐
└── IngestOrders ────┘── Step Functions holds here until BOTH finish
                              ↓
                       IngestOrderItems
```

This means the sequencing guarantee is **structural, not code-based.** The `order_items` Glue job doesn't poll to check if the other jobs are done. It doesn't check whether the Delta tables exist before running. It just runs — and it's guaranteed to run only after both prerequisite jobs have finished, because Step Functions won't start it earlier.

If either `IngestProducts` or `IngestOrders` fails, Step Functions routes the whole execution to `NotifyFailure`. `IngestOrderItems` never runs at all — so you won't get a misleading success on order items while the product data is broken.

### What happens if you bypass Step Functions

If you manually start the `order_items` Glue job (from the console or CLI) before the products and orders jobs have finished, it will fail with a "path does not exist" error — the Delta tables won't be at the expected S3 paths. The ordering guarantee lives in the state machine, not in the job code. Don't bypass it.

---

## The upsert pattern: running twice should be safe

### The problem without upserts

Suppose a pipeline run fails halfway through — the products job succeeded but the orders job crashed. You fix the problem and rerun the pipeline. Now the products job runs again for the same file. What should happen?

With plain file writes (overwrite), you'd be fine on the first duplicate. But with some file formats, re-running writes a second copy of every row alongside the first — now you have duplicates. Even worse: if the table has partition folders, a re-run might add a second batch of files to the same folder, doubling the row counts.

This class of bug — where re-running a pipeline *looks* like it worked but silently corrupted the data — is one of the hardest to catch because everything appears normal.

### The fix: Delta MERGE

Every data write in this pipeline uses Delta Lake's `MERGE INTO` operation. It works like this:

- For each incoming row, look for an existing row with the same primary key.
- **If a match is found:** update the existing row's values.
- **If no match is found:** insert the row as new.

The result is the same whether you run the pipeline once or ten times on the same file: the table ends up with exactly the right rows. This property is called **idempotent** — safe to repeat.

### First run vs subsequent runs

Delta `MERGE` requires the target table to already exist. The very first time the pipeline runs, there's no table yet. The code handles this by checking first:

```
Does the Delta table exist at this path?
    → No (first run)  → Write with overwrite mode (creates the table)
    → Yes (re-run)    → Run MERGE (updates matching rows, inserts new ones)
```

`overwrite` on first run is also defensive: if a previous failed run left half-written partial files, overwrite replaces them cleanly before the merge logic takes over.

---

## Data quality: cast, deduplicate, validate, reject

Every incoming file goes through exactly four steps in order. The order matters — doing them in a different sequence produces wrong results or misleading errors.

### Step 1: Cast column types

XLSX and CSV files commonly load every column as text (strings), even columns that should be numbers. A validation rule like "total_amount must not be negative" written as `total_amount < 0` silently returns `null` for every row if `total_amount` is still a string — comparisons between a string and a number are undefined in Spark. You'd mark no rows as rejected but the validation did nothing.

Casting converts each column to its proper type first (`total_amount` → double, `order_id` → integer, `date` → date, `order_timestamp` → timestamp). If a cast fails — say a cell contains `"abc"` where a number is expected — Spark replaces it with `null`, which is then caught by the required-field check in step 3. No silent misses.

### Step 2: Remove duplicates

A duplicate row is the same record appearing twice in the source file. It is not a data error — it's a source system quirk. The right response is to keep one copy, not to reject either.

Deduplication must happen before validation for two reasons:

1. **Accurate reject counts.** If a bad row appears twice and you validate before deduplicating, you get two identical reject records for the same underlying problem. The rejects report looks twice as bad as it is.
2. **Safe Delta MERGE.** If two rows with the same primary key both passed validation and were both sent to `MERGE`, the `MERGE` receives contradictory instructions — update this key to value A AND update this key to value B. The result is undefined. Deduplication ensures each primary key appears exactly once in the source going into `MERGE`.

### Step 3: Validate rows

Validation rules are applied in a fixed order. Each row starts with no rejection reason. A rule fires only if no earlier rule has already tagged the row. Once a row is tagged, all later rules skip it.

The rules actually applied, per dataset, in order:

- **products:** required field is null (`product_id`, `product_name`).
- **orders:** required field is null (`order_id`, `user_id`, `date`) → `total_amount` is negative → `date` is in the future (an order dated three years from now is almost certainly wrong).
- **order_items:** required field is null (`id`, `order_id`, `product_id`) → foreign key doesn't exist (`order_id` not in the orders table → `orphan_order_id`; `product_id` not in the products table → `orphan_product_id`).

**Why first-rule-wins instead of recording all violations?**

An `orders` row with a null `order_id` AND a negative `total_amount` has two problems, but the null `order_id` is the root problem — the amount check is meaningless for a row that can't be identified in the first place. First-rule-wins means the rejects file says `null_required_field`, which is the signal the source system needs to fix. Recording both reasons for the same row creates confusion about which one to fix first.

### Step 4: Separate valid rows from rejects

After validation, rows split into two groups:
- **Valid rows** → Delta MERGE into the `dwh` bucket
- **Rejected rows** → written as CSV to the `rejects` bucket with an extra `_rejection_reason` column

If there are no rejected rows, no file is written to the rejects bucket. The absence of a rejects file is itself a positive signal — the data was completely clean.

Rejects are written as CSV (not Parquet or Delta) because the intended reader is a human investigating a data quality issue, likely using Excel or a text editor. CSV is universally readable without any special tools.

---

## Failure handling and alerting

A pipeline that silently drops errors is worse than no pipeline at all.

### Three layers of failure alerting

| What can break | What detects it | Where the alert comes from |
|---|---|---|
| A Glue job fails (bad data, memory error, code bug) | CloudWatch monitors the Glue job run | Glue failure → CloudWatch → SNS email |
| The Step Functions execution fails overall | CloudWatch monitors state machine executions | SFN failure → CloudWatch → SNS email |
| Any failure at any state | The state machine's `Catch` block | `NotifyFailure` → SNS directly |

All three paths feed the same SNS topic, so you manage one email subscription. The alert includes the error message and the state that failed, so you know immediately which part of the pipeline broke.

### Confirm your email subscription

When you run `terraform apply` for the first time, AWS sends a "Subscription Confirmation" email to the address in your `terraform.tfvars`. **You must click the confirmation link.** Until you do, no failure alerts will reach you. Check your spam folder if it doesn't arrive within a few minutes.

### Recovery flow

When you receive a failure alert:
1. Open the Step Functions console → click the failed execution → click the red state
2. Read the error message displayed in the state output panel
3. For full Glue logs: Glue console → ETL Jobs → `lakehouse-dev-etl` → Run history → click the run → Output logs
4. Fix the root cause (bad file, code bug, permissions issue)
5. Re-upload the source files and trigger the pipeline again
6. The Delta MERGE handles re-processing safely — no manual cleanup needed

---

## Quality: tests and what they actually prove

Two things separate a working pipeline from a *trusted* one: tests that catch regressions, and defensive code that limits the blast radius of bugs.

### What is tested

| Test file | What it tests |
|---|---|
| `test_validators.py` | Each validation rule in isolation — no Spark, no AWS, just the rule logic |
| `test_products.py` | Full `products.run()` end to end: correct rows go to Delta, bad rows to rejects, file archived |
| `test_orders.py` | Same for orders |
| `test_order_items.py` | Same for order_items, including foreign-key validation against mock parent tables |

### How the tests are structured

**S3 and the Glue catalog are mocked** — fake implementations that return pre-built data without needing real AWS credentials. This means the tests run anywhere, instantly.

**But Spark and Delta Lake are real.** Each test writes actual Delta tables to a local temporary directory and reads them back. This is the key design decision: if the Delta MERGE logic produces wrong results, the test fails because the data read back doesn't match what was expected — not because a mock assertion says a function was called. You get real confidence that the logic works, not just that it ran.

**The session-scoped SparkSession:** Starting a SparkSession takes ~5 seconds because it starts a JVM (Java Virtual Machine). With one shared session, this happens once for the entire test suite. Without it, every test would wait 5 seconds on startup and the suite would take minutes before a line of test logic ran.

Running the tests:

```bash
pytest tests            # everything
pytest tests -v         # with verbose output
pytest tests -m "not spark"  # skip Spark tests (faster for quick checks)
```

---

## Why these specific choices?

Here are the design decisions you might be asked to defend.

### "Why a lakehouse (S3 + Delta) instead of a data warehouse like Redshift?"

A data warehouse stores data inside the vendor's managed service. It's fast to query but the data is locked in — extracting raw files is difficult, and storage is expensive. If you ever want to switch providers or use the files for something else, you're stuck.

A plain data lake (files in S3) is cheap and open, but has no update mechanism and no protection against partial writes. Delta Lake adds the reliability of a database on top of S3 files: the files stay in S3 (cheap, yours, open), but writes are safe and rows can be updated efficiently.

### "Why Terraform instead of clicking through the AWS console?"

The console is fine to **explore**. It's terrible to **maintain**:
- Clicks aren't repeatable. Setting up a new environment means remembering every step.
- Clicks aren't reviewable. There's no diff showing what changed last week.
- Clicks aren't recoverable. Accidentally delete a bucket on Wednesday, discover Friday you needed it.

Terraform turns infrastructure into code — versionable, reviewable, reproducible. The same `terraform apply` command builds identical environments in any account.

### "Why Glue instead of Lambda or a regular Python script?"

| Option | Why not used |
|---|---|
| Python script on a server | The server runs 24/7 even when idle. Crashes need manual recovery. No built-in scaling. |
| AWS Lambda | 15-minute maximum runtime, limited memory. Can't run Spark for large datasets. |
| **AWS Glue** | Serverless Spark — spins up on demand, runs the code, shuts down when done. No server to manage. Supports Delta Lake natively. |

### "Why split the code into `utils/` and `jobs/`?"

`utils/` contains reusable building blocks: the validation engine, the Delta write logic, the S3 operations, the catalog registration. It knows nothing about products, orders, or S3 bucket names.

`jobs/` contains the dataset-specific logic: products schema, orders validation rules, order_items foreign-key checks. Each job file calls `utils/` functions with the right parameters.

This separation means: a bug in `products.py` can't affect `orders.py`. Changes to the shared Delta write logic update all three datasets at once. `utils/` functions can be tested in complete isolation without loading any real data.

---

## What you need to know to work with this

You don't need to be an AWS expert. These basics will help:

### Concepts
- **What S3 is** — file storage in the cloud. Buckets are top-level containers.
- **What IAM is** — the permissions system. Roles are like name badges that say what a service is allowed to do.
- **What a Glue job is** — a Python/Spark script that AWS runs on a temporary cluster.
- **What Step Functions is** — a flowchart that AWS executes, with retries and a visual audit trail.
- **What Delta Lake is** — a smarter file format that makes S3 behave like a database.
- **What Athena is** — SQL queries that run directly against files in S3.

### Tools
- **AWS CLI** — the command-line tool for AWS. Used to upload files, check state, etc.
- **Terraform** — the tool that creates AWS resources from the `.tf` config files.
- **PowerShell** — the terminal for running commands on Windows.

### Skills
- **Reading error messages carefully** — when something breaks, the message almost always says exactly what's wrong. Don't skim it.
- **Using the AWS Console for debugging** — even when deploying with Terraform, the console is invaluable for seeing what actually got built and what failed.
- **Finding logs in CloudWatch** — Glue job output streams here in real time.

---

## How to deploy it (step by step)

> **Heads up:** These commands create real AWS resources that cost money (a few dollars for a dev environment). Always run `terraform destroy` when you're done experimenting.

### Prerequisites

| Tool | What it does | Minimum version |
|------|-------------|-----------------|
| AWS CLI | Runs AWS commands from your terminal | v2 |
| Terraform | Creates cloud infrastructure from code | 1.5 |
| Python | Runs local tests | 3.10 |
| Java | Required by PySpark when running tests locally | 11 or 17 |
| Git | Source control | any |

---

### Step 1 — Configure AWS credentials

AWS credentials prove who you are. They live in a file on your machine (`~/.aws/credentials`) — **never** inside this project.

```powershell
aws configure
```

You will be prompted for four values:

| Prompt | What to enter |
|--------|--------------|
| AWS Access Key ID | From your AWS IAM user's security credentials page |
| AWS Secret Access Key | Same page as above |
| Default region name | `eu-west-1` |
| Default output format | `json` |

Verify it worked:
```powershell
aws sts get-caller-identity
```
You should see your AWS account ID and user name. If you see an error, fix your credentials before going further.

---

### Step 2 — Configure the project

Open [envs/dev/terraform.tfvars](envs/dev/terraform.tfvars). This is the only file you need to edit:

```hcl
aws_region  = "eu-west-1"
project     = "lakehouse"
environment = "dev"
alert_email = "your-email@example.com"   # ← change this line
```

The email is where pipeline failure alerts will be sent. Everything else can stay as-is for a development environment.

---

### Step 3 — Create the cloud infrastructure

This creates everything: 5 S3 buckets, IAM roles and policies, the Glue job, the Step Functions state machine, the SNS alert topic, and the CloudWatch log group.

```powershell
cd envs/dev
terraform init                               # downloads the AWS provider — run once only
terraform plan '-var-file=terraform.tfvars'  # shows what will be created — review first
terraform apply '-var-file=terraform.tfvars'
cd ../..
```

> **PowerShell note:** The single quotes around `-var-file=terraform.tfvars` are required. Without them, PowerShell misreads the `=` as a property access operator and Terraform never sees the argument.

`terraform plan` shows a preview of every resource that will be created. Nothing changes until you run `apply`. Read the plan, then type `yes` when prompted.

After `apply` finishes (2–3 minutes), you will see:

```
Apply complete! Resources: 14 added, 0 changed, 0 destroyed.

Outputs:
  glue_job_name       = "lakehouse-dev-etl"
  raw_bucket_name     = "lakehouse-dev-raw"
  scripts_bucket_name = "lakehouse-dev-scripts"
  state_machine_arn   = "arn:aws:states:eu-west-1:xxxxxxxxxxxx:stateMachine:lakehouse-dev-pipeline"
```

Keep this output visible — the bucket names and ARN are used in the steps below.

---

### Step 4 — Confirm your SNS email subscription

Check your inbox for an email titled **"AWS Notification — Subscription Confirmation."** Click the **Confirm subscription** link. Failure alerts will not reach you until you do this.

---

### Step 5 — Upload the ETL code

The Glue job needs the Python processing code uploaded to S3 before it can run.

```powershell
# Package the code into a zip — IMPORTANT: zip from INSIDE glue_jobs/
# so that utils/ and jobs/ sit at the root of the zip.
# If you zip from the project root, Glue sees glue_jobs/utils/ and
# cannot find it when doing "import utils".
New-Item -ItemType Directory -Force -Path dist | Out-Null
Push-Location glue_jobs
Compress-Archive -Path utils, jobs -DestinationPath ..\dist\etl_libs.zip -Force
Pop-Location

# Read the scripts bucket name from Terraform
cd envs/dev
$SCRIPTS_BUCKET = terraform output -raw scripts_bucket_name
cd ../..

# Verify the variable was set — should print: lakehouse-dev-scripts
$SCRIPTS_BUCKET

# Upload
aws s3 cp dist/etl_libs.zip "s3://$SCRIPTS_BUCKET/glue_jobs/etl_libs.zip"
aws s3 cp glue_jobs/main.py  "s3://$SCRIPTS_BUCKET/glue_jobs/main.py"
```

Repeat this step any time you change code in `glue_jobs/`.

---

### Step 6 — Upload the raw data files

The pipeline expects source files in specific S3 subfolders. The path matters — the Glue job looks for files under `products/`, `orders/`, and `order_items/` prefixes.

```powershell
# Read the raw bucket name from Terraform
cd envs/dev
$RAW_BUCKET = terraform output -raw raw_bucket_name
cd ../..

# Verify the variable was set — should print: lakehouse-dev-raw
$RAW_BUCKET

# Upload the three source files
aws s3 cp "Project 2 - Lakehouse Architecture/Data/products.csv" `
    "s3://$RAW_BUCKET/products/products.csv"

aws s3 cp "Project 2 - Lakehouse Architecture/Data/orders_apr_2025.xlsx" `
    "s3://$RAW_BUCKET/orders/orders_apr_2025.xlsx"

aws s3 cp "Project 2 - Lakehouse Architecture/Data/order_items_apr_2025.xlsx" `
    "s3://$RAW_BUCKET/order_items/order_items_apr_2025.xlsx"
```

After a successful pipeline run these files are automatically moved to the archive bucket. To run the pipeline again, upload them again.

---

### Step 7 — Trigger the pipeline

```powershell
# Read the state machine ARN from Terraform
cd envs/dev
$STATE_MACHINE_ARN = terraform output -raw state_machine_arn
cd ../..

# Verify it was set — should start with: arn:aws:states:
$STATE_MACHINE_ARN

# Start the pipeline
aws stepfunctions start-execution `
    --state-machine-arn $STATE_MACHINE_ARN `
    --input '{"run_date": "2025-04-01"}'
```

The `run_date` value is used to organise archived files and rejected rows by date. Use the date that corresponds to your source data.

The command returns a JSON object containing an `executionArn`. Copy it — you need it to check progress in the next step.

---

### Step 8 — Monitor the execution

**Option A — AWS Console (recommended — shows a live visual graph):**

1. Go to [Step Functions in eu-west-1](https://eu-west-1.console.aws.amazon.com/states) — confirm the region selector in the top-right reads **eu-west-1**
2. Click **State machines** in the left sidebar
3. Click `lakehouse-dev-pipeline`
4. Under **Executions**, click the running execution (status: `Running`)
5. Watch the graph: grey = not started, blue = running, green = succeeded, red = failed

**Option B — Terminal:**

```powershell
aws stepfunctions describe-execution `
    --execution-arn <paste-your-execution-arn-here> `
    --query '{status: status, startDate: startDate, stopDate: stopDate}'
```

Run every minute or so until status changes from `RUNNING` to `SUCCEEDED` or `FAILED`.

**How long it takes:**

| Phase | Typical duration |
|-------|-----------------|
| IngestProducts + IngestOrders (running in parallel) | 4–6 min (includes ~2 min Glue cold start each) |
| IngestOrderItems | 2–4 min |
| Crawler poll loop | 1–2 min |
| **Total** | **~7–12 minutes** |

**If the execution fails:**
- You will receive an email at the address in `terraform.tfvars`
- In the Console graph, click the red state to read the error message
- For full Glue logs: AWS Console → **Glue** → **ETL Jobs** → `lakehouse-dev-etl` → **Run history** → click the run → **Output logs**
- For Step Functions logs: AWS Console → **CloudWatch** → **Log groups** → `/aws/states/lakehouse-dev-pipeline`

---

### Step 9 — Query the results in Athena

Once the execution reaches `PipelineSucceeded`, the data is ready.

1. Open the [Athena console](https://eu-west-1.console.aws.amazon.com/athena) — confirm region is **eu-west-1**
2. If prompted, set a query result location: click **Settings** → **Manage** → pick an S3 bucket to store results
3. Set **Data source** to `AwsDataCatalog`
4. Set **Database** to `lakehouse_db`

Example queries:

```sql
-- Highest-value orders on April 1
SELECT order_id, user_id, total_amount, order_timestamp
FROM lakehouse_db.orders
WHERE date = DATE '2025-04-01'
ORDER BY total_amount DESC
LIMIT 10;

-- Total revenue and average order value
SELECT
    COUNT(*)                    AS order_count,
    ROUND(SUM(total_amount), 2) AS total_revenue,
    ROUND(AVG(total_amount), 2) AS avg_order_value
FROM lakehouse_db.orders;

-- How many products live in each department
SELECT department, COUNT(*) AS product_count
FROM lakehouse_db.products
GROUP BY department
ORDER BY product_count DESC;
```

---

### Step 10 — Check for rejected rows (optional)

If any rows failed validation, they are written to the rejects bucket. Each file includes all original columns plus a `_rejection_reason` column that says exactly why the row was rejected.

```powershell
# See what reject files exist
aws s3 ls s3://lakehouse-dev-rejects/rejects/ --recursive

# Download rejects for a specific dataset and run date
aws s3 cp s3://lakehouse-dev-rejects/rejects/order_items/2025-04-01/ ./rejects/ --recursive
```

If no files appear, that means all rows passed validation. The absence of a rejects file is a positive signal.

---

### Step 11 — Tear down (when finished)

To delete all AWS resources created by Terraform:

```powershell
cd envs/dev
terraform destroy '-var-file=terraform.tfvars'
cd ../..
```

> **Warning:** This permanently deletes all S3 buckets and their contents, including processed Delta tables. Back up any data you want to keep before running this.

---

## How to query the data in Athena

Once data is in the `dwh` bucket and registered in the Glue Data Catalog, Athena can query it with standard SQL.

### How Athena sees your data

The Glue Data Catalog acts as a directory: it maps table names (`lakehouse_db.orders`) to S3 paths (`s3://lakehouse-dev-dwh/orders/`) and records the schema. Athena reads that directory and then reads the actual files from S3. No data is moved or copied.

### A few useful queries

```sql
-- The 10 most frequently ordered products
SELECT p.product_name, p.department, COUNT(*) AS times_ordered
FROM lakehouse_db.order_items oi
JOIN lakehouse_db.products p ON oi.product_id = p.product_id
GROUP BY p.product_name, p.department
ORDER BY times_ordered DESC
LIMIT 10;

-- Items sold by department
SELECT p.department, COUNT(*) AS items_sold
FROM lakehouse_db.order_items oi
JOIN lakehouse_db.products p ON oi.product_id = p.product_id
GROUP BY p.department
ORDER BY items_sold DESC;

-- Reorder rate by department (share of line items that were repeat purchases)
SELECT p.department,
       ROUND(AVG(CAST(oi.reordered AS DOUBLE)) * 100, 1) AS reorder_pct
FROM lakehouse_db.order_items oi
JOIN lakehouse_db.products p ON oi.product_id = p.product_id
GROUP BY p.department
ORDER BY reorder_pct DESC;

-- Average basket size (line items per order)
SELECT ROUND(AVG(items), 1) AS avg_items_per_order
FROM (
    SELECT order_id, COUNT(*) AS items
    FROM lakehouse_db.order_items
    GROUP BY order_id
);
```

### Querying rejects (if you want to investigate data quality)

If you set up a Glue crawler on the rejects bucket (or create the table manually), you can also query rejects in Athena:

```sql
SELECT _rejection_reason, COUNT(*) AS row_count
FROM lakehouse_db.order_items_rejects
GROUP BY _rejection_reason
ORDER BY row_count DESC;
```

---

## Performance and timing expectations

| Metric | Typical value |
|--------|--------------|
| **Trigger to first Glue job start** | ~10–30 seconds |
| **IngestProducts duration** | 3–5 min (2 min Glue cold start + ~1 min actual work) |
| **IngestOrders duration** | 3–5 min |
| **IngestOrderItems duration** | 2–4 min |
| **Crawler duration** | 1–2 min |
| **Total end-to-end** | **~7–12 minutes** |

About 60–70% of pipeline time is Glue cluster boot — the actual Spark data processing takes seconds for typical file sizes. At larger data volumes (10× more rows), boot time stays the same; only the data processing portion grows. The pipeline scales better than the small-file timings suggest.

All three S3 write operations (products, orders, order_items) are Delta MERGE — they are safe to interrupt and retry. If the pipeline fails at any point, re-triggering it produces the same final result.

---

## What success looks like

When everything works, triggering the pipeline produces:

1. A green Step Functions execution in the console within 10–12 minutes.
2. Delta table files appearing in `s3://lakehouse-dev-dwh/products/`, `orders/`, and `order_items/`.
3. All three tables queryable in Athena under `lakehouse_db`.
4. The three source files moved from `raw/` to `s3://lakehouse-dev-archive/archived/2025-04-01/`.
5. A rejects file in `s3://lakehouse-dev-rejects/rejects/` only if any rows failed validation — absence means everything passed.
6. **No alert email** (if one arrives, see Troubleshooting below).

If you got all six of those, you have a working data lakehouse.

---

## Troubleshooting common issues

### "Access Denied" or "not authorised" errors during `terraform apply`

Your AWS credentials don't have sufficient permissions to create some resource. Run `aws sts get-caller-identity` to confirm which user or role is being used, then make sure that identity has AdministratorAccess (for a dev environment) or specific permissions for S3, IAM, Glue, Step Functions, SNS, and CloudWatch.

### `terraform plan` or `apply` says "argument --job-name: expected one argument"

This happens when a PowerShell variable like `$STATE_MACHINE_ARN` is empty. Verify the variable was set correctly by printing it before using it. If it prints nothing, run the `terraform output` command again from inside the `envs/dev/` folder.

### No execution appears after uploading files (pipeline doesn't trigger)

The pipeline must be triggered manually — there is no automatic file-upload trigger in this project. Upload your files to S3, then run the `aws stepfunctions start-execution` command from Step 7 above.

### Step Functions execution fails on "IngestOrderItems" immediately

This almost always means `IngestProducts` or `IngestOrders` didn't actually complete successfully on a previous run, leaving no Delta tables for `order_items` to check against. Check the Glue run history for products and orders. Fix any errors there first, then re-trigger the full pipeline.

### "I never got the SNS confirmation email"

Check spam. If it's not there, go to **SNS → Topics → (your alert topic) → Subscriptions** in the AWS console and look for a subscription in "PendingConfirmation" status — you can resend the confirmation from there.

### Athena returns "Table not found" or "Database not found"

The Glue crawler at the end of the pipeline registers the tables in the catalog. If the pipeline didn't complete successfully through the `StartCrawler` state, the tables may not exist yet. Run the full pipeline to completion first.

### `ConcurrentRunsExceededException` when the pipeline starts

The `IngestDimensions` Parallel state launches `products` and `orders` as **two simultaneous runs of the same Glue job**. That needs at least 2 concurrent run slots. If `glue_job_max_concurrent_runs` is set to exactly 2, the system runs at its ceiling with zero headroom — and the moment a lingering run (in `STARTING`/`STOPPING`/`WAITING`) or Glue's run-count consistency lag pushes you over, the second branch is rejected. The retry block can't reliably help because both parallel branches compete for the same slots.

Fix: set `glue_job_max_concurrent_runs = 5` in `envs/dev/terraform.tfvars` and re-apply. This gives room for the 2 parallel runs plus retry overlap. Before re-triggering, clear any zombie runs in non-terminal states:

```powershell
aws glue get-job-runs --job-name lakehouse-dev-etl `
  --query "JobRuns[?JobRunState=='RUNNING' || JobRunState=='STARTING' || JobRunState=='WAITING' || JobRunState=='STOPPING'].{Id:Id,State:JobRunState}" `
  --output table
```

Stop any that appear with `aws glue batch-stop-job-run --job-name lakehouse-dev-etl --job-run-ids <ID>`.

### Glue job fails with `ModuleNotFoundError: No module named 'utils'`

The zip was created from the wrong folder. When you run `Compress-Archive -Path glue_jobs\utils, glue_jobs\jobs` from the project root, PowerShell puts `glue_jobs/utils/` *inside* the zip. Glue adds the zip root to Python's path, so `import utils` fails — there is no `utils/` at the root, only `glue_jobs/utils/`.

Fix: always zip from *inside* `glue_jobs/`:

```powershell
Push-Location glue_jobs
Compress-Archive -Path utils, jobs -DestinationPath ..\dist\etl_libs.zip -Force
Pop-Location
```

This places `utils/` and `jobs/` at the zip root, exactly where Glue expects them. Then re-upload the zip (Step 5) and re-trigger the pipeline.

### Glue job fails with "File not found" or "Path does not exist"

The source files weren't uploaded to the correct S3 prefix, or the `terraform apply` hasn't been run yet. Double-check the exact S3 paths in the upload commands (Step 6) and verify the files are there with `aws s3 ls s3://lakehouse-dev-raw/ --recursive`.

---

## How to explain this to a non-technical person

Here's a script you can use almost verbatim:

> "It's an automated system that takes raw sales files from the business — a product catalogue, a list of customer orders, and the line items inside each order — and turns them into a clean, searchable database.
>
> Every month, someone uploads three spreadsheets to a cloud folder. Our system then runs a four-step process automatically:
>
> 1. **Load the catalogue and orders first.** Before anything else, it brings in the product list and the order records, checks them for errors, and stores them in a reliable database format.
>
> 2. **Then load the order line items.** Each line item has to match a real order and a real product. The system checks every single one. Any line item that references an order or product that doesn't exist gets flagged and set aside — it doesn't silently enter the database and corrupt the numbers.
>
> 3. **Update the search index.** Once the data is stored, it updates a catalogue so the query tool can find it.
>
> 4. **Alert if anything went wrong.** If any step fails, an email goes out explaining exactly what broke and where.
>
> After the system finishes, the analytics team can run SQL queries against the data — "what was our total revenue in April, and which departments got reordered most?" — without downloading anything, without loading data into a separate tool, without waiting. The data is just there.
>
> The whole thing is described in code files, which means we can rebuild it from scratch in a new environment in minutes if we ever had to. There are no manual steps, no tribal knowledge, no "remember to click this thing in the console." It just runs."

---

## Future improvements

These are things a real team would do over time. Items already implemented are marked **DONE**.

### Reliability
- **DONE — Delta MERGE (upsert):** Re-running the pipeline for the same file produces the same result. No duplicates, no full-table rewrites.
- **DONE — Pipeline ordering via Step Functions Parallel:** `order_items` is structurally guaranteed to run after products and orders, not dependent on polling or timing assumptions.
- **DONE — Automatic retry on `ConcurrentRunsExceededException`:** Glue job concurrency limits are handled by the state machine's retry block, not in application code.
- Add input checksumming: verify source files weren't truncated mid-upload by comparing expected vs actual checksums.
- Add a Step Functions execution history export: write execution results to S3 so you can audit "which files were processed on which date" months later.

### Data quality
- **DONE — First-rule-wins validation with rejects output:** Bad rows are captured with a reason, not silently discarded.
- **DONE — Deduplication before validation:** Prevents double-counting in rejects and ensures safe Delta MERGE semantics.
- Add data volume monitoring: alert if a source file is less than 50% the size of the previous run's file — likely a truncation or export error.
- Add schema change detection: alert if the incoming file's columns don't match the expected schema rather than failing mid-pipeline.

### Performance
- **DONE — Partition by date on `orders` and `order_items`:** Athena queries with a date filter only scan the relevant partition folder, not the full table.
- Use Glue job auto-scaling: instead of a fixed number of workers, let Glue scale up for large files and down for small ones.

### Observability
- **DONE — CloudWatch + SNS alerting on all failure paths.**
- **DONE — Structured logging:** Glue job logs stream to CloudWatch in real time.
- Add a Grafana or QuickSight dashboard: pull pipeline metrics (rows processed, rows rejected, duration per run) into a single view.

### Security
- **DONE — Least-privilege IAM:** Each service role has only the permissions it actually needs.
- **DONE — Five separate S3 buckets:** Deletion rights on `raw` cannot cascade to deletion rights on `dwh`.
- Add customer-managed KMS keys for S3 encryption: lets you rotate encryption keys on your own schedule rather than AWS's.
- Restrict Glue job internet egress: run Glue inside a VPC with no outbound internet route, so code bugs can't phone home or exfiltrate data.

### Code quality
- **DONE — Unit tests** for all validation logic and end-to-end job runs (local Spark + Delta, mocked S3/catalog).
- **DONE — Multi-environment Terraform layout:** `envs/dev/` is ready to copy to `envs/staging/` or `envs/prod/`.
- **DONE — Makefile:** Local development commands mirror CI exactly — no surprises when a PR is pushed.
- Add `tflint` and `checkov` to CI: static analysis catches bad Terraform patterns and security issues before they merge.

---

## Glossary: jargon translated

| Term | Plain English |
|---|---|
| **ETL** | "Extract, Transform, Load" — the three steps of moving data from a messy source to a clean destination. |
| **Lakehouse** | A system that stores files in cheap cloud storage (S3) but adds database-like reliability (Delta Lake) on top. |
| **IaC** | Infrastructure as Code — describing your servers, databases, and permissions in text files instead of clicking around in a console. |
| **Bucket** | An S3 top-level container. Think of it as a top-level folder with its own access rules. |
| **Object** | A file in S3. |
| **Prefix** | A folder path inside a bucket (e.g. `products/`, `orders/`). |
| **ARN** | "Amazon Resource Name" — a globally unique ID for any AWS resource. Looks like `arn:aws:states:eu-west-1:...` |
| **Role** | A set of permissions that an AWS service uses to access other AWS services. Like a name badge that says what the wearer is allowed to do. |
| **Delta Lake** | A file format for S3 that adds a transaction log, safe updates, and time travel on top of Parquet files. |
| **Transaction log** | Delta Lake's record of every write ever made. Ensures readers only ever see completed, consistent writes. |
| **MERGE (upsert)** | A Delta Lake operation that updates existing rows and inserts new ones in a single pass. Re-running it with the same data produces the same result. |
| **Idempotent** | An operation that can safely be run multiple times — the result is always the same regardless of how many times you run it. |
| **Parquet** | A column-oriented file format. Much faster to query than CSV when you only need some of the columns. |
| **Partition** | A sub-folder of data organised by some key (e.g. by date). Lets Athena scan only the relevant subset instead of the whole table. |
| **PySpark** | Apache Spark, controlled from Python. The standard way to process large datasets across a cluster of machines. |
| **Glue job** | A Python/Spark script that AWS Glue runs on a temporary cluster. You pay only while the cluster is running. |
| **State machine** | A flowchart that Step Functions executes. Each step can succeed or fail; the machine knows exactly what to do next in either case. |
| **Parallel state** | A Step Functions state that runs multiple branches at the same time and waits for all of them before moving on. |
| **`.sync` resource** | A Step Functions task that starts a Glue job and waits synchronously for it to complete before proceeding. |
| **Crawler** | A Glue service that scans S3 paths and registers (or updates) table metadata in the Glue Data Catalog. Lets Athena see new partitions. |
| **Glue Data Catalog** | A directory that maps table names to S3 paths and schemas. Athena reads this catalog to know where to find your data. |
| **Foreign key** | A column in one table that must reference a primary key in another table. An `order_items.order_id` must match a real `orders.order_id`. |
| **Orphan** | A row with a foreign key that doesn't match any row in the parent table. An orphan order item is rejected, not inserted. |
| **First-rule-wins** | A validation strategy where the first matching rule tags the row and all later rules skip it. One clear reason per rejected row. |
| **SNS** | Amazon's notification service. Publish one message to a topic; it fans out to all subscribers (email, SMS, Lambda, etc.). |
| **CloudWatch** | AWS's monitoring service. Records metrics from all AWS services and can trigger alarms when values cross thresholds. |
| **Least privilege** | Security principle: give each service only the permissions it strictly needs, nothing more. Limits damage if something goes wrong. |
| **Module** | A reusable chunk of Terraform code with its own variables and outputs. Groups related resources together. |
| **State file** | A file Terraform keeps that maps your `.tf` configuration to the actual AWS resources it created. |
| **terraform plan** | A dry run showing exactly what Terraform *would* create, change, or destroy. Nothing happens until you run `apply`. |
| **terraform apply** | The command that actually creates or updates AWS resources to match your configuration. |
| **terraform destroy** | The command that deletes all AWS resources described in your configuration. Irreversible — back up data first. |

---

**That's the whole project.** If you read this top to bottom and followed the deployment steps, you now understand a production-grade AWS data lakehouse — not as a black box, but as a system of cooperating parts where every piece has a reason for being there.
