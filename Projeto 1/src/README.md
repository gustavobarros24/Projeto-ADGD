# Table of Contents

- [Environment](#environment)
- [Optimization Strategy](#optimization-strategy)
  - [v1](statsEHPC_v1_init.py) - Baseline
  - [v2](statsEHPC_v2_file_load.py) - Lazy and Parallel data loading
  - [v3](statsEHPC_v3_single_write.py) - Buffer data in memory and write parameters file once
  - [v4](statsEHPC_v4_cache_dataframe.py) - Enable DataFrame caching
  - [v5](statsEHPC_v5_column_pruning.py) - Column pruning with `select()`
  - [v6](statsEHPC_v6_reuse_filtered_dataframes.py) - Reuse intermediate filtered DataFrames
  - [v7](statsEHPC_v7_aggregation_fusion.py) - Aggregation fusion and shuffle reduction
  - [v8](statsEHPC_v8_reduce_collects.py) - Single pass aggregation and minimized `collect()`'s

# Environment

- ARM partition
- Number of nodes: 1
- Cores per node: 48
- Memory per node: 16GB
- Number of executors: 4 (12 cores per executor)
- Executor memory: 4GB
- PySpark version: 4.1.1
- Datasets are read from, and the parameters output file is written to, the
  `/projects` directory on the Parallel File System (PFS).

# Optimization Strategy

**TODO** add speedup percentage for each optimization (relative and absolute)!

## [v1](statsEHPC_v1_init.py) - Baseline

Baseline implementation without any optimizations.

Only minor code refactoring was done to improve readability and maintainability,
but the overall structure of the code and the logic of the queries remain unchanged.

List of changes:
- Measure and report total wall clock time.
- Save Spark's execution plan and event logs for analysis and comparisons.
- Update Spark configuration to be compatible with current PySpark version (4.1.1).
- Remove find spark in favor of native PySpark as a library.
- Updated command line arguments to be more versatile and assure compatibility with future optimizations.
- Moved parameters dictionary to another `.py` file to avoid cluttering the main script.

**Single Run Wall clock tmp**: 67.63s

**Wall clock (mean ± std)**: 66.10s ± 0.40s

**Shuffle Read/Write (MB)**: 0.019MB / 0.018MB

**Metrics** (3 runs):

| Run | Wall Time (s) | Stages | Tasks | Shuffle Read (MB) | Shuffle Write (MB) | Driver Time (s) | Peak Memory (MB) |
|-----|---------------|--------|-------|-------------------|--------------------|-----------------|--------------------|
| 1   | 66.34         | 77     | 135   | 0.0185            | 0.0178             | 20.971          | 3501.31            |
| 2   | 66.31         | 77     | 135   | 0.0185            | 0.0178             | 20.941          | 4113.88            |
| 3   | 65.64         | 77     | 135   | 0.0185            | 0.0178             | 20.271          | 4195.00            |

## [v2](statsEHPC_v2_file_load.py) - Lazy and Parallel data loading

Implemented lazy file loading based on command line arguments, for example
passing the flag `-m Jan` will only load the January file saving a substantial
amount of time in the first stage of the script.

Also in scenarios where multiple datasets are **truly** required, using the
`scanner.read.csv` method allows parallel file loading, where Spark can split
the work across the cluster and further reducing the loading time.

```diff
-# load all files from args.datadir starting with jobs_*
-nd = None
-for root, dirs, files in os.walk(args.datadir):
-    for f in files:
-        print(f)
-        if f.startswith('jobs'):
-            month = "_".join(f.split("_")[1:]).split(".")[0]
-            print(f"Process: {month} {args.datadir}/{f}")
-            data = sc.read.option("delimiter", "|").csv(f'{args.datadir}/{f}',
-                                                        inferSchema=True,
-                                                        header=True)
-            data = data\
-                .withColumn('EState', F.regexp_replace(F.col('State'), "CANCELLED(.*)", "CANCELLED")) \
-                .withColumn('COMPLETED', F.when(F.col('State') == 'COMPLETED', "COMPLETED").otherwise("FAILED"))
-            data = data.withColumn('Period', F.lit(month))
-            if nd is None:
-                nd = data
-            else:
-                nd = nd.union(data)
+# Read all files at once
+# Spark sc.read.csv(target_files) can take a list of files, and Spark will split them into partitions.
+# Each partition is processed in parallel by the executors.
+nd = (
+    sc.read
+    .option("delimiter", "|")
+    .option("header", True)
+    .option("inferSchema", True)
+    .csv(target_files)
+    .withColumn("Period", F.regexp_extract(F.input_file_name(), r"jobs_(\w+)\.txt$", 1))
+    .withColumn("EState", F.col("State"))
+    .withColumn('COMPLETED', F.when(F.col('State') == 'COMPLETED', "COMPLETED").otherwise("FAILED"))
+)
```

Previously, the `union` operation was performed iteratively for each file,
which can lead to significant overhead due to multiple shuffles and the creation
of intermediate DataFrames. By loading all files at once, we allow Spark to
optimize the read and union operations internally, reducing the overall
execution time and memory overhead.

**Single Run Wall clock tmp**: 53.38s

**Wall clock (mean ± std)**: 52.38s ± 0.61s

**Shuffle Read/Write (MB)**: 0.013MB / 0.012MB

**Metrics** (3 runs):

| Run | Wall Time (s) | Stages | Tasks | Shuffle Read (MB) | Shuffle Write (MB) | Driver Time (s) | Peak Memory (MB) |
|-----|---------------|--------|-------|-------------------|--------------------|-----------------|--------------------|
| 1   | 52.77         | 55     | 55    | 0.0127            | 0.0120             | 21.054          | 3326.19            |
| 2   | 52.70         | 55     | 55    | 0.0127            | 0.0120             | 20.984          | 3330.00            |
| 3   | 51.68         | 55     | 55    | 0.0127            | 0.0120             | 19.964          | 3311.13            |

## [v3](statsEHPC_v3_single_write.py) - Buffer data in memory and write parameters file once

On previous versions the data saved in the dictionary `params` was written to disk
key-value pair one at a time, which caused a considerable number of small writes,
which can be inefficient due to the overhead of opening and closing the file for
each write operation, especially on the Parallel File System (PFS) used in HPC
environments, where in case of congestion, the latency can be even higher.

In this version, we buffer the data in memory and write it to disk only once at
the end of the loop, avoiding congestion and increasing scalability of the
number of parameters for future updates.

```diff
-for k, v in params.items():
-    msg = f"\\def\\{k}{{{v}}}\n"
-    wfile.write(msg)
+buffered_params = ""
+for k, v in params.items():
+    buffered_params += f"\\def\\{k}{{{v}}}\n"
+wfile.write(buffered_params)
```

Furthermore, we also reduce the number of `print()` and `show()` statements
reducing the IO overhead, improving the overall performance of the script.

**Single Run Wall clock tmp**: 51.51s

**Wall clock (mean ± std)**: 50.07s ± 0.31s

**Shuffle Read/Write (MB)**: 0.006MB / 0.006MB

**Metrics** (3 runs):

| Run | Wall Time (s) | Stages | Tasks | Shuffle Read (MB) | Shuffle Write (MB) | Driver Time (s) | Peak Memory (MB) |
|-----|---------------|--------|-------|-------------------|--------------------|-----------------|--------------------|
| 1   | 50.32         | 50     | 50    | 0.0063            | 0.0055             | 20.779          | 3758.13            |
| 2   | 50.18         | 50     | 50    | 0.0063            | 0.0055             | 20.639          | 3626.50            |
| 3   | 49.72         | 50     | 50    | 0.0063            | 0.0055             | 20.179          | 3655.81            |

## [v4](statsEHPC_v4_cache_dataframe.py) - Enable DataFrame caching

This version introduces caching of the `nd` DataFrame to eliminate redundant
recomputation of its transformation lineage across multiple actions.

The DataFrame `nd` is reused in several downstream aggregations and filters.
Without caching, Spark would recompute the entire DAG for each action due to
its lazy evaluation model. This leads to repeated scans, redundant shuffles,
and unnecessary CPU and I/O overhead. Persisting `nd` ensures that the
transformation pipeline is executed only once, and its results are reused
across all subsequent operations.

### Why `StorageLevel.MEMORY_AND_DISK`?

```py
nd = nd.persist(StorageLevel.MEMORY_AND_DISK)
```

We explicitly choose `MEMORY_AND_DISK` over `MEMORY_ONLY`, even though the
current dataset is relatively small.

The key reason is **robustness under memory pressure**. With `MEMORY_ONLY`,
if the cached DataFrame does not fully fit in memory, Spark will silently
evict partitions and recompute them on demand. This reintroduces the exact
performance penalty caching is meant to avoid, often in a non-obvious way.

By contrast, `MEMORY_AND_DISK` guarantees that once a partition is computed,
it is retained, either in memory or spilled to disk. This provides:

- **Predictable performance:** avoids recomputation regardless of memory pressure
- **Scalability:** supports future increases in dataset size without requiring code changes
- **Operational safety:** prevents performance degradation caused by cache eviction

Even if disk access is slower than memory, it is still significantly cheaper
than recomputing a complex lineage involving filters, projections, and
potential shuffles.

### Materialization strategy

```py
nd.count()
```

Calling `count()` immediately after `persist()` forces Spark to execute the
entire transformation DAG and populate the cache.

This is necessary because Spark uses lazy evaluation: without an action, the
cache declaration is only a hint and no data is actually materialized.

`count()` is a deliberate choice because:

- **requires a full scan** of the DataFrame, ensuring that all partitions are
computed and cached
- has **minimal computational overhead**: it does not require maintaining large
intermediate structures or performing expensive aggregations
- is **semantically neutral**: it does not interfere with downstream logic, as
its result is not used

This makes it a reliable and low-risk way to guarantee that caching is
fully realized before subsequent actions.

### Why not avoid materialization?

If we omit this step, the first "real" action would implicitly trigger both
the computation and caching of `nd`. While this may seem efficient, it has
two drawbacks:

- The first action pays the full cost of the lineage execution, increasing its
latency unpredictably
- It becomes harder to isolate and measure the cost/benefit of caching itself

By materializing explicitly, we **separate concerns**:
- one phase for building and caching the dataset
- another phase for executing analytical queries on top of it

This leads to more predictable and analyzable performance behavior.

This version makes the design decisions explicit: caching is not just an
optimization, but a control mechanism over Spark’s execution model, and
`MEMORY_AND_DISK` is chosen to preserve that control under uncertainty.

**Single Run Wall clock tmp**: 43.15s

**Wall clock (mean ± std)**: 42.12s ± 0.03s

**Shuffle Read/Write (MB)**: 0.006MB / 0.006MB

**Metrics** (3 runs):

| Run | Wall Time (s) | Stages | Tasks | Shuffle Read (MB) | Shuffle Write (MB) | Driver Time (s) | Peak Memory (MB) |
|-----|---------------|--------|-------|-------------------|--------------------|-----------------|--------------------|
| 1   | 42.14         | 53     | 53    | 0.0063            | 0.0056             | 20.704          | 3684.94            |
| 2   | 42.08         | 53     | 53    | 0.0063            | 0.0056             | 20.644          | 3679.63            |
| 3   | 42.14         | 53     | 53    | 0.0063            | 0.0056             | 20.704          | 3693.50            |

## [v5](statsEHPC_v5_column_pruning.py) - Column pruning with `select()`

Punning columns to reduce the amount of data loaded and processed by Spark.

In this version, we explicitly select only the necessary columns from the input
files right after reading them, using the `select()` method. This is a key
optimization technique in Spark, as it enables the engine to read and propagate
only the required data through the execution plan, reducing I/O, memory
footprint, and serialization overhead.

Additionally, the "EState" column was removed since it was not used in any
subsequent transformations or aggregations, and its value could be fully derived
from the existing "State" column, which is already part of the selected schema.

```diff
nd = (
  sc.read
  .option("delimiter", "|")
  .option("header", True)
  .option("inferSchema", True)
  .csv(target_files)
+ .select("Partition", "Account", "State", "AllocCPUS", "AllocTRES", "ElapsedRaw", "NNodes")
  .withColumn("Period", F.regexp_extract(F.input_file_name(), r"jobs_(\w+)\.txt$", 1))
- .withColumn("EState", F.col("State"))
  .withColumn('COMPLETED', F.when(F.col('State') == 'COMPLETED', "COMPLETED").otherwise("FAILED"))
)
```

By constraining the DataFrame schema to only the required fields at the source,
Spark can avoid unnecessary column materialization across the DAG, resulting in
more efficient execution and reduced resource utilization.

**Single Run Wall clock tmp**: 41.89s

**Wall clock (mean ± std)**: 40.11s ± 0.49s

**Shuffle Read/Write (MB)**: 0.006MB / 0.005MB

**Metrics** (3 runs):

| Run | Wall Time (s) | Stages | Tasks | Shuffle Read (MB) | Shuffle Write (MB) | Driver Time (s) | Peak Memory (MB) |
|-----|---------------|--------|-------|-------------------|--------------------|-----------------|--------------------|
| 1   | 40.60         | 53     | 53    | 0.0059            | 0.0052             | 21.005          | 3261.88            |
| 2   | 39.62         | 53     | 53    | 0.0059            | 0.0052             | 20.025          | 3402.38            |
| 3   | 40.12         | 53     | 53    | 0.0059            | 0.0052             | 20.525          | 3412.31            |

## [v6](statsEHPC_v6_reuse_filtered_dataframes.py) - Reuse intermediate filtered DataFrames

This version reduces redundant computations inside the main loop by reusing
intermediate filtered DataFrames.

Instead of reapplying identical filter predicates for each aggregation, the
filtering is structured hierarchically so that each transformation builds on the
previous one. This reduces duplicated work and leads to a more compact and
optimized execution plan. Since Spark can combine and push down predicates, this
approach also improves scan efficiency and minimizes repeated evaluation of the
same conditions.

Implementation, at the start of the main loop:

```py
# Reuse filtered DataFrames to avoid redundant filtering in the loop
nd_period    = nd.filter(F.col('Period').isin(months))
nd_non_local = nd_period.filter(F.col("Agency") != 'LOCAL')
nd_ehpc      = nd_non_local.filter(F.col("Agency") == 'EHPC')
```

Here, `nd_period` is reused to derive `nd_non_local`, which is then reused to
derive `nd_ehpc`. This eliminates repeated filtering over the base DataFrame,
reducing unnecessary scans and predicate re-evaluation, while preserving a
composable structure for downstream aggregations.

**Single Run Wall clock tmp**: 40.10s

**Wall clock (mean ± std)**: 39.12s ± 0.20s

**Shuffle Read/Write (MB)**: 0.005MB / 0.005MB

**Metrics** (3 runs):

| Run | Wall Time (s) | Stages | Tasks | Shuffle Read (MB) | Shuffle Write (MB) | Driver Time (s) | Peak Memory (MB) |
|-----|---------------|--------|-------|-------------------|--------------------|-----------------|--------------------|
| 1   | 39.35         | 47     | 47    | 0.0048            | 0.0048             | 20.804          | 3291.38            |
| 2   | 39.01         | 47     | 47    | 0.0048            | 0.0048             | 20.464          | 3387.50            |
| 3   | 38.99         | 47     | 47    | 0.0048            | 0.0048             | 20.444          | 3437.81            |

## [v7](statsEHPC_v7_aggregation_fusion.py) - Aggregation fusion and shuffle reduction

This version optimizes aggregation patterns by consolidating multiple independent
`groupBy` operations into single aggregated passes. The goal is to further
eliminate redundant computations, reduce shuffle operations, and minimize the
number of Spark jobs triggered.

### Key improvements

- **Aggregation fusion**: Combined multiple aggregations (`count`, `sum`) over
  the same grouping key into a single `.agg()` call.
- **Elimination of iterative aggregations**: Replaced per-cluster loops that
  triggered repeated Spark jobs with a single grouped aggregation.
- **Shuffle reduction**: Avoided executing the same `groupBy` multiple times,
  reducing wide dependencies.
- **Driver-side efficiency**: Reduced the number of `.collect()` calls and
  simplified result handling.
- **Redundant grouping removal**: Dropped unnecessary grouping columns when the
  DataFrame is already filtered (e.g., removed `Agency` from `groupBy` after
  filtering `Agency == 'EHPC'`).
- **Branching elimination**: Replaced row-wise conditional logic with a mapping
  approach for deterministic and cleaner result extraction.
- **Improved DAG compactness**: Fused aggregations lead to fewer stages and
  better Catalyst optimization opportunities.

### 1. Merge aggregations for `nd_non_local`

Replaced multiple `groupBy("cluster")` operations (for `count` and `sum`) with a
single aggregation:

```diff
-# Consumed hours per cluster (ignoring local)
-for c in cl:
-    hours[c] = (
-        nd_non_local
-        .groupby("cluster")
-        .sum()
-        .filter(F.col("cluster") == c)
-        .collect()[0]['sum(totalJobSeconds)']
-    )
-
-# Total jobs per cluster (ignoring local)
-for row in (
-    nd_non_local
-    .groupby("cluster")
-    .count()
-    .collect()
-):
-    jobs[row['cluster']] = row['count']
+# Consumed hours per cluster and total jobs per cluster (ignoring local)
+cluster_stats_rows = (
+    nd_non_local
+    .groupBy("cluster")
+    .agg(
+        F.count("*").alias("jobs"),
+        F.sum("totalJobSeconds").alias("totalSeconds")
+    )
+    .collect()
+)
+
+hours = {r["cluster"]: r["totalSeconds"] for r in cluster_stats_rows}
+jobs  = {r["cluster"]: r["jobs"]         for r in cluster_stats_rows}

# Store results (hours and jobs) in params dictionary
for k, v in hours.items():
    params[f"{k.lower()}usedhours{tag}"] = v / 3600

for k, v in jobs.items():
    params[f"{k.lower()}Jobs{tag}"] = v
```

This removes repeated full-data aggregations, executes a single shuffle, and
avoids per-key filtering on the driver side.

### 2. Merge EHPC aggregations and remove redundant grouping

Since `nd_ehpc` is already filtered with `Agency == 'EHPC'`, grouping by
`Agency` is redundant. Aggregations are merged into a single pass:

```diff
-# EHPC jobs count per cluster
-for row in (
-    nd_ehpc
-    .groupby(['Agency', 'cluster'])
-    .count()
-    .collect()
-):
-    params[f"{row.cluster.lower()}JobsEuroHPC{tag}"] = row['count']
-
-# EHPC consumed hours per cluster
-for row in (
-    nd_ehpc
-    .groupby(['Agency', 'cluster'])
-    .sum()
-    .collect()
-):
-    params[f"{row.cluster.lower()}usedhoursEuroHPC{tag}"] = \
-        row['sum(totalJobSeconds)'] / 3600
+# EHPC jobs count and consumed hours per cluster
+ehpc_rows = (
+    nd_ehpc
+    .groupBy("cluster")
+    .agg(
+        F.count("*").alias("jobs"),
+        F.sum("totalJobSeconds").alias("totalSeconds")
+    )
+    .collect()
+)
+
+for r in ehpc_rows:
+    params[f"{r.cluster.lower()}JobsEuroHPC{tag}"]      = r["jobs"]
+    params[f"{r.cluster.lower()}usedhoursEuroHPC{tag}"] = r["totalSeconds"] / 3600
```

This reduces both shuffle cost and grouping overhead while preserving the same
semantics.

### 3. Eliminate branching in completed/failed aggregation

Although the original aggregation was already efficient, row-wise branching was
removed in favor of a mapping-based approach:

```diff
# Completed / Failed jobs per cluster
-for row in (
-    nd_period
-    .groupby('COMPLETED', 'cluster')
-    .count()
-    .collect()
-):
-    if row.COMPLETED == 'COMPLETED':
-        params[f"{row.cluster.lower()}CompletedJobs{tag}"] = row['count']
-    else:
-        params[f"{row.cluster.lower()}FailedJobs{tag}"] = row['count']
+cf_rows = (
+    nd_period
+    .groupBy("cluster", "COMPLETED")
+    .count()
+    .collect()
+)
+
+cf_map = {(r["cluster"], r["COMPLETED"]): r["count"] for r in cf_rows}
+
+for c in cl:
+    params[f"{c.lower()}CompletedJobs{tag}"] = cf_map.get((c, "COMPLETED"), 0)
+    params[f"{c.lower()}FailedJobs{tag}"]    = cf_map.get((c, "FAILED"), 0)
```

This removes conditional logic inside loops and ensures consistent and robust
handling of missing `(cluster, state)` combinations.

### Impact

- Fewer Spark jobs triggered per loop iteration
- Significant reduction in shuffle operations ((fewer wide transformations))
- Elimination of redundant full-data aggregations
- More compact DAG with improved Catalyst optimization opportunities
- Cleaner and more maintainable driver-side logic

These changes are particularly relevant in HPC environments, where shuffle
operations incur high network and I/O costs across nodes.

**Single Run Wall clock tmp**: 34.44s

**Wall clock (mean ± std)**: 33.82s ± 0.39s

**Shuffle Read/Write (MB)**: 0.003MB / 0.003MB

**Metrics** (3 runs):

| Run | Wall Time (s) | Stages | Tasks | Shuffle Read (MB) | Shuffle Write (MB) | Driver Time (s) | Peak Memory (MB) |
|-----|---------------|--------|-------|-------------------|--------------------|-----------------|--------------------|
| 1   | 33.37         | 23     | 23    | 0.0030            | 0.0030             | 20.218          | 3405.69            |
| 2   | 34.04         | 23     | 23    | 0.0030            | 0.0030             | 20.888          | 3399.44            |
| 3   | 34.04         | 23     | 23    | 0.0030            | 0.0030             | 20.888          | 3318.38            |

## [v8](statsEHPC_v8_reduce_collects.py) - Single pass aggregation and minimized `collect()`'s

Minimize driver–executor communication by consolidating all
required metrics into a **single aggregated DataFrame per iteration**, followed
by a **single** `collect()`. The objective is to reduce the number of Spark
jobs, limit synchronization points, and avoid repeated materialization of
intermediate results.

Instead of triggering multiple actions (`collect()`) for different metrics, all
aggregations are computed in one pass using conditional aggregation. This allows
Spark to execute a single shuffle and produce all required metrics together.

### Key improvements

- **Single-pass aggregation:** All metrics (completed/failed jobs, non-local
  usage, EHPC usage) are computed in one `.groupBy().agg()` call.
- **Minimized actions:** Exactly one `collect()` per iteration, reducing
  driver–executor round-trips.
- **Shuffle consolidation:** Only one wide transformation is executed per loop.
- **Predicate fusion:** Conditional logic is pushed into aggregations, allowing
  Spark to optimize execution via Catalyst.
- **Driver-side efficiency:** Results are processed in-memory with a single
  pass.

### Implementation

```py
for tag, months in tag_month.items():
    # Single aggregation: compute all metrics in one pass
    agg_df = (
        nd
        .filter(F.col('Period').isin(months))
        .withColumn("isNonLocal", F.col("Agency") != "LOCAL")
        .withColumn("isEHPC", F.col("Agency") == "EHPC")
        .groupBy("cluster")
        .agg(
            # Completed / Failed jobs counts
            F.sum(F.when(F.col("COMPLETED") == "COMPLETED", 1).otherwise(0)).alias("completedJobs"),
            F.sum(F.when(F.col("COMPLETED") == "FAILED", 1).otherwise(0)).alias("failedJobs"),

            # Total jobs and total seconds for non-local
            F.sum(F.when(F.col("isNonLocal"), 1).otherwise(0)).alias("jobsNonLocal"),
            F.sum(F.when(F.col("isNonLocal"), F.col("totalJobSeconds")).otherwise(0)).alias("secondsNonLocal"),

            # EHPC jobs count and seconds
            F.sum(F.when(F.col("isEHPC"), 1).otherwise(0)).alias("jobsEHPC"),
            F.sum(F.when(F.col("isEHPC"), F.col("totalJobSeconds")).otherwise(0)).alias("secondsEHPC")
        )
        .collect()
    )

    # Update params dictionary from collected results
    for row in agg_df:
        cluster = row["cluster"]

        # Completed / Failed
        params[f"{cluster.lower()}CompletedJobs{tag}"] = row["completedJobs"]
        params[f"{cluster.lower()}FailedJobs{tag}"]    = row["failedJobs"]

        # Non-local jobs / hours
        params[f"{cluster.lower()}Jobs{tag}"]      = row["jobsNonLocal"]
        params[f"{cluster.lower()}usedhours{tag}"] = row["secondsNonLocal"] / 3600

        # EHPC jobs / hours
        params[f"{cluster.lower()}JobsEuroHPC{tag}"]      = row["jobsEHPC"]
        params[f"{cluster.lower()}usedhoursEuroHPC{tag}"] = row["secondsEHPC"] / 3600
```

### Trade-offs

#### Pros

- Reduces latency by minimizing driver–executor communication.
- Eliminates redundant Spark actions and job scheduling overhead.
- Consolidates multiple aggregations into a single optimized execution plan.
- Particularly effective in distributed/HPC environments where shuffle and
synchronization costs are significant.

#### Cons

- Aggregated results must fit in driver memory.
- Less modular than separate aggregations, which may reduce flexibility if
metrics need to be reused independently.

### Impact

- One Spark job per iteration instead of multiple
- Single shuffle stage for all metrics
- Reduced execution time due to fewer synchronization points
- Improved scalability by limiting driver interaction frequency

**Single Run Wall clock tmp**: 32.84s

**Wall clock (mean ± std)**: 31.59s ± 0.06s

**Shuffle Read/Write (MB)**: 0.001MB / 0.001MB

**Metrics** (3 runs):

| Run | Wall Time (s) | Stages | Tasks | Shuffle Read (MB) | Shuffle Write (MB) | Driver Time (s) | Peak Memory (MB) |
|-----|---------------|--------|-------|-------------------|--------------------|-----------------|--------------------|
| 1   | 31.54         | 11     | 11    | 0.0009            | 0.0009             | 20.397          | 3220.69            |
| 2   | 31.57         | 11     | 11    | 0.0009            | 0.0009             | 20.427          | 3427.94            |
| 3   | 31.66         | 11     | 11    | 0.0009            | 0.0009             | 20.517          | 3419.31            |

# Benchmarking

All benchmarks were run 3 times per configuration. Values below are mean ± std.

## Summary Table

| Config   | Wall Time (s)    | Stages | Tasks | Shuffle Read (MB) | Shuffle Write (MB) | Peak Memory (MB)    | Driver Time (s)    | Speedup |
|----------|-----------------|--------|-------|-------------------|--------------------|--------------------|-------------------|---------:|
| baseline | 66.10 ± 0.40    | 77     | 135   | 0.0185 ± 0.00     | 0.0178 ± 0.00     | 3936.73 ± 379.26   | 20.73 ± 0.40      | 1.00x   |
| v2       | 52.38 ± 0.61    | 55     | 55    | 0.0127 ± 0.00     | 0.0120 ± 0.00     | 3322.44 ± 9.98     | 20.67 ± 0.61      | 1.26x   |
| v3       | 50.07 ± 0.31    | 50     | 50    | 0.0063 ± 0.00     | 0.0055 ± 0.00     | 3680.15 ± 69.10    | 20.53 ± 0.31      | 1.32x   |
| v4       | 42.12 ± 0.03    | 53     | 53    | 0.0063 ± 0.00     | 0.0056 ± 0.00     | 3686.02 ± 7.00     | 20.68 ± 0.03      | 1.57x   |
| v5       | 40.11 ± 0.49    | 53     | 53    | 0.0059 ± 0.00     | 0.0052 ± 0.00     | 3358.85 ± 84.13    | 20.52 ± 0.49      | 1.65x   |
| v6       | 39.12 ± 0.20    | 47     | 47    | 0.0048 ± 0.00     | 0.0048 ± 0.00     | 3372.23 ± 74.40    | 20.57 ± 0.20      | 1.69x   |
| v7       | 33.82 ± 0.39    | 23     | 23    | 0.0030 ± 0.00     | 0.0030 ± 0.00     | 3374.50 ± 48.71    | 20.66 ± 0.39      | 1.95x   |
| final    | 31.59 ± 0.06    | 11     | 11    | 0.0009 ± 0.00     | 0.0009 ± 0.00     | 3355.98 ± 117.25   | 20.45 ± 0.06      | 2.09x   |

