#!/usr/bin/env python3
# coding: utf-8

from params import params

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import argparse
import calendar
import os
import time
import sys
from io import StringIO
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
from pyspark.storagelevel import StorageLevel


def save_spark_plan(nd, path):
    """
    Save the Spark execution plan to a file.

    Captures explain() output and writes it to the specified path.
    """
    buf = StringIO()
    old_stdout = sys.stdout
    sys.stdout = buf
    nd.explain(True)
    sys.stdout = old_stdout
    with open(path, "w") as f:
        f.write(buf.getvalue())


if __name__ == '__main__':
    start_time = time.time()

    DATADIR     = "data"          # /projects/F202500010HPCVLABUMINHO/DataSets/Reports/2025
    EVENTLOGDIR = "spark-events"  # /projects/F202500010HPCVLABUMINHO/<FOLDER>/spark-events
    OUTDIR      = "output"        # /projects/F202500010HPCVLABUMINHO/<FOLDER>/DATA

    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--month",       nargs="?", help="month")
    parser.add_argument("-y", "--year",        nargs="?", help="year")
    parser.add_argument("-s", "--start",       nargs="?", help="start day")
    parser.add_argument("-d", "--datadir",     nargs="?", help="datadir",     default=DATADIR)
    parser.add_argument("-e", "--eventlogdir", nargs="?", help="eventlogdir", default=EVENTLOGDIR)
    parser.add_argument("-o", "--outdir",      nargs="?", help="outdir",      default=OUTDIR)
    parser.add_argument("-O", "--outfile",     nargs="?", help="outfile",     default="params.tex")
    args = parser.parse_args()

    list_of_Months     = list(calendar.month_name)[1:]
    list_of_months_abr = list(calendar.month_abbr)[1:]

    today     = datetime.now().date()
    year      = today.year
    month_int = today.month - 2
    month     = list_of_months_abr[month_int]
    syear     = date(year, 1, 1)

    if args.month:
        month_int = list_of_months_abr.index(args.month)
        month     = list_of_months_abr[month_int]

    if args.year:
        year  = int(args.year)
        syear = date(year, 1, 1)

    if args.start:
        syear = datetime.strptime(args.start, "%Y-%m-%d").date()

    params['reportMonth'] = list_of_Months[month_int]
    params['reportYear']  = year

    smonth  = date(year, month_int + 1, 1)
    emonthd = smonth + relativedelta(months=1) + relativedelta(days=-1)
    emonth  = smonth + relativedelta(months=1)
    tmonth  = emonth - relativedelta(months=min(3, month_int + 1))

    params['reportPeriod']          = f"{smonth.strftime('%d/%m/%Y')} - {emonthd.strftime('%d/%m/%Y')}"
    params['reportPeriodTrimester'] = f"{tmonth.strftime('%d/%m/%Y')} - {emonthd.strftime('%d/%m/%Y')}"
    params['reportPeriodYear']      = f"{syear.strftime('%d/%m/%Y')} - {emonthd.strftime('%d/%m/%Y')}"

    params['ndays']          = (emonth - smonth).days
    params['ndaysTrimester'] = (emonth - tmonth).days
    params['ndaysYear']      = (emonth - syear).days

    tag_month = {
        '': [month],
        'Trimester': list_of_months_abr[max(0, month_int - 2):month_int + 1],
        'Year': list_of_months_abr[:month_int + 1]
    }

    # Ensure output directories exist
    os.makedirs(args.outdir, exist_ok=True)
    os.makedirs(args.eventlogdir, exist_ok=True)

    sc = (
        SparkSession.builder
        .config("spark.executor.memory",    "4g")
        .config("spark.executor.instances", "4")
        .config("spark.eventLog.enabled",   "true")
        .config("spark.eventLog.dir",       f"file://{os.path.abspath(args.eventlogdir)}")
        .getOrCreate()
    )

    # Optimized file loading based on CLI arguments
    months_to_load = set()

    # Only load the month(s) requested via -m, otherwise all months
    if args.month:
        months_to_load.add(args.month)
    else:
        # load months needed for all tags
        for lst in tag_month.values():
            if lst:
                months_to_load.update(lst)

    # Build file paths only for the selected months
    target_files = [
        os.path.join(args.datadir, f"jobs_{m}.txt")
        for m in months_to_load
        if os.path.exists(os.path.join(args.datadir, f"jobs_{m}.txt"))
    ]

    if not target_files:
        raise FileNotFoundError(f"No job files found for months: {months_to_load}")

    # Read all files at once
    # Spark sc.read.csv(target_files) can take a list of files, and Spark will split them into partitions.
    # Each partition is processed in parallel by the executors.
    nd = (
        sc.read
        .option("delimiter", "|")
        .option("header", True)
        .option("inferSchema", True)
        .csv(target_files)
        .select("Partition", "Account", "State", "AllocCPUS", "AllocTRES", "ElapsedRaw", "NNodes")
        .withColumn("Period", F.regexp_extract(F.input_file_name(), r"jobs_(\w+)\.txt$", 1))
        .withColumn("COMPLETED", F.when(F.col("State") == "COMPLETED", "COMPLETED").otherwise("FAILED"))
    )

    # nd.describe()
    # adicionar coluna cluster com valores ARM, AMD, GPU
    nd = nd.withColumn(
        "cluster",
        F.when(
            F.col('Partition').contains("arm"), "ARM"
        ).otherwise(
            F.when(
                F.col('Partition').contains("a100"), "GPU"
            ).otherwise("AMD")
        )
    )

    # Adicionar coluna Agency com valores FCT, EHPC, LOCAL
    nd = nd.withColumn(
        "Agency",
        F.when(
            F.col('Account').startswith("f"), "FCT"
        ).otherwise(
            F.when(
                F.col('Account').startswith("ee"), "EHPC"
            ).otherwise("LOCAL")
        )
    )

    # Adicionar coluna NNodes com o numero de nodos alocados por causa de nós GPU não serem exclusivos
    nd = nd.withColumn(
        "OldVNodes",
        F.when(
            F.col("Partition").contains("a100"),
            F.when(
                F.col('AllocCPUS') % 32 == 0,
                ((F.col('AllocCPUS') / 32).cast("int"))
            ).otherwise(
                ((F.col('AllocCPUS') / 32).cast("int") + 1)
            )
        ).otherwise(F.col("NNodes")))

    # Forma do calcular os nós usados nos jobs com GPU
    nd = nd.withColumn(
        "VNodes",
        F.when(
            F.col("Partition").contains("a100"),
            F.when(
                F.col("AllocTRES").isNull(),
                F.col("NNodes")
            ).otherwise(
                F.when(F.col("AllocTRES").rlike(r"gres/gpu=(\d+)"),
                       F.regexp_extract(F.col("AllocTRES"), r"gres/gpu=(\d+)", 1)
                       ).otherwise(
                    F.col("NNodes") * 4  # antes de ter este valor usava todo o nó
                )
            )
        ).otherwise(
            F.col("NNodes")
        )
    )

    # Adicionar coluna totalJobSeconds = ElapsedRaw * NNodes
    nd = nd.withColumn(
        "totalJobSeconds",
        (F.col('ElapsedRaw')) * F.col('VNodes')
    )

    # Cache dataframe is reused multiple times
    nd = nd.persist(StorageLevel.MEMORY_AND_DISK)

    # Force materialization to cache the dataframe and measure time taken for caching
    nd.count()

    # Save execution plan to file
    save_spark_plan(nd, f"{args.outdir}/execution_plan.txt")

    cl = ['ARM', 'AMD', 'GPU']
    # nd.groupby('State').count().show()
    # nd.show()

    for tag, months in tag_month.items():
        # Single aggregation: Completed/Failed jobs per cluster, total jobs and
        # total seconds per cluster, and EHPC-specific counts and seconds
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

        # Update params dictionary
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

    # Write all params to output file
    wfile = open(f"{args.outdir}/{args.outfile}", "w+")
    buffered_params = ""
    for k, v in params.items():
        buffered_params += f"\\def\\{k}{{{v}}}\n"
    wfile.write(buffered_params)
    wfile.close()

    # Measure total execution time
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Execution time: {execution_time:.2f} seconds")
