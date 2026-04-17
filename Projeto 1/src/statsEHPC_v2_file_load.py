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
    EVENTLOGDIR = "spark-events"  # /projects/F202500010HPCVLABUMINHO/<OURFOLDER>/spark-events
    OUTDIR      = "output"        # /projects/F202500010HPCVLABUMINHO/<OURFOLDER>/DATA

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

    today = datetime.now().date()
    year = today.year
    month_int = today.month - 2
    month = list_of_months_abr[month_int]
    syear = date(year, 1, 1)

    if args.month:
        month_int = list_of_months_abr.index(args.month)
        month = list_of_months_abr[month_int]

    if args.year:
        year = int(args.year)
        syear = date(year, 1, 1)

    if args.start:
        syear = datetime.strptime(args.start, "%Y-%m-%d").date()

    params['reportMonth'] = list_of_Months[month_int]
    params['reportYear']  = year

    smonth = date(year, month_int + 1, 1)
    emonthd = smonth + relativedelta(months=1) + relativedelta(days=-1)
    emonth = smonth + relativedelta(months=1)
    tmonth = emonth - relativedelta(months=min(3, month_int + 1))

    params['reportPeriod'] = f"{smonth.strftime('%d/%m/%Y')} - {emonthd.strftime('%d/%m/%Y')}"
    params['reportPeriodTrimester'] = f"{tmonth.strftime('%d/%m/%Y')} - {emonthd.strftime('%d/%m/%Y')}"
    params['reportPeriodYear'] = f"{syear.strftime('%d/%m/%Y')} - {emonthd.strftime('%d/%m/%Y')}"

    params['ndays'] = (emonth - smonth).days
    params['ndaysTrimester'] = (emonth - tmonth).days
    params['ndaysYear'] = (emonth - syear).days

    tag_month = {
        '': [month],
        'Trimester': list_of_months_abr[max(0, month_int - 2):month_int + 1],
        'Year': list_of_months_abr[:month_int + 1]
    }

    # Ensure output directories exist
    os.makedirs(args.outdir, exist_ok=True)
    os.makedirs(args.eventlogdir, exist_ok=True)

    wfile = open(f"{args.outdir}/{args.outfile}", "w+")

    sc = (
        SparkSession.builder
        .config("spark.executor.memory", "4g")
        .config("spark.executor.instances", "4")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", f"file://{os.path.abspath(args.eventlogdir)}")
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
        .withColumn("Period", F.regexp_extract(F.input_file_name(), r"jobs_(\w+)\.txt$", 1))
        .withColumn("EState", F.col("State"))
        .withColumn('COMPLETED', F.when(F.col('State') == 'COMPLETED', "COMPLETED").otherwise("FAILED"))
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

    #                   F.regexp_extract(F.col("AllocTRES"), r"gres/gpu=(\d+)", 1))
    # nd = nd.withColumn("totalJobSeconds",
    #                   (F.col('ElapsedRaw') ) * F.col('NNodes')
    #                   )
    # nd.show()

    # Save execution plan to file
    save_spark_plan(nd, f"{args.outdir}/execution_plan.txt")

    cl = ['ARM', 'AMD', 'GPU']
    nd.groupby('EState').count().show()
    print(f"1. {nd.count()}")
    nd.show()

    for tag, months in tag_month.items():
        print(f"{tag} {months}")
        hours = dict()
        jobs = dict()
        completed = nd.filter(F.col('Period').isin(months)).groupby('COMPLETED', 'cluster').count().collect()
        msg = ''
        for row in completed:
            print(f"ROW: {row}")
            if row.COMPLETED == 'COMPLETED':
                params[f"{row.cluster.lower()}CompletedJobs{tag}"] = row.asDict()['count']
                msg = f"\\def\\{row.cluster.lower()}CompletedJobs{tag}{{{row.asDict()['count']}}}\n"
            else:
                params[f"{row.cluster.lower()}FailedJobs{tag}"] = row.asDict()['count']
                msg = f"\\def\\{row.cluster.lower()}FailedJobs{tag}{{{row.asDict()['count']}}}\n"
            print(f"MSG: {tag} {msg}")
        # ignoring local consumed hours
        for c in cl:
            # hours[c] = nd.filter(F.col('Period').isin(months)).groupby("cluster").sum().filter(F.col("cluster") == c).collect()[0].asDict()['sum(ElapsedRaw)']
            hours[c] = (
                nd
                .filter(F.col("Agency") != 'LOCAL')
                .filter(F.col('Period').isin(months))
                .groupby("cluster").sum().filter(F.col("cluster") == c)
                .collect()[0].asDict()['sum(totalJobSeconds)']
            )
            print(f" {months} HOURS {c} {hours[c]}")
        # ignoring local consumed hours
        for row in (nd.filter(F.col("Agency") != 'LOCAL')
                    .filter(F.col('Period').isin(months))
                    .groupby("cluster")
                    .count()
                    .collect()):
            print(f"ROW jobs: {row}")
            r = row.asDict()
            jobs[r['cluster']] = r['count']

        print(f"JOBS: {jobs}")

        for k, v in hours.items():
            params[f"{k.lower()}usedhours{tag}"] = v / 3600
            msg = f"\\def\\{k.lower()}usedhours{tag}{{{v / 3600}}}\n"
            print(f"HOURS {tag} {msg}")

        for k, v in jobs.items():
            params[f"{k.lower()}Jobs{tag}"] = v
            msg = f"\\def\\{k.lower()}Jobs{tag}{{{v}}}\n"
            print(f"JOBS {tag} {msg}")

        for row in (nd
                    .filter(F.col('Period').isin(months))
                    .groupby(['Agency', 'cluster'])
                    .count()
                    .orderBy('Agency')
                    .filter(F.col("Agency") == 'EHPC')
                    .collect()):
            params[f"{row.cluster.lower()}JobsEuroHPC{tag}"] = row.asDict()['count']
            msg = f"\\def\\{row.cluster.lower()}JobsEuroHPC{tag}{{{row.asDict()['count']}}}\n"
            print(msg)

        rows = (nd
                .filter(F.col("Agency") == 'EHPC')
                .filter(F.col('Period').isin(months))
                .groupby(['Agency', 'cluster'])
                .sum()
                .collect())

        for row in rows:
            # print(f"EHPC {row} --> {row.cluster.lower()}usedhoursEuroHPC{tag}")
            params[f"{row.cluster.lower()}usedhoursEuroHPC{tag}"] = row.asDict()['sum(totalJobSeconds)'] / 3600
            msg = f"\\def\\{row.cluster.lower()}usedhoursEuroHPC{tag}{{{row.asDict()['sum(totalJobSeconds)'] / 3600}}}\n"
            print(msg)

    # Write all params to output file
    for k, v in params.items():
        msg = f"\\def\\{k}{{{v}}}\n"
        wfile.write(msg)
    wfile.close()

    # Measure total execution time
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Execution time: {execution_time:.2f} seconds")
