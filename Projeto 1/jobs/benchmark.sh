#!/bin/bash
#SBATCH --job-name=benchmark-spark-ehpc
#SBATCH --partition=dev-arm
#SBATCH --account=f202500010hpcvlabuminhoa
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=48  # 12 cores per executor, 4 executors total
#SBATCH --mem=16G
#SBATCH --time=00:10:00
#SBATCH --output=jobs/out/benchmark_%j.out

# Benchmark runner for statsEHPC python script.
# This script only EXECUTES the runs and collects dstat resource data.
# Metric extraction (stages, tasks, shuffle, driver time, peak memory)
# is handled by metrics_parser.py, which reads the Spark event logs
# and dstat CSVs produced here.

if [ "$#" -ne 2 ]; then
    echo "Usage: ./benchmark.sh <executable_name> <spark_events_dir>"
    exit 1
fi

# Variables
EXEC="$1"
SPARK_EVENTS_DIR="$2"
ARGS="-m Jan"
ITERATIONS=3
PYTHON2_EXEC=""
DSTAT_EXEC="dstat/dstat"
DSTAT_FIX_FILE="/tmp/fix_types.py"
DSTAT_OUT_DIR="output"

# Load python2 for dstat execution
if ! module load "Python/2.7.16-GCCcore-8.3.0" &>/dev/null; then
    echo "Warning: Failed to load Python 2 module for dstat."
else
    PYTHON2_EXEC=$(readlink -f "$(which python)")
    echo "Using Python 2 at ${PYTHON2_EXEC} for dstat execution."
    pip2 install --user six &>/dev/null \
        && echo "Successfully installed module six for Python 2." \
        || echo "Warning: Failed to install six for Python 2. Dstat may not work properly."
    echo "import types" > ${DSTAT_FIX_FILE}
fi

# Ensure dstat output directory exists
mkdir -p "$DSTAT_OUT_DIR"

# Load required modules
if command -v module &>/dev/null; then
    source modules.sh
else
    echo "Warning: Please load the required modules manually."
fi

# Load python virtual environment
if [ -d "venv" ]; then
    source venv/bin/activate
else
    echo "Error: Virtual environment not found." \
         "Please set up the virtual environment and install dependencies."
    exit 1
fi

# Dry run to warm up the JVM (discarded, not reported)
echo -n "Performing dry run to warm up JVM... "
#shellcheck disable=SC2086
warm_up_time=$(
    $EXEC $ARGS 2>/dev/null \
        | grep --color=never -oP '(?<=Execution time: )[0-9]+\.[0-9]+'
    )
echo "Completed in ${warm_up_time} seconds."

# Run for N iterations, collecting dstat resource usage per run.
# Spark event logs are written automatically by the Spark application
# (spark.eventLog.enabled=true) into SPARK_EVENTS_DIR.

for i in $(seq 1 $ITERATIONS); do
    echo "=== Iteration $i ==="

    # Start dstat to monitor resource usage (CPU, memory, disk, net)
    "${PYTHON2_EXEC}" -c "import types; execfile('${DSTAT_EXEC}')" \
        -tcmndry --output="/tmp/dstat_run_${i}.csv" >/dev/null &
    DSTAT_PID=$!

    sleep 1  # Give dstat a moment to start up

    # Run the Python script with Spark
    #shellcheck disable=SC2086
    $EXEC $ARGS 2>/dev/null \
        | grep --color=never -oP '(?<=Execution time: )[0-9]+\.[0-9]+ seconds' \
        || echo "Error: Execution failed for iteration $i."

    # Kill the princess gently while we pray for a csv flush
    kill -2 $DSTAT_PID
    sleep 1
    kill -0 $DSTAT_PID 2>/dev/null && kill -9 $DSTAT_PID
    wait $DSTAT_PID 2>/dev/null

    # Move the dstat CSV to the output directory
    mv "/tmp/dstat_run_${i}.csv" "${DSTAT_OUT_DIR}/dstat_run_${i}.csv"
done

echo ""
echo "=== All ${ITERATIONS} iterations complete ==="
echo "Spark event logs: ${SPARK_EVENTS_DIR}"
echo "Dstat CSV files:  ${DSTAT_OUT_DIR}/dstat_run_{1..${ITERATIONS}}.csv"
echo ""
echo "Run metrics_parser.py to extract and aggregate benchmark metrics:"
echo "\`\`\`"
echo "python3 metrics_parser.py \\
    --logs-dir ${SPARK_EVENTS_DIR} \\
    --dstat-dir ${DSTAT_OUT_DIR} \\
    --runs ${ITERATIONS} \\
    --config-name <name>"
echo "\`\`\`"
