#!/bin/bash
#SBATCH --job-name=single-run-spark-ehpc
#SBATCH --partition=dev-arm
#SBATCH --account=f202500010hpcvlabuminhoa
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=48  # 5 cores per executor, 4 executors total
#SBATCH --mem=16G
#SBATCH --time=00:05:00
#SBATCH --output=jobs/out/single_run_%j.out

SCRIPT="$1"
if [ "$#" -ne 1 ]; then
    SCRIPT="src/statsEHPC_latest.py"
    echo "Defaulting execution script to $SCRIPT"
fi

source modules.sh
source venv/bin/activate

# dstat -tcmndry --output=output/dstat_$SLURM_JOB_ID.csv 1 &
# DSTAT_PID=$!

echo "Running: python3 $SCRIPT -m Jan"
python3 "$SCRIPT" -m Jan

# kill $DSTAT_PID

# Simple but accurate validation test
cmp -s "output/params.tex" "output/params_baseline.tex" \
    && echo -e "Validation test: \e[32mpassed ✓\e[0m" \
    || echo -e "Validation test: \e[31mfailed ✕\e[0m"
