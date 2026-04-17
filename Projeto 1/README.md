# *ADGD* Project

**Note**: Clone this repository with the `--recursive` flag to ensure the dstat
submodule is included.

```sh
ln -s src/statsEHPC_v8_reduce_collects.py src/statsEHPC_latest.py
```

## Setup

**Note**: Run the following setup commands on the desired Deucalion partition.
The work team has decided to use the ARM partition for this project, though the
x86 partition is also supported.

```sh
source modules.sh                # Load necessary modules (Java, OpenMPI, Python)

python3 -m venv venv             # Create a virtual environment for PySpark
source venv/bin/activate         # Activate the virtual environment
pip install -r requirements.txt  # Install Python dependencies (PySpark, etc.)
```

## Project Structure

```sh
.
├── src/                     # Source code for the Spark optimization scripts
├── data/                    # Default input datasets
│
├── output/                  # Default output directory
│   ├── execution_plan.txt       # Spark execution plan for analysis
│   ├── main.pdf                 # EHPC report generated from main.tex
│   ├── main.tex                 # LaTeX source for EHPC report
│   ├── params.tex               # Latest output parameters from the Spark script
│   └── params_baseline.tex      # Baseline parameters for validation testing
│
├── jobs/                    # SLURM job scripts and benchmarking tools
│   ├── out/                     # Standard output from SLURM jobs
│   ├── benchmark.sh             # Main benchmarking script with validation testing
│   ├── metrics_parser.py        # Script to parse dstat and Spark events
│   ├── plots.ipynb              # Jupyter notebook for visualizing performance metrics
│   ├── requirements.txt         # Python dependencies for analysis script
│   └── single-run.sh            # Script for a single execution a Spark script
│
├── spark-events/            # Directory for Spark event logs generated during execution
├── dstat/                   # dstat git submodule for system monitoring
├── report/                  # Assignment report and related files
├── assignment.pdf           # Assignment description and requirements
├── modules.sh
├── requirements.txt
├── spark-ui.sh              # Helper script to launch Spark UI
└── README.md
```

## Optimization Strategies

See [src/README.md](src/README.md) for details on the Spark optimizations
implemented in this project, as well as the benchmarking environment.

## SLURM Job Scripts

**Note to Professor**: The `src/statsEHPC_latest.py` script is a symbolic link
to the latest optimized version of the Spark script. While not mandatory, it's
mentioned throughout this section for convenience. Create it with:

### Benchmarking

To run the benchmarking script on the Deucalion's ARM partition:

```sh
sbatch jobs/benchmark.sh src/statsEHPC_latest.py spark-events/
```

### Single Execution

To run a single execution of the Spark script:

```sh
sbatch jobs/single-run.sh src/statsEHPC_latest.py
```

## Contributors

- [Miguel Carvalho](https://github.com/migueltc13)
- [André Miranda](https://github.com/RollingJack)
- [Gustavo Barros](https://github.com/gustavobarros24)

<a href="https://github.com/migueltc13/project-1-ADGD/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=migueltc13/project-1-ADGD" />
</a>
