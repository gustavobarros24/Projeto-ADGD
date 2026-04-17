import io
import json
import os
import glob
import subprocess
import pandas as pd
import zstandard as zstd
import argparse


# Neste metrics_parser são calculadas as seguintes métricas:
#   - wall_time_s      : tempo total de execução da aplicação Spark (ApplicationStart → ApplicationEnd), nos event logs
#   - stages           : número de stages submetidos, nos event logs (SparkListenerStageSubmitted)
#   - tasks            : total de tasks em todos os stages, nos event logs (SparkListenerStageSubmitted)
#   - shuffle_read_mb  : total de bytes lidos em shuffle, nos event logs (SparkListenerTaskEnd → Task Metrics)
#   - shuffle_write_mb : total de bytes escritos em shuffle, nos event logs (SparkListenerTaskEnd → Task Metrics)
#   - driver_time_s    : estimativa do tempo do driver (wall_time − span_máximo_das_tasks), nos event logs
#   - peak_memory_mb   : pico de memória RAM usada durante cada run, nos ficheiros CSV do dstat


def open_event_log(log_path):
    ext = os.path.splitext(log_path)[1].lower()

    if ext == ".gz":
        import gzip
        return gzip.open(log_path, "rt", encoding="utf-8", errors="replace")

    if ext in (".zst", ".zstd"):
        try:
            fh = open(log_path, "rb")
            dctx = zstd.ZstdDecompressor()
            return io.TextIOWrapper(dctx.stream_reader(fh), encoding="utf-8", errors="replace")
        except ImportError:
            pass

        if _zstd_cli_available():
            proc = subprocess.Popen(
                ["zstd", "-dc", log_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
            )
            return io.TextIOWrapper(proc.stdout, encoding="utf-8", errors="replace")

        raise OSError(
            f"Cannot decompress {log_path}: neither the 'zstandard' Python package "
            "nor the 'zstd' CLI tool is available. "
            "Install one of them: pip install zstandard  or  apt install zstd"
        )

    # Plain text (default).
    return open(log_path, "r", encoding="utf-8", errors="replace")


def _zstd_cli_available():
    """Returns True if the zstd binary is on PATH."""
    try:
        subprocess.run(["zstd", "--version"], capture_output=True, check=True)
        return True
    except (FileNotFoundError, subprocess.CalledProcessError):
        return False


def parse_dstat_peak_memory(csv_path):
    """
    Parses a dstat CSV file to find peak memory usage (in MB).

    Looks specifically for the column under the "memory usage" section labelled
    "used", ignoring other "used" columns (e.g. swap).  Handles values in bytes
    (the dstat default) as well as files that already express values in kB, MB
    or GB by inspecting the unit hint in the header block.

    Returns peak used memory in MB, or 0.0 if the file cannot be parsed.
    """
    if not os.path.exists(csv_path):
        print(f"[WARN] dstat file not found: {csv_path}")
        return 0.0

    peak_mem = 0.0
    idx_mem = -1
    # dstat can report memory in B, kB, MB, GB depending on --noheaders / unit flags.
    # We default to bytes and try to detect from the header.
    divisor = 1024 * 1024  # bytes → MB

    with open(csv_path, "r") as f:
        lines = f.readlines()

    # ------------------------------------------------------------------ #
    # Phase 1: locate the "memory usage" section and its "used" column.   #
    # dstat CSV structure (simplified):                                    #
    #   "Dstat ..."           <- banner line                               #
    #   ...                   <- author / date lines                       #
    #   "memory usage","disk" <- section-header line                       #
    #   "used","free",...     <- column-header line                        #
    #   <data rows>                                                         #
    # ------------------------------------------------------------------ #
    in_memory_section = False

    for line in lines:
        row = [x.strip().strip('"') for x in line.split(",")]

        # Detect section header that contains "memory usage"
        if any("memory usage" in cell.lower() for cell in row):
            in_memory_section = True
            # Remember the column offset where "memory usage" starts so we can
            # map "used" to the right absolute column index on the next header row.
            mem_section_start = next(
                i for i, c in enumerate(row) if "memory usage" in c.lower()
            )
            continue

        # Detect column-header row immediately after the memory-usage section header
        if in_memory_section and idx_mem == -1:
            if "used" in row:
                # Find "used" starting from the memory section offset
                candidates = [i for i, c in enumerate(row) if c == "used"]
                # Pick the first candidate that is >= mem_section_start
                for c in candidates:
                    if c >= mem_section_start:
                        idx_mem = c
                        break
                if idx_mem == -1:
                    # Fallback: take the first "used"
                    idx_mem = candidates[0]

                # Try to detect unit from the same header area (e.g. "used/MB")
                header_cell = row[idx_mem]
                if "/kb" in header_cell.lower():
                    divisor = 1024
                elif "/mb" in header_cell.lower():
                    divisor = 1
                elif "/gb" in header_cell.lower():
                    divisor = 1 / 1024
            continue

        # Data rows
        if idx_mem != -1 and len(row) > idx_mem:
            val_str = row[idx_mem]
            try:
                val = float(val_str)
                peak_mem = max(peak_mem, val)
            except ValueError:
                # Skip non-numeric rows (e.g. repeated headers mid-file)
                continue

    if peak_mem == 0.0:
        print(f"[WARN] Could not extract memory data from {csv_path}")
        return 0.0

    return peak_mem / divisor


def parse_event_log(log_path):
    """
    Parses a Spark JSON Event Log (one JSON object per line).

    Returns a dict with:
      wall_time_s       – application wall-clock time in seconds
      stages            – number of stages submitted
      tasks             – total number of tasks across all stages
      shuffle_read_mb   – total shuffle bytes read, in MB
      shuffle_write_mb  – total shuffle bytes written, in MB
      driver_time_s     – estimated driver-only time (wall − max task span).
                          This is a lower-bound approximation; it does not
                          account for overlapping executor and driver work.
    """
    stages_submitted = 0
    tasks_count = 0
    shuffle_read_bytes = 0
    shuffle_write_bytes = 0

    task_start_times = []
    task_end_times = []
    app_start = 0
    app_end = 0

    try:
        with open_event_log(log_path) as f:
            for lineno, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    event = json.loads(line)
                except json.JSONDecodeError as exc:
                    print(f"[WARN] {log_path}:{lineno} – JSON parse error: {exc}")
                    continue

                e_type = event.get("Event")

                if e_type == "SparkListenerApplicationStart":
                    app_start = event.get("Timestamp", 0)

                elif e_type == "SparkListenerApplicationEnd":
                    app_end = event.get("Timestamp", 0)

                elif e_type == "SparkListenerStageSubmitted":
                    stages_submitted += 1
                    info = event.get("Stage Info", {})
                    tasks_count += info.get("Number of Tasks", 0)

                elif e_type == "SparkListenerTaskStart":
                    t = event.get("Task Info", {}).get("Launch Time", 0)
                    if t:
                        task_start_times.append(t)

                elif e_type == "SparkListenerTaskEnd":
                    t = event.get("Task Info", {}).get("Finish Time", 0)
                    if t:
                        task_end_times.append(t)

                    metrics = event.get("Task Metrics") or {}
                    sr = metrics.get("Shuffle Read Metrics") or {}
                    shuffle_read_bytes += sr.get("Total Bytes Read", 0) + sr.get("Local Bytes Read", 0)
                    sw = metrics.get("Shuffle Write Metrics") or {}
                    shuffle_write_bytes += sw.get("Shuffle Bytes Written", 0)

    except OSError as exc:
        print(f"[ERROR] Cannot open {log_path}: {exc}")
        return _empty_metrics()

    wall_time_s = (app_end - app_start) / 1000.0 if app_end > app_start else 0.0

    # Driver time estimate (see docstring for caveats).
    driver_time_s = 0.0
    if task_start_times and task_end_times and wall_time_s > 0:
        task_span_s = (max(task_end_times) - min(task_start_times)) / 1000.0
        driver_time_s = max(0.0, wall_time_s - task_span_s)

    return {
        "wall_time_s": wall_time_s,
        "stages": stages_submitted,
        "tasks": tasks_count,
        "shuffle_read_mb": shuffle_read_bytes / (1024 * 1024),
        "shuffle_write_mb": shuffle_write_bytes / (1024 * 1024),
        "driver_time_s": driver_time_s,
    }


def _empty_metrics():
    """Return a zeroed metrics dict so callers always get a consistent shape."""
    return {
        "wall_time_s": 0.0,
        "stages": 0,
        "tasks": 0,
        "shuffle_read_mb": 0.0,
        "shuffle_write_mb": 0.0,
        "driver_time_s": 0.0,
    }


def collect_log_files(logs_dir, n=3):
    """
    Returns the *n* most recent Spark event log files, ordered oldest-first
    (Run 1, Run 2, …).

    Ordering is based on the SparkListenerApplicationStart timestamp embedded
    in each file, which is more reliable than filesystem mtime.  Falls back to
    mtime if the timestamp cannot be read.
    """
    # Support both flat files and Spark v2 event log directories
    # (eventlog_v2_*/events_*.zstd)
    candidates = []
    for entry in glob.glob(os.path.join(logs_dir, "*")):
        if os.path.isfile(entry) and not entry.endswith(".crc"):
            candidates.append(entry)
        elif os.path.isdir(entry):
            # Spark v2 format: directory containing events_*.zstd / events_*.gz
            for f in glob.glob(os.path.join(entry, "events_*")):
                if os.path.isfile(f) and not f.endswith(".crc"):
                    candidates.append(f)
                    break  # one event file per directory

    def app_start_ts(path):
        try:
            with open_event_log(path) as fh:
                for line in fh:
                    try:
                        ev = json.loads(line)
                        if ev.get("Event") == "SparkListenerApplicationStart":
                            return ev.get("Timestamp", 0)
                    except json.JSONDecodeError:
                        continue
        except OSError:
            pass
        return os.path.getmtime(path) * 1000  # fallback: mtime in ms

    candidates.sort(key=app_start_ts, reverse=True)
    recent = candidates[:n]
    recent.reverse()  # oldest-first → Run 1, Run 2, Run 3
    return recent


def main():
    parser = argparse.ArgumentParser(
        description="Parse Spark Event Logs and generate benchmark metrics."
    )
    parser.add_argument(
        "--logs-dir",
        default="spark-events",
        help="Directory containing Spark event log files.",
    )
    parser.add_argument(
        "--dstat-dir",
        default="output",
        help="Directory containing dstat CSV files (dstat_run_1.csv, …).",
    )
    parser.add_argument(
        "--config-name",
        default="baseline",
        help="Label for this configuration (e.g. baseline, opt1).",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=3,
        help="Number of benchmark iterations to collect.",
    )
    parser.add_argument(
        "--out-json",
        default="benchmark_metrics.json",
        help="Path for the output JSON file.",
    )
    args = parser.parse_args()

    # ------------------------------------------------------------------ #
    # Collect log files                                                    #
    # ------------------------------------------------------------------ #
    log_files = collect_log_files(args.logs_dir, n=args.runs)

    if not log_files:
        print(f"[ERROR] No event log files found in '{args.logs_dir}'. Exiting.")
        raise SystemExit(1)

    if len(log_files) < args.runs:
        print(
            f"[WARN] Expected {args.runs} log files but found {len(log_files)}. "
            "Statistics may be unreliable."
        )

    # ------------------------------------------------------------------ #
    # Parse each run                                                       #
    # ------------------------------------------------------------------ #
    results = []
    for i, log in enumerate(log_files):
        run_id = i + 1
        print(f"[INFO] Parsing run {run_id}: {log}")
        metrics = parse_event_log(log)

        dstat_file = os.path.join(args.dstat_dir, f"dstat_run_{run_id}.csv")
        metrics["peak_memory_mb"] = parse_dstat_peak_memory(dstat_file)
        metrics["run"] = run_id
        metrics["config"] = args.config_name
        results.append(metrics)

    # ------------------------------------------------------------------ #
    # Aggregate                                                            #
    # ------------------------------------------------------------------ #
    df = pd.DataFrame(results)
    numeric_cols = df.select_dtypes(include="number").columns
    mean_s = df[numeric_cols].mean()
    std_s = df[numeric_cols].std()

    summary = {
        "config": args.config_name,
        "n_runs": len(results),
        "mean": mean_s.to_dict(),
        "std": std_s.to_dict(),
    }

    output = {"runs": results, "summary": summary}
    with open(args.out_json, "w") as f:
        json.dump(output, f, indent=4)

    # ------------------------------------------------------------------ #
    # Report                                                               #
    # ------------------------------------------------------------------ #
    def fmt(key):
        m, s = mean_s.get(key, float("nan")), std_s.get(key, float("nan"))
        return f"{m:.2f} ± {s:.2f}"

    print(f"\n--- Summary: {args.config_name} ({len(results)} runs) ---")
    print(f"  Wall time (s):        {fmt('wall_time_s')}")
    print(f"  Driver time (s)*:     {fmt('driver_time_s')}")
    print(f"  Shuffle read  (MB):   {fmt('shuffle_read_mb')}")
    print(f"  Shuffle write (MB):   {fmt('shuffle_write_mb')}")
    print(f"  Stages:               {mean_s.get('stages', float('nan')):.1f}")
    print(f"  Tasks:                {mean_s.get('tasks', float('nan')):.1f}")
    print(f"  Peak memory (MB):     {fmt('peak_memory_mb')}")
    print(f"\n* Driver time is an approximation (wall − max-task-span).")
    print(f"  Results saved to {args.out_json}")


if __name__ == "__main__":
    main()
