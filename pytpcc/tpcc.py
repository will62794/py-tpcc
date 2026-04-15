#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2011
# Andy Pavlo
# http:##www.cs.brown.edu/~pavlo/
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
# -----------------------------------------------------------------------

import sys
import os
import string
import datetime
import logging
import re
import argparse
import glob
import time
import csv
import json
import multiprocessing
import subprocess
from configparser import ConfigParser
from pprint import pprint, pformat

from util import results, scaleparameters
from runtime import executor, loader

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s",
                    datefmt="%m-%d-%Y %H:%M:%S",
                    #
                    filename='results.log')

console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter(
    '%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s'))
logging.getLogger('').addHandler(console)

NOTIFY_PHASE_START_PATH = '/data/workdir/src/flamegraph/notify_phase_start.py'
NOTIFY_PHASE_END_PATH = '/data/workdir/src/flamegraph/notify_phase_start.py'

## ==============================================
## noftifyDsiOfPhaseStart
## ==============================================
def noftifyDsiOfPhaseStart(phasename):
    if os.path.isfile(NOTIFY_PHASE_START_PATH):
        output = subprocess.run(["python3", NOTIFY_PHASE_START_PATH, phasename], capture_output=True)
        if output.returncode != 0:
            raise RuntimeError("Failed to notify DSI of phase starting:", output)
## DEF

## ==============================================
## noftifyDsiOfPhaseStart
## ==============================================
def noftifyDsiOfPhaseEnd(phasename):
    if os.path.isfile(NOTIFY_PHASE_END_PATH):
        output = subprocess.run(["python3", NOTIFY_PHASE_END_PATH, phasename], capture_output=True)
        if output.returncode != 0:
            raise RuntimeError("Failed to notify DSI of phase starting:", output)
## DEF

## ==============================================
## createDriverClass
## ==============================================
def createDriverClass(name):
    full_name = "%sDriver" % name.title()
    mod = __import__('drivers.%s' % full_name.lower(), globals(), locals(), [full_name])
    klass = getattr(mod, full_name)
    return klass
## DEF

## ==============================================
## getDrivers
## ==============================================
def getDrivers():
    drivers = []
    for f in [os.path.basename(drv).replace("driver.py", "") for drv in glob.glob("./drivers/*driver.py")]:
        if f != "abstract":
            drivers.append(f)
    return drivers
## DEF

## ==============================================
## startLoading
## ==============================================
def startLoading(driverClass, scaleParameters, args, config):
    logging.debug("Creating client pool with %d processes", args['clients'])
    pool = multiprocessing.Pool(args['clients'])

    # Split the warehouses into chunks
    w_ids = [[] for _ in range(args['clients'])]
    for w_id in range(scaleParameters.starting_warehouse, scaleParameters.ending_warehouse+1):
        idx = w_id % args['clients']
        w_ids[idx].append(w_id)
    ## FOR

    loader_results = []
    try:
        del args['config']
    except KeyError:
        print()
    for i in range(args['clients']):
        r = pool.apply_async(loaderFunc, (driverClass, scaleParameters, args, config, w_ids[i]))
        loader_results.append(r)
    ## FOR

    pool.close()
    logging.debug("Waiting for %d loaders to finish", args['clients'])
    pool.join()
## DEF

## ==============================================
## loaderFunc
## ==============================================
def loaderFunc(driverClass, scaleParameters, args, config, w_ids):
    driver = driverClass(args['ddl'])
    assert driver != None, "Driver in loadFunc is none!"
    logging.debug("Starting client execution: %s [warehouses=%d]", driver, len(w_ids))

    config['load'] = True
    config['execute'] = False
    config['reset'] = False
    config['warehouses'] = args['warehouses']
    driver.loadConfig(config)

    try:
        loadItems = (1 in w_ids)
        l = loader.Loader(driver, scaleParameters, w_ids, loadItems)
        driver.loadStart()
        l.execute()
        driver.loadFinish()
    except KeyboardInterrupt:
        return -1
    except (Exception, AssertionError) as ex:
        logging.warn("Failed to load data: %s", ex)
        raise

## DEF

## ==============================================
## startExecution
## ==============================================
def startExecution(driverClass, scaleParameters, args, config):
    logging.debug("Creating client pool with %d processes", args['clients'])
    pool = multiprocessing.Pool(args['clients'])
    debug = logging.getLogger().isEnabledFor(logging.DEBUG)
    try:
        del args['config']
    except KeyError:
        print()
    worker_results = []
    for _ in range(args['clients']):
        r = pool.apply_async(executorFunc, (driverClass, scaleParameters, args, config, debug,))
        worker_results.append(r)
    ## FOR
    pool.close()
    pool.join()

    total_results = results.Results()
    for asyncr in worker_results:
        asyncr.wait()
        r = asyncr.get()
        assert r != None, "No results object returned by thread!"
        if r == -1:
            sys.exit(1)
        total_results.append(r)
    ## FOR

    return total_results
## DEF

## ==============================================
## executorFunc
## ==============================================
def executorFunc(driverClass, scaleParameters, args, config, debug):
    driver = driverClass(args['ddl'])
    assert driver != None, "No driver in executorFunc"
    logging.debug("Starting client execution: %s", driver)

    config['execute'] = True
    config['reset'] = False
    driver.loadConfig(config)
    installLatencySimulation(driver, args.get('simulated_latency_ms', 0))

    e = executor.Executor(driver, scaleParameters, stop_on_error=args['stop_on_error'])
    driver.executeStart()
    results = e.execute(args['duration'])
    driver.executeFinish()

    return results
## DEF

## ==============================================
## installLatencySimulation
## ==============================================
def installLatencySimulation(driver, simulated_latency_ms):
    """Inject artificial client/server latency around each transaction call."""
    if simulated_latency_ms is None:
        return
    if simulated_latency_ms <= 0:
        return

    original_execute_txn = driver.executeTransaction
    delay_sec = float(simulated_latency_ms) / 1000.0

    def executeWithLatency(txn, params):
        # One sleep before and one after models request/response network transit.
        time.sleep(delay_sec)
        value = original_execute_txn(txn, params)
        time.sleep(delay_sec)
        return value

    driver.executeTransaction = executeWithLatency
## DEF

## ==============================================
## parseIntegerListArg
## ==============================================
def parseIntegerListArg(value):
    if value is None:
        return []
    cleaned = [x.strip() for x in str(value).split(",") if x.strip()]
    if not cleaned:
        return []
    parsed = sorted(set([int(x) for x in cleaned]))
    for i in parsed:
        if i <= 0:
            raise ValueError("All values must be positive integers")
    return parsed
## DEF

## ==============================================
## getBenchmarkRows
## ==============================================
def getBenchmarkRows(driverClass, scaleParameters, args, config):
    if args['benchmark_threads']:
        thread_counts = parseIntegerListArg(args['benchmark_threads'])
    else:
        thread_counts = [args['clients']]
    latencies_ms = list(range(args['benchmark_latency_start'],
                              args['benchmark_latency_end'] + 1,
                              args['benchmark_latency_step']))

    rows = []
    total_runs = len(thread_counts) * len(latencies_ms)
    run_id = 0
    output_dir = os.path.realpath(args['benchmark_output_dir'])
    if not os.path.isdir(output_dir):
        os.makedirs(output_dir)
    for clients in thread_counts:
        for latency_ms in latencies_ms:
            run_id += 1
            logging.info("Benchmark sweep run %d/%d: clients=%d latency_ms=%d",
                         run_id, total_runs, clients, latency_ms)
            bench_args = dict(args)
            bench_args['clients'] = clients
            bench_args['simulated_latency_ms'] = latency_ms

            run_start = time.time()
            if clients == 1:
                driver = driverClass(bench_args['ddl'])
                run_config = dict(config)
                run_config['execute'] = True
                run_config['reset'] = False
                driver.loadConfig(run_config)
                installLatencySimulation(driver, latency_ms)
                e = executor.Executor(driver, scaleParameters, stop_on_error=bench_args['stop_on_error'])
                driver.executeStart()
                run_results = e.execute(bench_args['duration'])
                driver.executeFinish()
            else:
                run_results = startExecution(driverClass, scaleParameters, bench_args, dict(config))
            assert run_results, "No results from benchmark execution for %d clients!" % clients

            duration = (run_results.stop - run_results.start) if run_results.stop else (time.time() - run_results.start)
            total_committed = sum(run_results.txn_counters.values())
            total_aborts = sum(run_results.txn_aborts.values())
            throughput_tps = (float(total_committed) / duration) if duration > 0 else 0.0
            txn_attempts = total_committed + total_aborts
            abort_rate = (float(total_aborts) / txn_attempts) if txn_attempts > 0 else 0.0
            new_order_cnt = run_results.txn_counters.get('NEW_ORDER', 0)
            tpmc = (new_order_cnt * 60.0 / duration) if duration > 0 else 0.0

            row = {
                'clients': clients,
                'latency_ms': latency_ms,
                'duration_s': round(duration, 4),
                'committed_txns': total_committed,
                'aborts': total_aborts,
                'attempted_txns': txn_attempts,
                'throughput_tps': round(throughput_tps, 4),
                'tpmc': round(tpmc, 4),
                'abort_rate': round(abort_rate, 6),
                'total_retries': int(sum(run_results.txn_retries.values())),
                'wall_clock_s': round(time.time() - run_start, 4),
            }
            rows.append(row)
            # Persist and re-plot after each run so users can watch progress live.
            saveBenchmarkArtifacts(rows, args, emit_logs=False)
            logging.info("Updated benchmark artifacts after run %d/%d", run_id, total_runs)
            logging.info("Run complete: clients=%d latency=%dms throughput=%.2f TPS abort_rate=%.2f%%",
                         clients, latency_ms, throughput_tps, abort_rate * 100.0)
    return rows
## DEF

## ==============================================
## saveBenchmarkArtifacts
## ==============================================
def saveBenchmarkArtifacts(rows, args, emit_logs=True):
    output_dir = os.path.realpath(args['benchmark_output_dir'])
    if not os.path.isdir(output_dir):
        os.makedirs(output_dir)

    csv_path = os.path.join(output_dir, "latency_thread_sweep.csv")
    json_path = os.path.join(output_dir, "latency_thread_sweep.json")

    fieldnames = [
        'clients', 'latency_ms', 'duration_s', 'committed_txns', 'aborts',
        'attempted_txns', 'throughput_tps', 'tpmc', 'abort_rate',
        'total_retries', 'wall_clock_s'
    ]
    with open(csv_path, "w", newline="") as csv_out:
        writer = csv.DictWriter(csv_out, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    with open(json_path, "w") as json_out:
        json.dump(rows, json_out, indent=2, sort_keys=True)

    plot_paths = generateBenchmarkPlots(rows, output_dir)
    if emit_logs:
        logging.info("Benchmark artifacts written to %s", output_dir)
        logging.info("CSV results: %s", csv_path)
        logging.info("JSON results: %s", json_path)
        for plot_path in plot_paths:
            logging.info("Plot: %s", plot_path)

## DEF

## ==============================================
## generateBenchmarkPlots
## ==============================================
def generateBenchmarkPlots(rows, output_dir):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        logging.warning("matplotlib not available; skipping plot generation")
        return []

    latencies = sorted(set([r['latency_ms'] for r in rows]))
    threads = sorted(set([r['clients'] for r in rows]))
    rows_by_thread = dict((t, sorted([r for r in rows if r['clients'] == t], key=lambda r: r['latency_ms'])) for t in threads)
    rows_by_latency = dict((l, sorted([r for r in rows if r['latency_ms'] == l], key=lambda r: r['clients'])) for l in latencies)
    saved = []

    # Throughput vs latency (one line per thread count)
    fig, ax = plt.subplots(figsize=(11, 7))
    for t in threads:
        x = [r['latency_ms'] for r in rows_by_thread[t]]
        y = [r['throughput_tps'] for r in rows_by_thread[t]]
        ax.plot(x, y, marker='o', linewidth=2, label="%d threads" % t)
    ax.set_title("TPC-C Throughput vs Simulated Client/Server Latency")
    ax.set_xlabel("Simulated latency (ms, one-way)")
    ax.set_ylabel("Throughput (transactions/second)")
    ax.grid(True, alpha=0.3)
    ax.legend(title="Clients", loc="best")
    throughput_vs_latency = os.path.join(output_dir, "throughput_vs_latency.png")
    fig.tight_layout()
    fig.savefig(throughput_vs_latency, dpi=150)
    plt.close(fig)
    saved.append(throughput_vs_latency)

    # Abort rate vs latency (one line per thread count)
    fig, ax = plt.subplots(figsize=(11, 7))
    for t in threads:
        x = [r['latency_ms'] for r in rows_by_thread[t]]
        y = [r['abort_rate'] * 100.0 for r in rows_by_thread[t]]
        ax.plot(x, y, marker='o', linewidth=2, label="%d threads" % t)
    ax.set_title("TPC-C Abort Rate vs Simulated Client/Server Latency")
    ax.set_xlabel("Simulated latency (ms, one-way)")
    ax.set_ylabel("Abort rate (%)")
    ax.grid(True, alpha=0.3)
    ax.legend(title="Clients", loc="best")
    abort_vs_latency = os.path.join(output_dir, "abort_rate_vs_latency.png")
    fig.tight_layout()
    fig.savefig(abort_vs_latency, dpi=150)
    plt.close(fig)
    saved.append(abort_vs_latency)

    # Throughput vs thread count (one line per latency)
    fig, ax = plt.subplots(figsize=(11, 7))
    for l in latencies:
        x = [r['clients'] for r in rows_by_latency[l]]
        y = [r['throughput_tps'] for r in rows_by_latency[l]]
        ax.plot(x, y, marker='o', linewidth=2, label="%d ms" % l)
    ax.set_title("TPC-C Throughput vs Client Threads")
    ax.set_xlabel("Client threads")
    ax.set_ylabel("Throughput (transactions/second)")
    ax.grid(True, alpha=0.3)
    ax.legend(title="Latency", loc="best", ncol=2)
    throughput_vs_threads = os.path.join(output_dir, "throughput_vs_threads.png")
    fig.tight_layout()
    fig.savefig(throughput_vs_threads, dpi=150)
    plt.close(fig)
    saved.append(throughput_vs_threads)

    # Abort rate vs thread count (one line per latency)
    fig, ax = plt.subplots(figsize=(11, 7))
    for l in latencies:
        x = [r['clients'] for r in rows_by_latency[l]]
        y = [r['abort_rate'] * 100.0 for r in rows_by_latency[l]]
        ax.plot(x, y, marker='o', linewidth=2, label="%d ms" % l)
    ax.set_title("TPC-C Abort Rate vs Client Threads")
    ax.set_xlabel("Client threads")
    ax.set_ylabel("Abort rate (%)")
    ax.grid(True, alpha=0.3)
    ax.legend(title="Latency", loc="best", ncol=2)
    abort_vs_threads = os.path.join(output_dir, "abort_rate_vs_threads.png")
    fig.tight_layout()
    fig.savefig(abort_vs_threads, dpi=150)
    plt.close(fig)
    saved.append(abort_vs_threads)

    return saved
## DEF

## ==============================================
## main
## ==============================================
if __name__ == '__main__':
    aparser = argparse.ArgumentParser(description='Python implementation of the TPC-C Benchmark')
    aparser.add_argument('system', choices=getDrivers(),
                         help='Target system driver')
    aparser.add_argument('--config', type=open,
                         help='Path to driver configuration file')
    aparser.add_argument('--reset', action='store_true',
                         help='Instruct the driver to reset the contents of the database')
    aparser.add_argument('--scalefactor', default=1, type=float, metavar='SF',
                         help='Benchmark scale factor')
    aparser.add_argument('--warehouses', default=4, type=int, metavar='W',
                         help='Number of Warehouses')
    aparser.add_argument('--duration', default=60, type=int, metavar='D',
                         help='How long to run the benchmark in seconds')
    aparser.add_argument('--ddl',
                         default=os.path.realpath(os.path.join(os.path.dirname(__file__), "tpcc.sql")),
                         help='Path to the TPC-C DDL SQL file')
    aparser.add_argument('--clients', default=1, type=int, metavar='N',
                         help='The number of blocking clients to fork')
    aparser.add_argument('--stop-on-error', action='store_true',
                         help='Stop the transaction execution when the driver throws an exception.')
    aparser.add_argument('--no-load', action='store_true',
                         help='Disable loading the data')
    aparser.add_argument('--no-execute', action='store_true',
                         help='Disable executing the workload')
    aparser.add_argument('--print-config', action='store_true',
                         help='Print out the default configuration file for the system and exit')
    aparser.add_argument('--debug', action='store_true',
                         help='Enable debug log messages')
    aparser.add_argument('--simulated-latency-ms', default=0, type=int, metavar='MS',
                         help='Artificial one-way client/server latency per transaction in milliseconds')
    aparser.add_argument('--benchmark-latency-sweep', action='store_true',
                         help='Run a latency/thread sweep benchmark and output CSV/JSON/plots')
    aparser.add_argument('--benchmark-latency-start', default=0, type=int, metavar='MS',
                         help='Latency sweep start in milliseconds')
    aparser.add_argument('--benchmark-latency-end', default=100, type=int, metavar='MS',
                         help='Latency sweep end in milliseconds')
    aparser.add_argument('--benchmark-latency-step', default=10, type=int, metavar='MS',
                         help='Latency sweep step in milliseconds')
    aparser.add_argument('--benchmark-threads', default="1,2,4,8", metavar='LIST',
                         help='Comma-separated thread/client counts for benchmark sweeps (e.g., 1,2,4,8)')
    aparser.add_argument('--benchmark-output-dir', default='benchmark_results', metavar='DIR',
                         help='Output directory for benchmark CSV/JSON/plots')
    args = vars(aparser.parse_args())

    if args['debug']:
        logging.getLogger().setLevel(logging.DEBUG)
    if args['simulated_latency_ms'] < 0:
        raise ValueError("--simulated-latency-ms must be >= 0")
    if args['benchmark_latency_start'] < 0 or args['benchmark_latency_end'] < 0:
        raise ValueError("benchmark latency range must be >= 0")
    if args['benchmark_latency_step'] <= 0:
        raise ValueError("--benchmark-latency-step must be > 0")
    if args['benchmark_latency_start'] > args['benchmark_latency_end']:
        raise ValueError("benchmark latency start must be <= benchmark latency end")
    try:
        parseIntegerListArg(args['benchmark_threads'])
    except ValueError as ex:
        raise ValueError("--benchmark-threads value is invalid: %s" % ex)

    ## Create a handle to the target client driver
    driverClass = createDriverClass(args['system'])
    assert driverClass != None, "Failed to find '%s' class" % args['system']
    driver = driverClass(args['ddl'])
    assert driver != None, "Failed to create '%s' driver" % args['system']
    if args['print_config']:
        config = driver.makeDefaultConfig()
        print(driver.formatConfig(config))
        print()
        sys.exit(0)

    ## Load Configuration file
    if args['config']:
        logging.debug("Loading configuration file '%s'", args['config'])
        cparser = ConfigParser()
        cparser.read(os.path.realpath(args['config'].name))
        config = dict(cparser.items(args['system']))
    else:
        logging.debug("Using default configuration for %s", args['system'])
        defaultConfig = driver.makeDefaultConfig()
        config = dict([(param, defaultConfig[param][1]) for param in defaultConfig.keys()])
    config['reset'] = args['reset']
    config['load'] = False
    config['execute'] = False
    if config['reset']:
        logging.info("Reseting database")
    config['warehouses'] = args['warehouses']
    driver.loadConfig(config)
    logging.info("Initializing TPC-C benchmark using %s", driver)

    ## Create ScaleParameters
    scaleParameters = scaleparameters.makeWithScaleFactor(args['warehouses'], args['scalefactor'])
    if args['debug']:
        logging.debug("Scale Parameters:\n%s", scaleParameters)

    ## DATA LOADER!!!
    load_time = None
    if not args['no_load']:
        logging.info("Loading TPC-C benchmark data using %s", (driver))
        noftifyDsiOfPhaseStart("TPC-C_load")
        load_start = time.time()
        if args['clients'] == 1:
            l = loader.Loader(
                driver,
                scaleParameters,
                range(scaleParameters.starting_warehouse, scaleParameters.ending_warehouse+1),
                True
            )
            driver.loadStart()
            l.execute()
            driver.loadFinish()
        else:
            startLoading(driverClass, scaleParameters, args, config)
        load_time = time.time() - load_start
        noftifyDsiOfPhaseEnd("TPC-C_load")
    ## IF

    ## WORKLOAD DRIVER!!!
    if not args['no_execute']:
        noftifyDsiOfPhaseStart("TPC-C_workload")
        if args['benchmark_latency_sweep']:
            benchmark_rows = getBenchmarkRows(driverClass, scaleParameters, args, dict(config))
            saveBenchmarkArtifacts(benchmark_rows, args)
            logging.info("Benchmark sweep complete: %d runs", len(benchmark_rows))
        else:
            if args['clients'] == 1:
                installLatencySimulation(driver, args.get('simulated_latency_ms', 0))
                e = executor.Executor(driver, scaleParameters, stop_on_error=args['stop_on_error'])
                driver.executeStart()
                results = e.execute(args['duration'])
                driver.executeFinish()
            else:
                results = startExecution(driverClass, scaleParameters, args, config)
            assert results, "No results from execution for %d client!" % args['clients']
            logging.info("Final Results")
            logging.info("Threads: %d", args['clients'])
            logging.info(results.show(load_time, driver, args['clients']))
        noftifyDsiOfPhaseEnd("TPC-C_workload")
    ## IF

## MAIN
