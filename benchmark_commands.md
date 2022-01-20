# Benchmark results

The benchmarks I have performed here are with changing the max poll records.
One can rerun them with the commands.

Run with these commands:

```
python process_messages.py --run_benchmark --max_poll_records 100
python process_messages.py --run_benchmark --max_poll_records 250
python process_messages.py --run_benchmark --max_poll_records 500
python process_messages.py --run_benchmark --max_poll_records 1000
python process_messages.py --run_benchmark --max_poll_records 2000
```

Which gave me the following frames per second:

```
13089.6
17838.6
18510.4
19223.5
19207.6
```

From the 1000 run, the time it takes to parse JSON:

3.148e-5

And the time it takes to parse the whole msg:

3.264e-5
