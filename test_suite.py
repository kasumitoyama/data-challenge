from process_messages import KafkaMapReduce

import argparse

small_file = "data/stream-first-100.jsonl"
small_file_with_bf = "data/stream-first-100-bit-flip.jsonl"

ts_normal = 24470739
ts_bitflip = 91137406
very_late_timestamp = 9999999999999

def get_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_file",
        help="Which input file to read from (for testing from stdin)",
        default="",
    )
    parser.add_argument(
        "--output_to_stdout",
        help="Whether to output to stdout (instead of to a new kafka topic)",
        action="store_true",
    )
    parser.add_argument(
        "--pytest_mode",
        help="Whether to return early and keep values for test",
        action="store_true",
    )

    # Kafka arguments
    parser.add_argument(
        "--max_poll_records",
        help="The max poll records to read at a time from kafka",
        default=500,
        type=int,
    )
    parser.add_argument(
        "--max_poll_interval_ms",
        help="The max poll interval for kafka",
        default=300000,
        type=int,
    )
    parser.add_argument(
        "--input_topic_name",
        help="Which topic name to read from",
        default="stream-topic",
    )
    parser.add_argument(
        "--output_topic_name",
        help="Which topic name to write to",
        default="output-topic",
    )
    parser.add_argument(
        "--consumer_group", help="Which cg to be a part of", default="default-cg"
    )
    return parser

def test_count_basic():
    argv = ["--input_file", small_file, "--pytest_mode"]
    
    parser = get_parser()
    args = parser.parse_args(argv)
    map_reducer = KafkaMapReduce(args)
    map_reducer.process_messages()

    assert len(map_reducer.unique_users_per_min[ts_normal]) == 100, "Error in counting users!"

def test_count_bit_flip():
    argv = ["--input_file", small_file_with_bf, "--pytest_mode"]
    
    parser = get_parser()
    args = parser.parse_args(argv)
    map_reducer = KafkaMapReduce(args)
    map_reducer.process_messages()

    assert len(map_reducer.unique_users_per_min[ts_normal]) == 99, "Error in counting users!"
    assert len(map_reducer.unique_users_per_min[ts_bitflip]) == 1, "Error in counting users for bitflip!"

def test_bitflip_wont_send():
    argv = ["--input_file", small_file_with_bf, "--pytest_mode", "--output_to_stdout"]
    
    parser = get_parser()
    args = parser.parse_args(argv)
    map_reducer = KafkaMapReduce(args)
    map_reducer.process_messages()
    map_reducer.send_messages_to_output(very_late_timestamp)

    assert ts_normal not in map_reducer.unique_users_per_min, "The normal timestamp was not sent!"
    assert len(map_reducer.unique_users_per_min[ts_bitflip]) == 1, "The bitflipped index was sent!"
