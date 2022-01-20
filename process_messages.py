import argparse
import json
import sys
import time

from kafka import KafkaConsumer, KafkaProducer


class KafkaMapReduce:
    def __init__(self, args):
        self.args = args
        self.unique_users_per_min = {}

        # Would be sent to an alert email instead of just being logged
        self.errors = []

        self.json_avg_read_time = 0
        self.json_read_time_events = 0

        self.message_avg_process_time = 0
        self.message_process_time_events = 0

        self.consumer = KafkaConsumer(
            args.input_topic_name,
            group_id=args.consumer_group,
            max_poll_records=args.max_poll_records,
            max_poll_interval_ms=args.max_poll_interval_ms,
        )
        self.producer = KafkaProducer(
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        self.INVALID_TIME = -1
        self.SEND_ALL_REMAINING_MESSAGES = -2

    def report_benchmark_stats(self, messages_consumed, read_time):
        """
        Reports some stats for benchmarking
        """
        print("Summary of all benchmarks:")
        print("The average read time of JSON: {}".format(self.json_avg_read_time))
        print(
            "The average individual message processing time: {}".format(
                self.message_avg_process_time
            )
        )
        print(
            "A total of {} messages were read in {} seconds, with an average frames per second of {}.".format(
                messages_consumed,
                read_time,
                messages_consumed / read_time
            )
        )

    def get_minutes_since_epoch(self, ts):
        """
        :param ts: timestamp in seconds (what comes from the record)
        :return: timestamp in mins (floor)
        """
        return int(ts / 60)

    def get_seconds_since_epoch(self, ts):
        """
        :param ts: timestamp in minutes
        :return: timestamp in seconds
        """
        return ts * 60

    def update_average(self, curr_time, new_time, events):
        """
        Calculates a running average of a timestamp and the amount of
        events when a new event comes in.
        """
        return (curr_time * (events - 1) / events) + new_time / events

    def latest_timestamp_valid(self, ts):
        """
        Takes the latest timestamp and see if it maps to any
        value in the intermediate dict. If not, it was an
        outlier.
        :param ts: The latest timestamp
        :return: Whether it is valid
        """
        is_valid = True
        for k, v in self.unique_users_per_min.items():
            is_valid = is_valid and len(self.unique_users_per_min[self.get_minutes_since_epoch(ts)]) != 1
        return is_valid

    def update_unique_users_per_minute(self, processed_records):
        """
        Given the current processed records from this poll loop,
        update the running count of users with a given timestamp
        from the information with these records
        :param processed_records: The records from this poll loop
        """
        for ts, users in processed_records.items():
            if ts in self.unique_users_per_min:
                self.unique_users_per_min[ts].update(users)
            else:
                self.unique_users_per_min[ts] = users

    def process_records_from_stream(self, records):
        """
        :param records: A list of records to be processed
        :return: The timestamp of the latest record, or self.INVALID_TIME if it is invalid (bit flip or other error)
        """
        processed_records = {}
        latest_timestamp = -1
        for line in records:
            try:
                start_time = time.time()
                json_line = json.loads(line)
                json_read_end_time = time.time()

                ts = json_line["ts"]
                latest_timestamp = max(ts, latest_timestamp)
                ts_min = self.get_minutes_since_epoch(ts)
                uid = json_line["uid"]

                if ts_min in processed_records:
                    processed_records[ts_min].add(uid)
                else:
                    processed_records[ts_min] = {uid}
                message_process_end_time = time.time()

                # Report the average running times of each stat
                self.json_read_time_events += 1
                self.message_process_time_events += 1
                self.json_avg_read_time = self.update_average(
                    self.json_avg_read_time,
                    json_read_end_time - start_time,
                    self.json_read_time_events,
                )
                self.message_avg_process_time = self.update_average(
                    self.message_avg_process_time,
                    message_process_end_time - start_time,
                    self.message_process_time_events,
                )

            except json.decoder.JSONDecodeError as d:
                self.errors.append(d)
                continue

        self.update_unique_users_per_minute(processed_records)
        return (
            latest_timestamp
            if self.latest_timestamp_valid(latest_timestamp)
            else self.INVALID_TIME
        )

    def send_single_message(self, ts, num_users):
        """
        Sends a single timestamp as well as the num users to either
        stdout or a kafka topic.
        """
        if self.args.output_to_stdout:
            print("The number of users for minute {} is {}.".format(ts, num_users))
        else:
            self.producer.send(
                self.args.output_topic_name, {"ts": ts, "n_users": num_users}
            )

    def send_messages_to_output(self, latest_ts):
        """
        Sends a record to the producer if the following conditions are met (for a given timestamp in mins and its users):

        - The time is not an invalid time (could happen due to outlier or no messages)
        - The len of the users is not one (could have been bitflipped)
        - The largest frame ts is 5 seconds after the minute range of the timestamp
        OR
        - This function has been instructed (by the self.SEND_ALL_REMAINING_MESSAGES flag) to
          simply send all messages (happens on a KeyboardInterrupt)
        This deletes the timestamp from the local store so it will not be sent again.

        :param latest_ts: The largest ts of a frame that was recv'd this poll loop
        """
        sent_timestamps = set()
        if latest_ts == self.INVALID_TIME:
            return
        for ts, users in self.unique_users_per_min.items():
            if latest_ts == self.SEND_ALL_REMAINING_MESSAGES:
                self.send_single_message(ts, len(users))
                sent_timestamps.add(ts)
            elif self.get_seconds_since_epoch(ts) + 65 < latest_ts and len(users) != 1:
                self.send_single_message(ts, len(users))
                sent_timestamps.add(ts)
        for ts in sent_timestamps:
            del self.unique_users_per_min[ts]

    def read_from_stdin(self):
        """
        Process messages from a standard input (a file)
        """
        total_messages_consumed = 0
        start_time = time.time()
        with open(self.args.input_file, "r") as f:
            records = f.readlines()
            total_messages_consumed = len(records)
            self.process_records_from_stream(records)

        end_time = time.time()

        if self.args.pytest_mode:
            return

        self.send_messages_to_output(self.SEND_ALL_REMAINING_MESSAGES)
        self.report_benchmark_stats(total_messages_consumed, end_time - start_time)
        self.consumer.close()
        self.producer.close()

    def read_from_kafka(self):
        """
        Process messages from a kafka topic
        """
        total_messages_consumed = 0
        start_time = time.time()
        try:
            while True:
                records = self.consumer.poll(timeout_ms=args.max_poll_interval_ms)

                for topic_partition, msgs in records.items():
                    if topic_partition.topic == self.args.input_topic_name:
                        values = list(map(lambda msg: msg.value, msgs))
                        latest_timestamp_message = self.process_records_from_stream(
                            values
                        )
                        self.consumer.commit()
                        total_messages_consumed += len(values)

                        self.send_messages_to_output(latest_timestamp_message)

                        # If in prod: Would store metric and display in Grafana
                        if (
                            not self.args.run_benchmark
                            and total_messages_consumed
                            % 50
                            * self.args.max_poll_records
                            == 0
                        ):
                            print("Read {} messages".format(total_messages_consumed))

                        if self.args.run_benchmark and total_messages_consumed > 200000:
                            return

        except KeyboardInterrupt:
            self.send_messages_to_output(self.SEND_ALL_REMAINING_MESSAGES)

        finally:
            end_time = time.time()
            self.report_benchmark_stats(total_messages_consumed, end_time - start_time)
            self.consumer.close()
            self.producer.close()

    def process_messages(self):
        """
        Entrypoint of the class, decides whether to read from stdin or kafka
        """
        if self.args.input_file != "":
            self.read_from_stdin()
        else:
            self.read_from_kafka()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_file",
        help="Which input file to read from (for testing from stdin)",
        default="",
    )
    parser.add_argument(
        "--run_benchmark",
        help="Whether to benchmark this run (stop after 200k messages read, print out stats)",
        action="store_true",
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

    args = parser.parse_args()
    map_reducer = KafkaMapReduce(args)
    map_reducer.process_messages()
