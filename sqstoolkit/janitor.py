#!/usr/bin/env python
# -*- coding: utf-8 -*-
import argparse
import hashlib
import time

import boto3

from .utils import build_logger, cached_property


logger = build_logger('sqstoolkit')

AWS_REGION = 'us-east-1'


class Janitor(object):

    def __init__(self, logger, args):
        self.logger = logger
        self.args = args
        self.interval_sleep = args.interval_sleep

    @cached_property
    def s3(self):
        return boto3.client('s3', region_name=AWS_REGION)

    @cached_property
    def sqs(self):
        return boto3.client('sqs', region_name=AWS_REGION)

    def queue_url(self, queue_name):
        """
        Return the queue URL for the queue with name <queue_name>
        """
        return self.sqs.get_queue_url(QueueName=queue_name).get('QueueUrl')

    def hash_it(self, raw):
        return hashlib.sha512(raw).hexdigest()

    def get_messages_from_queue(self, queue_name, max_messages=10):
        self.logger.debug('Fetching %s message(s) from %s (waiting up to %s secs)', max_messages, queue_name, self.args.sqs_wait_time)
        messages = self.sqs.receive_message(
            QueueUrl=self.queue_url(queue_name),
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=self.args.sqs_wait_time,
        ).get('Messages', [])
        self.logger.debug('Got %s message(s) from %s', len(messages), queue_name)
        return messages

    def send_messages_to_queue(self, queue_name, messages):
        """
            send multiple messages to an sqs queue

            queue_name - the name of the queue to send messages to (NOT the queue url)
            messages - list of messages to send; each object in this list should be a dictionary
                and have at least "MessageId" and "MessageBody" keys
        """
        self.logger.debug('Sending %s message(s) to %s', len(messages), queue_name)
        # self.logger.debug('Messages: %s', json.dumps(messages, indent=2, sort_keys=True))
        return self.sqs.send_message_batch(
            QueueUrl=self.queue_url(queue_name),
            Entries=[{
                'Id': message.get("MessageId"),
                'MessageBody': message.get('Body'),
            } for message in messages],
        )

    def remove_messages_from_queue(self, queue_name, messages):
        """
            remove multiple messages from sqs

            queue_name - the name of the queue to remove messages from (NOT the queue url)
            messages - list of messages to delete; each object in this list should be a dictionary
                and have at least "MessageId" and "ReceiptHandle" keys
        """
        self.logger.debug('Removing %s message(s) from %s', len(messages), queue_name)
        # self.logger.debug('Messages: %s', json.dumps(messages, indent=2, sort_keys=True))
        response = self.sqs.delete_message_batch(
            QueueUrl=self.queue_url(queue_name),
            Entries=[{
                'Id': message.get("MessageId"),
                'ReceiptHandle': message.get('ReceiptHandle'),
            } for message in messages],
        )
        if response.get('Failed'):
            self.logger.warn('Some SQS messages not deleted from queue %s - response: %s', queue_name, response)
        return response

    def reset_visibility_for_messages(self, queue_name, messages, visibility_timeout):
        """
            change visibility timeout for multiple messages from sqs

            queue_name - the name of the queue to remove messages from (NOT the queue url)
            messages - list of messages to change visibility timeout for; each object in this list should be a dictionary
                and have at least "MessageId" and "ReceiptHandle" keys
        """
        self.logger.debug('Resetting visibility timeout to %s seconds for %s message(s) in %s', visibility_timeout, len(messages), queue_name)
        response = self.sqs.change_message_visibility_batch(
            QueueUrl=self.queue_url(queue_name),
            Entries=[{
                'Id': message.get("MessageId"),
                'ReceiptHandle': message.get('ReceiptHandle'),
                'VisibilityTimeout': visibility_timeout,
            } for message in messages],
        )
        if response.get('Failed'):
            self.logger.warn("Some SQS messages' visibility timeouts not updated from queue %s - response: %s", queue_name, response)
        return response

    def filter_messages(self, messages):
        """
        Override this method to filter which messages the target operation will be performed on.

        messages - a list of SQS message items

        Returns
            messages - a list of SQS message items to process
            invalid_messages - a list of SQS message items to NOT process
        """
        return messages, []

    def before_delete(self, messages_to_delete):
        """
            Args
                messages - list of messages that will be passed to batch delete;
                    keep in mind that individual messages can fail to be deleted, while others may succeed
            Returns - ignored
        """
        pass

    def after_delete(self, messages_to_delete, response_from_batch_delete):
        """
            Args
                messages_to_delete - list of messages that were be passed to batch delete, NOT the
                    messages that were actually deleted - check response_from_batch_delete.get("Success", []) for successfully
                    deleted message Ids
                response_from_batch_delete - the response from the call to the SQS delete_message_batch() method; use this
                    to identify which messages that were successfully deleted (or not!)
            Returns - ignored
        """
        pass

    def move(self):
        """
        Reads batches of messages of size <max_messages> from <source_queue_name> and (batch) enqueues them to <dest_queue_name>.

        If remove_successful_from_source=True, messages successfully enqueued to <dest_queue_name> are removed from
        <source_queue_name>.

        If <max_total_messages> is not None, then processing will end after <max_total_messages> are read
        from <source_queue_name>, regardless of whether they were successfully processed or not.
        """
        total_rcvd = 0
        total_attempted = 0
        total_successful = 0
        total_failed = 0

        # grab the first batch, to kick off the processing loop
        messages = self.get_messages_from_queue(self.args.source_queue, max_messages=self.args.max_per_batch)
        if not messages:
            self.logger.debug("No more messages available from source queue %s", self.args.source_queue)
        while messages:
            total_rcvd += len(messages)
            messages, invalids = self.filter_messages(messages)
            if invalids and self.args.reset_visibility:
                self.reset_visibility_for_messages(self.args.source_queue, invalids, self.args.reset_visibility)
            if not messages:
                self.logger.warn("No messages remain after filtering!")
            else:
                # for easier lookups later on
                messages_by_id = {message.get("MessageId"): message for message in messages}
                total_attempted += len(messages)
                # send em
                response = self.send_messages_to_queue(self.args.destination_queue, messages)
                successful = response.get('Successful', [])
                total_successful += len(successful)
                failed = response.get('Failed', [])
                total_failed += len(failed)
                # report failures
                for failure_body in failed:
                    self.logger.warn('Message failed to queue to %s: %s', self.args.destination_queue, failure_body)
                if successful and self.args.remove_successful:
                    # filter original messages down to successful messages - cause we need original ReceiptHandles
                    messages_successfully_sent = [messages_by_id.get(successful_message.get("Id")) for successful_message in successful]
                    # ...and now delete them
                    self.before_delete(messages_successfully_sent)
                    response_from_delete = self.remove_messages_from_queue(self.args.source_queue, messages_successfully_sent)
                    self.after_delete(messages_successfully_sent, response_from_delete)
            # if we are supposed to exit after a certain number of messages, check it
            if self.args.max_total and total_rcvd >= self.args.max_total:
                self.logger.warn("total received messages (%s) >= --max-total (%s) - exiting...", total_rcvd, self.args.max_total)
                break
            self.logger.info("Received: %s - Attempted: %s - Successful: %s - Failed: %s",
                             total_rcvd, total_attempted, total_successful, total_failed)
            # get more (maybe)
            num_to_fetch = self.args.max_per_batch
            if self.args.max_total:
                num_to_fetch = min(num_to_fetch, self.args.max_total - total_rcvd)
            # if we should sleep between batches, do it now
            if self.interval_sleep:
                self.logger.debug("Sleeping for %s seconds before next batch..", self.interval_sleep)
                time.sleep(self.interval_sleep)
            messages = self.get_messages_from_queue(self.args.source_queue, max_messages=num_to_fetch)
            if not messages:
                self.logger.debug("No more messages available from source queue %s", self.args.source_queue)
        self.logger.info("Complete! Received: %s - Attempted: %s - Successful: %s - Failed: %s",
                         total_rcvd, total_attempted, total_successful, total_failed)

    def remove(self):
        """
        Reads batches of messages of size <max_messages> from <source_queue_name> and (batch) deletes them.
        """
        total_rcvd = 0
        total_attempted = 0
        total_successful = 0
        total_failed = 0

        messages = self.get_messages_from_queue(self.args.source_queue, self.args.max_per_batch)
        if not messages:
            self.logger.debug("No more messages available from source queue %s", self.args.source_queue)
        while messages:
            total_rcvd += len(messages)
            messages, invalids = self.filter_messages(messages)
            if invalids and self.args.reset_visibility:
                self.reset_visibility_for_messages(self.args.source_queue, invalids, self.args.reset_visibility)
            if not messages:
                self.logger.warn("No messages remain after filtering!")
            else:
                total_attempted += len(messages)
                self.before_delete(messages)
                response = self.remove_messages_from_queue(self.args.source_queue, messages)
                successful = response.get('Successful', [])
                total_successful += len(successful)
                failed = response.get('Failed', [])
                total_failed += len(failed)
                # report failures
                for failure_body in failed:
                    self.logger.warn('Delete message from %s failed: %s', self.args.source_queue, failure_body)
                self.after_delete(messages, response)
            # if we are supposed to exit after a certain number of messages, check it
            if self.args.max_total and total_rcvd >= self.args.max_total:
                self.logger.info("total received messages (%s) >= max_total_messages (%s) - exiting...", total_rcvd, self.args.max_total)
                break
            self.logger.info("Received: %s - Attempted: %s - Successful: %s - Failed: %s",
                             total_rcvd, total_attempted, total_successful, total_failed)
            # if we should sleep between batches, do it now
            if self.interval_sleep:
                self.logger.debug("Sleeping for %s seconds before next batch..", self.interval_sleep)
                time.sleep(self.interval_sleep)
            # get more (maybe)
            num_to_fetch = self.args.max_per_batch
            if self.args.max_total:
                num_to_fetch = min(num_to_fetch, self.args.max_total - total_rcvd)
            messages = self.get_messages_from_queue(self.args.source_queue, max_messages=num_to_fetch)
            if not messages:
                self.logger.debug("No more messages available from source queue %s", self.args.source_queue)
        self.logger.info("Completed! Received: %s - Attempted: %s - Successful: %s - Failed: %s",
                         total_rcvd, total_attempted, total_successful, total_failed)

    @classmethod
    def build_parser(cls):
        parser = argparse.ArgumentParser()
        parser.add_argument("source_queue", help="The source queue name (not URL!) to process messages from.")
        parser.add_argument("--dest", dest="destination_queue", help="The destination queue name (not URL!) to deliver messages "
                            "to (for those operations that require it).")
        parser.add_argument("--remove-successful", action="store_true", help="Remove messages from the source queue if they "
                            "were successfully processed. Note that this option might be ignored for some operations (eg, 'remove') "
                            "where this behavior is already implied.")
        parser.add_argument("--max-per-batch", default=10, type=int, help="The maximum number of messages to request from the source "
                            "queue at a time. Defaults to the SQS maximum of 10. If this value is greater than --max-total, it will be set to "
                            "the value of --max-total.")
        parser.add_argument("--max-total", default=1, type=int, help="The maximum number of messages to process. Defaults to %(default)s so you don't "
                            "shoot yourself in the foot.")
        parser.add_argument("--reset-visibility", default=None, type=int, help="If specified, this is the number of seconds to reset "
                            "the visibility timeout to for messages that were skipped due to filter criteria (if any). Defaults "
                            "to %(default)s, meaning the queue's default visibility timeout will apply.")
        parser.add_argument("--sqs-wait-time", default=3, type=int, help="Number of seconds to wait for messages in the source SQS queue. Defaults to %(default)s.")
        parser.add_argument("--interval-sleep", default=0, type=int, help="Number of seconds to pause between batches. Defaults to %(default)s.")
        parser.add_argument("--log-level", default="INFO", help="Sets the log level. Defaults to %(default)s")
        return parser

    @classmethod
    def parse_args(cls, parser=None):
        parser = parser or cls.build_parser()
        args, _ = parser.parse_known_args()
        logger.setLevel(args.log_level)
        if args.max_total and args.max_total < args.max_per_batch:
            args.max_per_batch = args.max_total
        return args