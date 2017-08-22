#!/usr/bin/env python
# -*- coding: utf-8 -*-
import hashlib

import boto3

from .utils import build_logger, cached_property


logger = build_logger(__name__)

AWS_REGION = 'us-east-1'


class Janitor(object):

    def __init__(self, logger):
        self.logger = logger

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

    def get_messages_from_queue(self, queue_name, max_messages=10, wait_time=5):
        self.logger.debug('Fetching %s message(s) from %s (waiting up to %s secs)', max_messages, queue_name, wait_time)
        messages = self.sqs.receive_message(
            QueueUrl=self.queue_url(queue_name),
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=wait_time,
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

    def filter_messages(self, messages):
        """
        Override this method to filter which messages the target operation will be performed on.

        messages - a list of SQS message items

        Returns - a list of SQS message items
        """
        return messages

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

    def move(self, source_queue_name, dest_queue_name, max_messages=10, wait_time=2,
             remove_successful_from_source=False, max_total_messages=None):
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

        messages = self.get_messages_from_queue(source_queue_name, max_messages, wait_time)
        if not messages:
            self.logger.debug("No more messages available from source queue %s", source_queue_name)
        while messages:
            total_rcvd += len(messages)
            messages = self.filter_messages(messages)
            if not messages:
                self.logger.warn("No messages remain after filtering!")
            else:
                # for easier lookups later on
                messages_by_id = {message.get("MessageId"): message for message in messages}
                total_attempted += len(messages)
                # send em
                response = self.send_messages_to_queue(dest_queue_name, messages)
                successful = response.get('Successful', [])
                total_successful += len(successful)
                failed = response.get('Failed', [])
                total_failed += len(failed)
                # report failures
                for failure_body in failed:
                    self.logger.warn('Message failed to queue to %s: %s', dest_queue_name, failure_body)
                if successful and remove_successful_from_source:
                    # filter original messages down to successful messages - cause we need original ReceiptHandles
                    messages_successfully_sent = [messages_by_id.get(successful_message.get("Id")) for successful_message in successful]
                    # ...and now delete them
                    self.before_delete(messages_successfully_sent)
                    response_from_delete = self.remove_messages_from_sqs(source_queue_name, messages_successfully_sent)
                    self.after_delete(messages_successfully_sent, response_from_delete)
            # if we are supposed to exit after a certain number of messages, check it
            if max_total_messages and total_rcvd >= max_total_messages:
                self.logger.info("total received messages (%s) >= max_total_messages (%s) - exiting...", total_rcvd, max_total_messages)
                break
            # get more (maybe)
            messages = self.get_messages_from_queue(source_queue_name, max_messages, wait_time)
            if not messages:
                self.logger.debug("No more messages available from source queue %s", source_queue_name)
        self.logger.info("Received: %s - Attempted: %s - Successful: %s - Failed: %s",
                         total_rcvd, total_attempted, total_successful, total_failed)

    def remove(self, queue_name, max_messages=10, wait_time=2, max_total_messages=None):
        """
        Reads batches of messages of size <max_messages> from <source_queue_name> and (batch) deletes them.
        """
        total_rcvd = 0
        total_attempted = 0
        total_successful = 0
        total_failed = 0

        messages = self.get_messages_from_queue(queue_name, max_messages, wait_time)
        if not messages:
            self.logger.debug("No more messages available from source queue %s", queue_name)
        while messages:
            total_rcvd += len(messages)
            messages = self.filter_messages(messages)
            if not messages:
                self.logger.warn("No messages remain after filtering!")
            else:
                total_attempted += len(messages)
                self.before_delete(messages)
                response = self.remove_messages_from_queue(queue_name, messages)
                successful = response.get('Successful', [])
                total_successful += len(successful)
                failed = response.get('Failed', [])
                total_failed += len(failed)
                # report failures
                for failure_body in failed:
                    self.logger.warn('Delete message from %s failed: %s', queue_name, failure_body)
                self.after_delete(messages, response)
            # if we are supposed to exit after a certain number of messages, check it
            if max_total_messages and total_rcvd >= max_total_messages:
                self.logger.info("total received messages (%s) >= max_total_messages (%s) - exiting...", total_rcvd, max_total_messages)
                break
            # get more (maybe)
            messages = self.get_messages_from_queue(queue_name, max_messages, wait_time)
            if not messages:
                self.logger.debug("No more messages available from source queue %s", queue_name)
        self.logger.info("Received: %s - Attempted: %s - Successful: %s - Failed: %s",
                         total_rcvd, total_attempted, total_successful, total_failed)