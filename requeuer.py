#!/usr/bin/env python
# -*- coding: utf-8 -*-
import hashlib
import json
import time

import boto3
import botocore

from utils import build_logger, cached_property


logger = build_logger()


class Requeuer(object):

    def __init__(self, logger):
        self.logger = logger

    @cached_property
    def s3(self):
        return boto3.client('s3', region_name=self.config.AWS_REGION)

    @cached_property
    def sqs(self):
        return boto3.client('sqs', region_name=self.config.AWS_REGION)

    def queue_url(self, queue_name):
        return self.sqs.get_queue_url(QueueName=queue_name).get('QueueUrl')

    def hash_it(self, raw):
        return hashlib.sha512(raw).hexdigest()

    def get_messages(self, queue_name, max_messages=10, wait_time=5):
        self.logger.debug('Fetching %s message(s) from %s (waiting up to %s secs)', max_messages, queue_name, wait_time)
        messages = self.sqs.receive_message(
            QueueUrl=self.queue_url(queue_name),
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=wait_time,
        ).get('Messages', [])
        self.logger.debug('Got %s message(s) from %s', len(messages), queue_name)
        return messages

    def send_messages(self, queue_name, entries):
        return self.sqs.send_message_batch(
            QueueUrl=self.queue_url(queue_name),
            Entries=[{
                'Id': _id,
                'MessageBody': message.get('Body'),
            } for _id, message in entries],
        )

    def remove_messages_from_sqs(self, queue_name, receipts):
        """
            remove multiple messages from sqs
        """
        response = self.sqs.delete_message_batch(
            QueueUrl=self.queue_url(queue_name),
            Entries=[{'Id': self.hash_it(r), 'ReceiptHandle': r} for r in receipts]
        )
        if response.get('Failed'):
            self.logger.warn('Some SQS messages not deleted from queue %s; full response: %s', queue_name, response)
        return response

    def queue_to_queue(self, source_queue_name, dest_queue_name, max_messages=10, wait_time=2,
                       remove_from_source=False, sleep=None, max_total_messages=None,
                       filter_fn=None):
        """
        Reads batches of messages of size <max_messages> from <source_queue_name> and (batch) enqueues them to <dest_queue_name>.

        If remove_from_source=True, messages successfully enqueued to <dest_queue_name> are removed from
        <source_queue_name>.

        If <sleep> is set to a positive number, then the loop will sleep(<sleep>) between each message
        fetch from <source_queue_name>.

        If <max_total_messages> is not None, then processing will end after <max_total_messages> are read
        from <source_queue_name>, regardless of whether they were successfully processed or not.

        filter_fn - a reference to a function; will get passed each message received from <source_queue_name>;
        should return True/False to indicate which messages should be queued to <dest_queue_name>.
        """
        total_rcvd_messages = 0
        total_attempted_sends = 0
        total_successful_sends = 0
        total_failed_sends = 0
        messages = self.get_messages(source_queue_name, max_messages, wait_time)
        while messages:
            total_rcvd_messages += len(messages)
            # if we got a filter_fn, filter the messages through it
            if filter_fn:
                messages = filter(filter_fn, messages)
            """
            build the entry dictionary for batch sending to the dest queue, where each entry looks like this:
                <hashed receipt handle> (for the batch send Id field): {
                    <message_body>,
                    <receipthandle> (of the retrieved message, so we can delete it from the source queue),
                }
            """
            entries = {
                self.hash_it(message.get('ReceiptHandle')): {
                    'MessageBody': message.get('Body'),
                    'ReceiptHandle': message.get('ReceiptHandle'),
                }
                for message in messages
            }
            # send em
            total_attempted_sends += len(entries)
            response = self.send_messages(dest_queue_name, entries)
            successful = response.get('Successful', [])
            total_successful_sends += len(successful)
            failed = response.get('Failed', [])
            total_failed_sends += len(failed)
            # report failures
            for failure_body in failed:
                self.logger.warn('Message failed to queue to %s: %s', dest_queue_name, failure_body)
            if remove_from_source:
                """
                build a list of the original ReceiptHandles that got successfully sent
                to the dest queue to batch delete from the source queue
                """
                receipts_to_delete = [
                    entries[message.get('Id')]['ReceiptHandle']
                    for message in successful
                ]
                # ...and now delete them
                if receipts_to_delete:
                    self.remove_messages_from_sqs(source_queue_name, receipts_to_delete)
            # if we are supposed to exit after a certain number of messages, check it
            if max_total_messages and total_rcvd_messages >= max_total_messages:
                self.logger.info("total received messages (%s) >= max_total_messages (%s) - exiting...", total_rcvd_messages, max_total_messages)
                break
            # if we're supposed to sleep between each call to get_messages(), then sleep
            if sleep:
                self.logger.debug("Sleeping for %s seconds...", sleep)
                time.sleep(sleep)
            # get the next batch to process
            messages = self.get_messages(source_queue_name, max_messages, wait_time)
            if not messages:
                self.logger.debug("No more messages available")
        self.logger.info("""
    Messages received: %s
    Attempted sends: %s
    Successful sends: %s
    Failed sends: %s""", total_rcvd_messages, total_attempted_sends, total_successful_sends, total_failed_sends)


class PostProcRequeuer(Requeuer):

    def fetch_from_s3(self, bucket, key):
        try:
            response = self.s3.get_object(Bucket=bucket, Key=key)
        except botocore.exceptions.ClientError, e:
            if e.response.get('Error').get('Code') == 'NoSuchKey':
                self.logger.debug("No such key! Bucket=%s Key=%s")
                # response = removeMessageFromSQS(message.get("ReceiptHandle"))
                # self.logger.debug("Removed message from SQS - {0}.".format(response))
                return None
        return json.load(response.get('Body'))

    def is_valid_message(self, message):
        """
        ensure that the doc referenced by this message has valid values for it's published, updated and content keys
        ...which means fetching the whole damn thing from S3 :/
        """
        body = json.loads(message.get('Body'))
        message_record = body.get('Records')[0]
        bucket = message_record.get('s3').get('bucket').get('name')
        key = message_record.get('s3').get('object').get('key')
        doc = self.fetch_from_s3(Bucket=bucket, Key=key)
        if doc is None:
            """
            a not-so-edge edge case: the referenced S3 doc is no longer in S3, so there's nothing to process ...which
            means this message is an orphan, so don't copy it - and we'll delete it right now
            """
            self.logger.error("Message references a nonexistent S3 object - ReceiptHandle: %s", message.get('ReceiptHandle'))
            # remove_messages_from_sqs()
            return False
        if doc.get('published', '').strip() == '':
            self.logger.info('Message doc had invalid "published" field: "%s" - rejecting...', doc.get('published'))
            return False
        if doc.get('updated', '').strip() == '':
            self.logger.info('Message doc had invalid "updated" field: "%s" - rejecting...', doc.get('updated'))
            return False
        if not isinstance(doc.get('content'), basestring):
            self.logger.info('Message doc had invalid "content" field type: "%s" - rejecting...', type(doc.get('content')))
            return False
        return True


'''
def test(queue_name):
    max_messages = 1
    wait_time = 5
    logger.debug('Reading %s message(s) from %s (waiting up to %s secs)' % (max_messages, queue_name, wait_time))
    messages = sqs.receive_message(
        QueueUrl=queue_url(queue_name),
        MaxNumberOfMessages=max_messages,
        WaitTimeSeconds=wait_time,
    ).get("Messages", [])
    logger.debug("Messages received: %s", json.dumps(messages, indent=2, sort_keys=True))
    for message in messages:
        receipt = message.get('ReceiptHandle')
        logger.debug("Setting message visibility to 1 min for receipt '%s...' on queue %s", receipt[:20], queue_name)
        sqs.change_message_visibility(
            QueueUrl=queue_url(queue_name),
            ReceiptHandle=receipt,
            VisibilityTimeout=60,   # seconds
        )
        # logger.debug(response)
'''


if __name__ == '__main__':
    dest_queue_name = 'production-sqs-PostProcessorQueue-1LVDUKHYJU06J'
    source_queue_name = 'production-sqs-PostProcessorQueueDL-4Z1PCQPVOU5X'
    # test(source_queue_name)
    requeuer = PostProcRequeuer(logger)
    requeuer.queue_to_queue(
        source_queue_name=source_queue_name,
        dest_queue_name=dest_queue_name,
        max_messages=1,
        max_total_messages=1,
        filter_fn=requeuer.is_valid_message,
        remove_from_source=True,
    )
