import logging
from os import environ

from mns.account import Account
from mns.mns_exception import MNSExceptionBase
from mns.queue import Queue, Message

logger = logging.getLogger(__name__)


class MNSClient:

    def __init__(self):
        self.endpoint = environ.get('MNS_ENDPOINT')
        self.access_id = environ.get('MNS_ACCESS_ID')
        self.access_key = environ.get('MNS_ACCESS_KEY')
        if not self.endpoint or not self.access_id or not self.access_key:
            raise Exception('missing endpoint, access_id or access_key')

        self.account = Account(self.endpoint, self.access_id, self.access_key)

    def send_message(self, queue_name: str, msg_body: str, delay_seconds: int = -1):
        try:
            q: Queue = self.account.get_queue(queue_name=queue_name)
            msg = Message(message_body=msg_body, delay_seconds=delay_seconds)
            return q.send_message(msg)
        except MNSExceptionBase as err:
            logger.error(err)

    def receive_message(self, queue_name: str, wait_seconds: int = -1):
        try:
            q: Queue = self.account.get_queue(queue_name=queue_name)
            return q.receive_message(wait_seconds=wait_seconds)
        except MNSExceptionBase as err:
            logger.error(err)

    def delete_message(self, queue_name: str, receipt_handle):
        try:
            q: Queue = self.account.get_queue(queue_name=queue_name)
            q.delete_message(receipt_handle=receipt_handle)
        except MNSExceptionBase as err:
            logger.error(err)
