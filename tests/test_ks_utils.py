import json

from ks_utils import __version__


def test_version():
    assert __version__ == '0.1.0'


def _get_env_data_as_dict(path: str) -> dict:
    with open(path, 'r') as f:
        return dict(tuple(line.replace('\n', '').split('='))
                    for line in f.readlines() if not line.startswith('#'))


def __init_mns():
    import os
    from ks_utils.aliyun.mns.client import MNSClient

    env = _get_env_data_as_dict('./.env')
    for k, v in env.items():
        os.environ[k] = v

    return MNSClient()


def test_msn_send():
    c = __init_mns()
    resp = c.send_message(queue_name='asics-runplus-log',
                          msg_body=json.dumps({'test': '0002'}),
                          delay_seconds=10)
    print(resp.message_id)
    print(resp.message_body_md5)
    assert resp.message_id is not None
    assert resp.message_body_md5 is not None


def test_msn_receive():
    c = __init_mns()
    resp = c.receive_message(queue_name='asics-runplus-log')
    assert resp is not None

    print(resp.message_id)
    print(resp.message_body_md5)
    print(resp.message_body)
    print(resp.receipt_handle)

    c.delete_message(queue_name='asics-runplus-log', receipt_handle=resp.receipt_handle)

    assert resp.message_id is not None
    assert resp.message_body_md5 is not None
