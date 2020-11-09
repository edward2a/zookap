#!/usr/bin/env python3

import argparse
import boto3
import lz4
import msgpack

from kazoo.client import KazooClient


p = argparse.ArgumentParser()
p.add_argument('-s', '--servers', required=True,
    help='Comma-list of zookeeper server:port')
p.add_argument('-d', '--dry-run', required=False, default=False, action='store_true',
    help='')
p.add_argument('-o', '--operation', required=True,choices=['backup', 'restore'],
    help='Whether to backup or restore')
p.add_argument('--start-path', required=False, default='/',
    help='The path from where to start the backup')

# restore options (TODO: subparser)
# differential restore: restore only missing


def prepare_backup(client, start_path='/'):
    targets = []
    next_target = [start_path]

    # do / separate to avoid duplicate // on the path
    if next_target[0] == '/':
        next_target.extend(
            client.get_children(next_target.pop()))

    while len(next_target) > 0:
        t = next_target.pop()
        targets.append(t)
        for child in client.get_children(t):
            next_target.append(t + '/' + child)

    return targets


def backup(client, config):
    targets = prepare_backup(client, config.start_path)
    print(targets)


def restore(client):
    pass


def main():
    args = p.parse_args()
    zk = KazooClient(hosts=args.servers)
    zk.start()
    backup(zk, args)


if __name__ == '__main__':
    main()

