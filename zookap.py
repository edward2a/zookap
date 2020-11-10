#!/usr/bin/env python3

# Zookap - Zookeeper Backups for human beings
# Copyright (C) 2020 Eduardo A. Paris Penas <epp@realreadme.com>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import argparse
import boto3
import lz4.frame
import msgpack

from kazoo.client import KazooClient


p = argparse.ArgumentParser()
p.add_argument('-s', '--servers', required=True,
    help='Comma-list of zookeeper server:port')
p.add_argument('-d', '--dry-run', required=False, default=False, action='store_true',
    help='')
# TODO: operation will be a subparser, leaving this code to remember
#p.add_argument('-o', '--operation', required=True,choices=['backup', 'restore'],
#    help='Whether to backup or restore')
p.add_argument('--start-path', required=False, default='/',
    help='The path from where to start the backup')

# backup options
p.add_argument('-t', '--backup-target', required=False, default='zoo-snap.mpk.lz4',
    help='The output file for the backup')

# restore options (TODO: subparser)
# differential restore: restore only missing


def prepare_backup(client, start_path='/'):
    targets = []
    next_target = [start_path]

    # do root path separate to avoid duplicate // on the child nodes
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
    chunk_size = 131072 # 128K
    targets = prepare_backup(client, config.start_path)
    # TODO: remove print, only for development debugging
    print(targets)


    c_ctx = lz4.frame.create_compression_context()
    z = lz4.frame.compress_begin(c_ctx,
        compression_level=lz4.frame.COMPRESSIONLEVEL_MINHC)

    with open(config.backup_target, 'w+b') as f:
        f.write(':<date>:msgpack:lz4-chunk\n'.encode())

        while len(targets) > 0:
            while len(z) < chunk_size and len(targets) > 0:
                t = targets.pop()
                o = client.get(t)

                # Store serialised/compressed dict with node and data
                z += lz4.frame.compress_chunk(c_ctx, msgpack.packb(
                    {'n': t, 'd': o[0]}) + b'\n')

            else:
                z += lz4.frame.compress_flush(c_ctx)
                f.write(z)

    return True


def restore(client):
    pass


def main():
    args = p.parse_args()
    zk = KazooClient(hosts=args.servers)
    zk.start()

    backup(zk, args)


if __name__ == '__main__':
    main()

