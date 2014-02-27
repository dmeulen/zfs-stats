#!/usr/bin/env python

import time
import socket
import logging
import subprocess
from argparse import ArgumentParser

log = logging.getLogger('zfs-graphite')

parser = ArgumentParser()
parser.add_argument('--hostname', '-H', help="Hostname to report to carbon", default="localhost")
parser.add_argument('--carbon-server', '-c', help="Carbon server", default="localhost")
parser.add_argument('--carbon-port', '-p', help="Corbon port", type=int, default="2003")
parser.add_argument('--interval', '-i', help="Check interval in seconds", type=int, default=10)
parser.add_argument('--verbose', '-v', help="Be verbose, useful for debugging", action="store_true")

def main():
    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)

    p_zfs_vols = subprocess.Popen(["/sbin/zfs", "list", "-H", "-o", "name"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    zfs_vols = [y for y in (x.strip() for x in p_zfs_vols.communicate()[0].splitlines()) if y]

    cmd = ["/sbin/zfs", "get", "-p", "-H", "-o", "name,property,value", "used,usedbydataset,available,referenced,compressratio"] + zfs_vols

    while True:
        sock = socket.socket()
        sock.settimeout(5.0)

        try:
            sock.connect((args.carbon_server, args.carbon_port))
        except:
            log.debug("Could not connect to graphite on {}:{}".format(args.carbon_server, args.carbon_port))
            time.sleep(2)
            continue

        while True:
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            zfs_stats, stderr = p.communicate()

            for l in zfs_stats.splitlines():
                filesystem, key, value = l.split()
                log.debug("{}.zfs.{}.{} {} {}".format(args.hostname.replace('.','_'), filesystem.replace('/', '.'), key, value.replace('x',''), int(time.time())))
                metrics = "{}.zfs.{}.{} {} {}\n".format(args.hostname.replace('.','_'), filesystem.replace('/', '.'), key, value.replace('x',''), int(time.time()))
                try:
                    sock.sendall(metrics)
                except:
                    log.debug("Could not send metrics...")
                    break

            else:
                time.sleep(args.interval)
                continue
            break


if __name__ == '__main__':
        main()
