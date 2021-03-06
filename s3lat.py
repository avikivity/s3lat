#!/usr/bin/python3

import boto3
import sys
import random
import hdrh.histogram
import time
import argparse
import multiprocessing

parser = argparse.ArgumentParser('s3 latency tester')
parser.add_argument('bucket')
parser.add_argument('object')
parser.add_argument('--iterations', type=int, default=100)
parser.add_argument('--concurrency', type=int, default=1)
args = parser.parse_args()

bucket = args.bucket
object = args.object
iterations = args.iterations
fetch_size = 512

s3 = boto3.client('s3')

hist = hdrh.histogram.HdrHistogram(1, 10000, 2)

rand_seeded = False

size = s3.get_object_attributes(Bucket=bucket, Key=object,
                                ObjectAttributes=['ObjectSize'])['ObjectSize']
if size < fetch_size:
    print("object size too small")
    sys.exit(1)

def request_latency(id):
    global rand_seeded
    if not rand_seeded:
        random.seed()
        rand_seeded = True
    offset = random.randrange(size - fetch_size)
    t1 = time.monotonic()
    fetch = s3.get_object(Bucket=bucket, Key=object,
                          Range=f'bytes={offset}-{offset+fetch_size-1}')
    fetch['Body'].read()
    t2 = time.monotonic()
    delta = t2 - t1
    return delta

with multiprocessing.Pool(processes=args.concurrency) as pool:
    for delta in pool.imap_unordered(request_latency, range(iterations)):
        hist.record_value(int(delta*1000))

def p(pct):
    return hist.get_value_at_percentile(pct)

print(f'''\
  10th percentile {p(10)}
  50th percentile {p(50)}
  75th percentile {p(75)}
  90th percentile {p(90)}
  95th percentile {p(95)}
  99th percentile {p(99)}
99.9th percentile {p(99.9)}
''')
    
