'''
getData.py - used for pulling down the desired data for statistical analysis from s3
Ben Brittain (bbrittain)
'''

import sys
import os
import json as json
import argparse
import Queue
import threading
from backports import lzma # pip install backports.lzma (need liblzma-dev package)

from datetime import datetime
from boto.s3.connection import S3Connection
from boto.s3.key import Key

# Global Queue to manage the network threads
inqueue = Queue.Queue()
outqueue = Queue.Queue()

class PrintThread(threading.Thread):
    def __init__(self, queue, date):
        threading.Thread.__init__(self)
        self.outqueue = outqueue
        self.fout = open(date +'.out', 'w')
    def run(self):
        while True:
            result = self.outqueue.get()
            self.fout.write(result + "\n")
            self.outqueue.task_done()

class RelevantData(threading.Thread):
  """Threaded Key Acqusition"""
  def __init__(self, inqueue, outqueue):
    threading.Thread.__init__(self)
    self.inqueue = inqueue
    self.outqueue = outqueue
    self.conn = S3Connection()
    self.bucket = self.conn.get_bucket('telemetry-published-v1')

  def run(self):
    while True:
      key = self.inqueue.get()

      k = Key(self.bucket)
      k.key=key
      keyContents = k.get_contents_as_string()
      lz = lzma.decompress(keyContents, format=lzma.FORMAT_ALONE)

      for line in lz.split('\n'):
        if line != '': 
            self.outqueue.put(line)
      self.inqueue.task_done()

class Job:
  """A class for acquireing the Telemetry Data"""
  def __init__(self, config):
    self.date = config.date
    self.bucket = 'telemetry-published-v1'
    self.allowed_values = dict()
    # hardcoded for ekr
    self.allowed_values[0] = 'loop'
    self.allowed_values[1] = 'Firefox'
    self.allowed_values[2] = 'OTHER'
    self.allowed_values[3] = '*'
    self.allowed_values[4] = '*'
    self.allowed_values[5] = config.date

  def get_filtered_files_s3(self):
    out_files = []
    print "Fetching file list from S3..."
    conn = S3Connection()
    bucket = conn.get_bucket(self.bucket)
    start = datetime.now()
    count = 0
    for f in self.list_partitions(bucket, include_keys=True):
      self.fetch_and_sanatize(f)
      count += 1
      out_files.append(f)
    conn.close()
    end = datetime.now()
    time = end-start
    print "Acquired knowledge of " + str(count) + " files in " + str(time) + " secs"
    print "still waiting on seperation of requested data..."

  def list_partitions(self, bucket, prefix='', level=0, include_keys=False):
      delimiter = '/'
      if level > 3:
          delimiter = '.'
      for k in bucket.list(prefix=prefix, delimiter=delimiter):
          partitions = k.name.split("/")
          if level > 3:
              # the last couple of partition is split by "." instead of "/" for some reason
              partitions.extend(partitions.pop().split(".", 2))
          if (self.allowed_values[level] == '*') or (partitions[level] == self.allowed_values[level]):
              if level >= 5:
                  if include_keys:
                      for f in bucket.list(prefix=k.name):
                          yield f
                  else:
                      yield k.name
              else:
                  for prefix in self.list_partitions(bucket, k.name, level + 1, include_keys):
                      yield prefix

  def fetch_and_sanatize(self, fin):
    inqueue.put(fin)
    # TODO stores data in a Platform/measurment/day setup
    pass


def main():
  parser = argparse.ArgumentParser(description='Get Telemetry Data.',
      formatter_class=argparse.ArgumentDefaultsHelpFormatter)
  parser.add_argument("date", help="Day to get data for (yyyymmdd)")
  args = parser.parse_args()

  job = Job(args)

  for i in range(10):
    t = RelevantData(inqueue, outqueue)
    t.setDaemon(True)
    t.start()

  thread = PrintThread(outqueue, args.date)
  thread.setDaemon(True)
  thread.start()

  job.get_filtered_files_s3()

  inqueue.join()
  outqueue.join()
  print("Finished getting data")

if __name__ == "__main__":
  main()
