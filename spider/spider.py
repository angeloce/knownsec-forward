#coding:utf-8

import time
import threading

import Queue
import logging
import urllib
import urllib2
from BeautifulSoup import BeautifulSoup
import sqlite3 as sqlite

LOGGER = logging.getLogger(__name__)

class Worker(threading.Thread):
    def __init__(self, queue, timeout=2000):
        threading.Thread.__init__(self)
        self.timeout = timeout
        self.queue = queue

    def run(self):
        while 1:
            url = self.queue.get(timeout=self.timeout)
            self.get_page(url)

    def get_page(self, url):
        try:
            res = urllib2.urlopen(url)
        except Exception, e:
            LOGGER.error('GET PAGE FAILED: [%s] %s'%(url, e.message))
            return
        dom = BeautifulSoup(res.read())
        atags = dom('a')
            
        print url, res.getcode()
        for a in atags:
            print a.attrs
    

class WorkerPool(object):
    def __init__(self, worker_count, timeout=2000):

        self.queue = Queue.Queue()
        self._workers = [Worker(self.queue, timeout) for i in range(worker_count)]

        
    
    def start_work(self, url, deep, store_path):
        for worker in self._workers:
            worker.start()

        self.queue.put(url)
        while 1:
            time.sleep(10)
    


class WebSpider(object):
    
    def __init__(self, url, deep, store_path, concurrency=10):
        self.url = url
        self.deep = deep
        self.store_path = store_path
        self.workpool = WorkerPool(concurrency)
    
    def _set_logger(self, filepath, level):
        handler = logging.Handler()
        handler.setFormatter(logging.Formatter('%(asctime)s %(message)s'))
        LOGGER.addHandler(handler)
    
    def run(self, **kw):
        keyword = kw.get('key', None)
        logfile = kw.get('logfile', 'spider.log')
        loglevel = kw.get('level', 1)
        self._set_logger(logfile, loglevel)
        
        self.workpool.start_work(self.url, self.deep, self.store_path)
        
        
def main():
    WebSpider("http://www.sina.com", 2, 1, 10).run()



if __name__ == '__main__':
    main()

        
        
