#!/usr/bin/python3
import atexit
from time import time, strftime, localtime
from datetime import timedelta
import logging

def secondsToStr(elapsed=None):
    if elapsed is None:
        return strftime("%Y-%m-%d %H:%M:%S", localtime())
    else:
        return str(timedelta(seconds=elapsed))

def log(s, elapsed=None):
    line = "="*40
    logging.info(line)
    logging.info(secondsToStr()+'-'+s)
    if elapsed:
        logging.info("Elapsed time:{0}".format(elapsed))
    logging.info(line)

def endlog():
    end = time()
    elapsed = end-start
    log("End Program", secondsToStr(elapsed))

logging.basicConfig(filename='timing.log',level=logging.INFO)
start = time()
atexit.register(endlog)
log("Start Program")