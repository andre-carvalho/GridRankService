#!/usr/bin/python3
from threading import Thread
from rankcell_dao import RankCellDao
from input_observer_dao import InputObserverDao
import logging
import time

class RankThread(Thread):

    def __init__ (self, thread_id, input_table, output_table):
        Thread.__init__(self)
        self.thread_id = thread_id
        self.input_table = input_table
        self.output_table = output_table
        logging.basicConfig(filename='rank_thread.log',level=logging.INFO)

    def run(self):
        logging.info('Thread{0} running and using this params:{1}'.format(self.thread_id, self.input_table))
        load = InputObserverDao()
        db = RankCellDao()
        old_output_table = load.loadOldOutputTables(self.input_table, self.output_table)
        db.dropWeekdayTable(old_output_table)
        db.rankCells(self.input_table, self.output_table)
        logging.info('Thread{0}'.format(self.thread_id))
    
    def getId(self):
        return self.output_table