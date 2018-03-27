#!/usr/bin/python3
from input_observer_dao import InputObserverDao
from rank_thread import RankThread
import timing
import logging

def makeRankMultiThread():
    logging.basicConfig(filename='start_multi_thread.log',level=logging.CRITICAL)
    try:
        load = InputObserverDao()
        input_data = load.loadInputData()
        thread_number = 0
        thread_group = {}

        for data in input_data:
            input_table = {'scene_id':data[0],'path':data[1],'row':data[2],'date':data[3]}
            output_table = data[5]
            if output_table not in thread_group:
                thread_group[output_table]=[]
            thread = RankThread(thread_number, input_table, output_table)
            thread.start()
            thread_group[output_table].append(thread)
            thread_number += 1
        
        responses = []

        for group in thread_group:
            for th in thread_group[group]:
                th.join()
                responses.append(th.getId())
        
        logging.info(responses)

    except Exception as error:
        logging.critical(error)
    except BaseException as error:
        logging.critical(error)

makeRankMultiThread()