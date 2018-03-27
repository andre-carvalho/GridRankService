#!/usr/bin/python3
from rankcell_dao import RankCellDao
from input_observer_dao import InputObserverDao
import timing
import logging

def makeRankSingleThread():
    logging.basicConfig(filename='start_single_thread.log',level=logging.CRITICAL)
    try:
        load = InputObserverDao()
        input_data = load.loadInputData()

        for data in input_data:
            input_table = {'scene_id':data[0],'path':data[1],'row':data[2],'date':data[3]}
            db = RankCellDao()
            old_output_table = load.loadOldOutputTables(input_table, data[5])
            db.dropWeekdayTable(old_output_table)
            db.rankCells(input_table, data[5])

    except Exception as error:
        logging.critical(error)
    except BaseException as error:
        logging.critical(error)

makeRankSingleThread()