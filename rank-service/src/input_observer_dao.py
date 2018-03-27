#!/usr/bin/python3
from psqldb import PsqlDB
from configmodel import ConfigModel
from app_exceptions import DatabaseError, MissingParameterError

class InputObserverDao:

    #constructor
    def __init__(self):
        self.db = PsqlDB()
        self.__loadConfigurations()


    def __loadConfigurations(self):
        cfg = ConfigModel()
        cfg_data = cfg.get()
        self.input_prefix = cfg_data['input_prefix']
        self.input_sufix = cfg_data['input_sufix']
        self.default_schema = cfg_data['default_schema']
        self.ta_schema = cfg_data['ta_schema']
        self.ta_scene = cfg_data['ta_scene']


    """
        Load scene id, input and output tables from postgres system table and terraamazon table
    """
    def loadInputData(self):

        sql = "SELECT ta.scene_id, ta.path, ta.row, ta.date, ta.view_date, ta.output_table FROM "
        sql += "( "
        sql += "SELECT sc.id as scene_id, sc.view_date, "
        sql += "split_part(sc.ai_object_id, '/', 1) as path, split_part(sc.ai_object_id, '/', 2) as row, "
        sql += "to_char(sc.view_date, 'DDMMYYYY') as date, "
        sql += "lower('"+self.default_schema+"."+self.input_prefix+"' || '_' || split_part(sc.ai_object_id, '/', 1) || "
        sql += "split_part(sc.ai_object_id, '/', 2) || '_' || to_char(sc.view_date, 'DDMMYYYY') || '_' || '"+self.input_sufix+"') as input_table, "
        sql += "CASE "
        sql += "WHEN (EXTRACT(ISODOW FROM sc.view_date) = 1) THEN 'segunda' "
        sql += "WHEN (EXTRACT(ISODOW FROM sc.view_date) = 2) THEN 'terca' "
        sql += "WHEN (EXTRACT(ISODOW FROM sc.view_date) = 3) THEN 'quarta' "
        sql += "WHEN (EXTRACT(ISODOW FROM sc.view_date) = 4) THEN 'quinta' "
        sql += "WHEN (EXTRACT(ISODOW FROM sc.view_date) = 5) THEN 'sexta' "
        sql += "WHEN (EXTRACT(ISODOW FROM sc.view_date) = 6) THEN 'sabado' "
        sql += "WHEN (EXTRACT(ISODOW FROM sc.view_date) = 7) THEN 'domingo' "
        sql += "END AS output_table "
        sql += "FROM "+self.ta_schema+"."+self.ta_scene+" as sc "
        sql += ") as ta, "
        sql += "( "
        sql += "SELECT lower('"+self.default_schema+".' || table_name) as input_table "
        sql += "FROM information_schema.tables "
        sql += "WHERE table_schema = '"+self.default_schema+"' AND table_name ilike '"+self.input_prefix+"%"+self.input_sufix+"' "
        sql += ") as sys "
        sql += "WHERE sys.input_table=ta.input_table"
        
        try:
            self.db.connect()
            data = self.db.fetchData(sql)
        except Exception as error:
            raise DatabaseError('Database error:', error)
        finally:
            self.db.close()
        
        return data

    """
        Load old output tables from postgres system table.
        
        The format to parameter table_identify is a dictionary like this: {'path':value,'row':value,'date':value}
        The parameter output_weekday_table is the weekday name: segunda, terca, quarta, quinta, sexta, sabado, domingo

        The output table name should following the pattern: {path}_{row}_%_{weekday}
        Return the table name otherwise raise an error MissingParameterError exception.
    """
    def loadOldOutputTables(self, table_identify, output_weekday_table):

        if table_identify is None:
            raise MissingParameterError('Get output tables','Parameter table_identify is missing')
        if output_weekday_table is None:
            raise MissingParameterError('Get output tables','Parameter output_weekday_table is missing')

        weekday_table = "%_{0}{1}_%_{2}".format(table_identify['path'], table_identify['row'], output_weekday_table)
        
        sql = "SELECT lower('{0}.' || table_name) as input_table ".format(self.default_schema)
        sql += "FROM information_schema.tables "
        sql += "WHERE table_schema = '{0}' AND table_name ilike '{1}' ".format(self.default_schema, weekday_table)
        
        try:
            self.db.connect()
            data = self.db.fetchData(sql)
        except Exception as error:
            raise DatabaseError('Database error:', error)
        finally:
            self.db.close()
        
        return data