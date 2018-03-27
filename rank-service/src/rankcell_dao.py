#!/usr/bin/python3
from psqldb import PsqlDB
from configmodel import ConfigModel
from app_exceptions import DatabaseError, MissingParameterError


# The RankCell Data Access Object handles all interactions with the RankCell table.
class RankCellDao:

    #constructor
    def __init__(self):
        self.db = PsqlDB()
        self.__loadConfigurations()


    def __loadConfigurations(self):
        cfg = ConfigModel()
        cfg_data = cfg.get()
        self.input_prefix = cfg_data['input_prefix']
        self.input_sufix = cfg_data['input_sufix']
        self.output_cloud_prefix = cfg_data['output_cloud_prefix']
        self.output_rank_prefix = cfg_data['output_rank_prefix']
        self.jobber_sufix = eval(cfg_data['jobber_sufix'])
        self.default_schema = cfg_data['default_schema']
        self.sequence = cfg_data['sequence']
        self.cell_table = cfg_data['cell_table']
        self.grid_table = cfg_data['grid_table']
        self.identify_cells = cfg_data['identify_cells']
        self.ta_schema = cfg_data['ta_schema']
        self.ta_scene = cfg_data['ta_scene']
        self.ta_tasklogs = cfg_data['ta_tasklogs']


    """
        Start process to one input table

        The format to parameter input_values is a dictionary like this: {'scene_id':value,path':value,'row':value,'date':value}
        The parameter output_weekday_table is the table name where result of intersection is stored.

        No return value but in error raise a DatabaseError exception.
        
        Warning: This method opens connection, run the process and close connection.
    """
    def rankCells(self, input_values, output_weekday_table):
        input_table_name = self.getInputTable(input_values)
        output_table_name = self.getOutputTable(input_values, output_weekday_table)

        try:
            self.db.connect()
            self.__createSequence(input_values['path'], input_values['row'])

            self.__intersectCellAndGrid(input_table_name, input_values['path'], input_values['row'])
            self.__intersectCloudAndGrid(input_table_name, input_values['scene_id'])
            self.__multiToSingle(input_table_name)
            self.__createCloudOutputTable(input_table_name, output_table_name, input_values['scene_id'])
            self.__createRankOutputTable(input_table_name, output_table_name)

            self.__dropSequence()
            self.db.commit()

            self.dropIntermediaryTables(input_values)

        except BaseException as error:
            raise error
        finally:
            self.db.close()


    """
        Drop weekday tables for old ranking process to cloud and rank tables.
        
        Parameter weekday_table is a set of old rank tables and old cloud tables.

        No return value but in error raise a DatabaseError exception.

        Warning: This method opens connection, run the process and close connection.
    """
    def dropWeekdayTable(self, weekday_table):
        try:
            self.db.connect()
            
            for table in weekday_table:
                sql = 'DROP TABLE IF EXISTS {0}'.format(table[0])
                self.__basicExecute(sql)
                self.db.commit()

        except Exception as error:
            raise DatabaseError('Database error:', error)
        finally:
            self.db.close()


    """
        Drop intermediary tables from the database.

        The format to parameter table_identify is a dictionary like this: {'path':value,'row':value,'date':value}

        No return value but in error raise a DatabaseError exception.
        Warning: This method opens connection, run the process and close connection.
    """
    def dropIntermediaryTables(self, table_identify):

        drop_table = "DROP TABLE IF EXISTS "
        
        table = self.getInputTable(table_identify)

        try:
            self.db.connect()
            for key in self.jobber_sufix:
                sql = '{0}{1}_{2}'.format(drop_table, table, self.jobber_sufix[key])
                self.db.execQuery(sql)
            self.db.commit()
        except Exception as error:
            self.db.rollback()
            raise DatabaseError('Database error:', error)
        finally:
            self.db.close()

    """
        Intersection between cell table 'intersect_ibge100k_cerrado' and scene table 'grade_cbers4_awfi_cerrado'
    """
    def __intersectCellAndGrid(self, table_name, path, row):

        sql = "CREATE TABLE "+table_name+"_"+self.jobber_sufix['tb2']+" AS "
        sql += "SELECT tb1.ta_cell, tb1.cell_area_km2, tb1.cell_scn_id, tb1.cell_kernel, tb1.geometries "
        sql += "FROM "+self.default_schema+"."+self.cell_table+" as tb1 "
        sql += "INNER JOIN "+self.default_schema+"."+self.grid_table+" as tb2 ON ST_Intersects(tb1.geometries, tb2.ogr_geometry) "
        sql += "WHERE NOT ST_IsEmpty(ST_Buffer(ST_Intersection(tb1.geometries, tb2.ogr_geometry),0.0)) "
        sql += "AND tb2.path = "+path+" AND tb2.row = "+row

        self.__basicExecute(sql)
    

    """
        Intersection (cloud table '<input_prefix>_PATH_ROW_DATE' and scene_id='SCENE' read from query that defines the input and output tables)
        
    """
    def __intersectCloudAndGrid(self, table_name, scene_id):
        sql = "CREATE TABLE "+table_name+"_"+self.jobber_sufix['tb1']+" AS "
        sql += "SELECT nextval('"+self.default_schema+"."+self.sequence+"'::regclass) as cloud_id, tb1.cell_scn_id, "
        sql += "'nuvem'::varchar as class_name, "+str(scene_id)+"::integer as scene_id, "
        sql += "ST_Buffer(ST_Intersection(tb1.geometries, tb2.ogr_geometry),0.0) as geometries "
        sql += "FROM "+table_name+"_"+self.jobber_sufix['tb2']+" as tb1 "
        sql += "INNER JOIN "+table_name+" as tb2 ON ST_Intersects(tb1.geometries, tb2.ogr_geometry) "
        sql += "WHERE NOT ST_IsEmpty(ST_Buffer(ST_Intersection(tb1.geometries, tb2.ogr_geometry),0.0))"

        self.__resetSequence()
        self.__basicExecute(sql)
        
    """
        From multipolygons to polygons
    """
    def __multiToSingle(self, table_name):
        sql = "CREATE TABLE "+table_name+"_"+self.jobber_sufix['tb3']+" AS "
        sql += "SELECT nextval('"+self.default_schema+"."+self.sequence+"'::regclass) as fid, cell_scn_id, class_name, scene_id, "
        sql += "'0'::double precision as cloud_area_km2, (ST_Dump(geometries)).geom as geometries "
        sql += "FROM "+table_name+"_"+self.jobber_sufix['tb1']

        self.__resetSequence()
        self.__basicExecute(sql)

    """
        Update polygon area
    """
    def __updateArea(self, table_name):
        sql = "UPDATE "+table_name+"_"+self.jobber_sufix['tb3']+" SET "
        sql += "cloud_area_km2=( ST_Area(geography( ST_Transform(geometries, 4326) ) ) )/1000000"
        
        self.__basicExecute(sql)


    """
        Putting clouds into weekday table.

        The weekday_table parameter is the output table name and should following the pattern: {path}{row}_{date}_{weekday}
        The scene_id parameter is the identifier for one scene of the cbers4 satellite in terraamazon database model.

        (cell_oid, class_name, scene_id, task_id, spatial_data) 
    """
    def __createCloudOutputTable(self, input_table, weekday_table, scene_id):
        sql = "CREATE TABLE "+self.default_schema+"."+self.output_cloud_prefix+"_"+weekday_table+" AS "
        sql += "SELECT nextval('"+self.default_schema+"."+self.sequence+"'::regclass) as cl_id, "
        sql += "tb2.cell_ta as cell_oid, tb1.class_name, tb3.id as scene_id, (0)::integer as task_id, tb1.geometries as spatial_data "
        sql += "FROM "+input_table+"_"+self.jobber_sufix['tb3']+" as tb1, "
        sql += self.default_schema+"."+self.identify_cells+" as tb2, "
        sql += self.ta_schema+"."+self.ta_scene+" as tb3 "
        sql += "WHERE tb1.cell_scn_id=tb2.cell_id AND tb3.id="+str(scene_id)
        
        self.__resetSequence()
        self.__basicExecute(sql)


    """
        Creating the final table with cells ranking.
    """
    def __createRankOutputTable(self, table_name, weekday_table):
        sql = "CREATE TABLE "+self.default_schema+"."+self.output_rank_prefix+"_"+weekday_table+" AS "
        sql += "SELECT nextval('"+self.default_schema+"."+self.sequence+"'::regclass) as ordered_id, ta.* "
        sql += "FROM ( "
        sql += "    SELECT tb1.ta_cell, tb1.observable_percent, tb2.last_visit_date, tb1.cell_kernel, tb1.geometries "
        sql += "    FROM "
        sql += "        ( "
        sql += "            SELECT tb2.ta_cell, tb2.geometries, tb2.observable_percent, tb1.cell_ibge1 as cell_kernel "
        sql += "            FROM "+self.default_schema+"."+self.identify_cells+" as tb1, "
        sql += "            ( "
        sql += "                SELECT tb1.ta_cell, tb1.geometries, "
        sql += "                ( (tb1.cell_area_km2 - ( COALESCE(tb2.cloud_area,0) ) ) * 100)/tb1.cell_area_km2 as observable_percent "
        sql += "                FROM ( "
        sql += "                    SELECT t1.ta_cell, t1.geometries, t1.cell_area_km2 "
        sql += "                    FROM "+table_name+"_"+self.jobber_sufix['tb2']+" as t1 "
        sql += "                ) as tb1 LEFT JOIN "
        sql += "                ( "
        sql += "                    SELECT tb.cell_oid, tb.scene_id, SUM( (ST_Area(geography(ST_Transform(tb.spatial_data,4326))) )/1000000 ) as cloud_area "
        sql += "                    FROM "+self.default_schema+"."+self.output_cloud_prefix+"_"+weekday_table+" as tb "
        sql += "                    GROUP BY tb.cell_oid, tb.scene_id "
        sql += "                ) as tb2 "
        sql += "                ON tb1.ta_cell=tb2.cell_oid "
        sql += "            ) as tb2 "
        sql += "            WHERE tb1.cell_ta=tb2.ta_cell "
        sql += "        ) as tb1 LEFT JOIN "
        sql += "        ( "
        sql += "            SELECT MAX(tb2.time) as last_visit_date, tb2.cell_id "
        sql += "            FROM "+self.ta_schema+"."+self.ta_tasklogs+" as tb2 "
        sql += "            WHERE tb2.finalized=true "
        sql += "            GROUP BY tb2.cell_id "
        sql += "        ) as tb2 "
        sql += "        ON tb1.ta_cell=tb2.cell_id "
        sql += "    ORDER BY tb1.observable_percent DESC, tb2.last_visit_date ASC, tb1.cell_kernel ASC "
        sql += ") as ta"

        self.__resetSequence()
        self.__basicExecute(sql)

    def __resetSequence(self):
        sql = "ALTER SEQUENCE "+self.default_schema+"."+self.sequence+" RESTART WITH 1"
        self.__basicExecute(sql)


    """
        Create one sequence used to intermediary tables
    """
    def __createSequence(self, path, row):
        self.sequence = '{0}_{1}_{2}'.format(self.sequence,path,row)
        sql = "CREATE SEQUENCE IF NOT EXISTS "+self.default_schema+"."+self.sequence+" INCREMENT 1 MINVALUE 1 "
        sql += "MAXVALUE 9223372036854775807 START 1 CACHE 1"
        self.__basicExecute(sql)


    def __dropSequence(self):
        sql = "DROP SEQUENCE "+self.default_schema+"."+self.sequence
        self.__basicExecute(sql)


    """
        Execute a basic SQL statement.
    """
    def __basicExecute(self, sql):
        try:
            self.db.execQuery(sql)
        except Exception as error:
            self.db.rollback()
            raise DatabaseError('Database error:', error)


    """
        Make input table name.

        The format to parameter table_identify is a dictionary like this: {'path':value,'row':value,'date':value}

        Return the table name otherwise raise an error MissingParameterError exception.
    """
    def getInputTable(self, table_identify):

        if table_identify is None:
            raise MissingParameterError('Get input tables','Parameter table_identify is missing')

        table_name = '{0}.{1}_{2}{3}_{4}_{5}'.format(self.default_schema, self.input_prefix, table_identify['path'],
                    table_identify['row'], table_identify['date'], self.input_sufix)

        return table_name
    
    """
        Make output table name.

        The format to parameter table_identify is a dictionary like this: {'path':value,'row':value,'date':value}
        The parameter output_weekday_table is the weekday name: segunda, terca, quarta, quinta, sexta, sabado, domingo

        The output table name should following the pattern: {path}{row}_{date}_{weekday}
        Return the table name otherwise raise an error MissingParameterError exception.
    """
    def getOutputTable(self, table_identify, output_weekday_table):
        
        if table_identify is None:
            raise MissingParameterError('Get output tables','Parameter table_identify is missing')
        if output_weekday_table is None:
            raise MissingParameterError('Get output tables','Parameter output_weekday_table is missing')
        
        weekday = '{0}{1}_{2}_{3}'.format(table_identify['path'],
                table_identify['row'], table_identify['date'], output_weekday_table)
        return weekday