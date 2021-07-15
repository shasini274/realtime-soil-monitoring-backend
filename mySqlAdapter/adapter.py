import hashlib
import json
import logging
import traceback
import copy

import pymysql.cursors
from .adapterError import InvalidDataAdapterError, DatabaseConstrainAdapterError, DatabaseAdapterError
COMMON_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

struct = {
            'id': '',
            'cluster': '',
            'stations': [{'id': '', 'measurements': [{'time': '', 'temperature': '', 'soilMoisture': ''}]}]

        }

pastdata_struct = {
            'stationId': '',
            'varId': '',
            'title': '',
            'data': []

}
timeseries_struct = {'time': '', 'value': ''}
class mySqlAdapter:

    def __init__(self, host="localhost", user="root", password="", db=""):
        """Initialize Database Connection"""

        self.connection = pymysql.connect(host=host,
                                          user=user,
                                          password=password,
                                          db=db)

        cursor = self.connection.cursor()

        # execute SQL query using execute() method.
        cursor.execute("SELECT VERSION()")

        # Fetch a single row using fetchone() method.
        data = cursor.fetchone()

    def get_cluster(self, serialNumber=None):

        if serialNumber is not None:
            try:
                with self.connection.cursor() as cursor:
                    sql = "SELECT clustername FROM soilmoistplatform.cluster WHERE serialnumber=%s"
                    logging.debug('sql (get_cluster):: %s', sql)
                    cursor.execute(sql, (serialNumber))

                    cluster = cursor.fetchone()
                    logging.debug('cluster:: %s', cluster)
                    return cluster[0]
            except Exception as error:
                traceback.print_exc(error)
        else:
            try:
                with self.connection.cursor() as cursor:
                    sql = "SELECT * FROM soilmoistplatform.cluster"
                    logging.debug('sql (get_cluster):: %s', sql)
                    cursor.execute(sql)

                    cluster = cursor.fetchall()
                    #logging.debug('cluster:: %s', cluster)
                    return cluster
            except Exception as error:
                traceback.print_exc(error)

    def get_cluster_station_id(self, clustername):

        try:
            with self.connection.cursor() as cursor:
                sql = [
                    "SELECT DISTINCT station FROM soilmoistplatform.summary WHERE cluster=%s",
                    "SELECT id, variable, enddatetime FROM soilmoistplatform.summary WHERE cluster=%s and station=%s order by variable",
                    "SELECT variabletype FROM soilmoistplatform.variables WHERE idvariable=%s",
                    "SELECT value FROM soilmoistplatform.data WHERE id=%s and datetime=%s"
                ]
                values = []
                cursor.execute(sql[0], (clustername[1]))
                stations = cursor.fetchall()


                if len(stations) == 0:
                    meta_struct = copy.deepcopy(struct)
                    meta_struct['id'] = clustername[0]
                    meta_struct['cluster'] = clustername[1]
                    values.append(meta_struct)

                else:
                    meta_struct = copy.deepcopy(struct)
                    meta_struct['id'] = clustername[0]
                    meta_struct['cluster'] = clustername[1]
                    for i, station in enumerate(stations):
                        print(i, station)
                        meta_struct['stations'][i]['id'] = station[0]
                        print(meta_struct)
                        print(meta_struct['stations'][i]['id'])
                        cursor.execute(sql[1], (clustername[1], station[0]))
                        results = cursor.fetchall()
                        print(results)
                        for result in results:
                            meta_struct['stations'][i]['measurements'][0]['time'] = result[2].strftime(COMMON_DATE_FORMAT)
                            print(meta_struct['stations'][i]['measurements'][0]['time'])
                            cursor.execute(sql[2], (result[1]))
                            variable = cursor.fetchone()
                            print(variable)
                            cursor.execute(sql[3], (result[0], result[2]))
                            var_val = cursor.fetchone()
                            print(var_val)
                            meta_struct['stations'][i]['measurements'][0][variable[0]] = var_val[0]
                        values.append(meta_struct)

                return values

        except Exception as error:
            traceback.print_exc(error)

    def get_timeseries(self, stationId, startDate, endDate):

        try:
            with self.connection.cursor() as cursor:
                sql = [
                    "SELECT id, variable FROM soilmoistplatform.summary WHERE station=%s",
                    "SELECT variabletype FROM soilmoistplatform.variables WHERE idvariable=%s",
                    "SELECT datetime, value FROM soilmoistplatform.data WHERE id=%s and datetime>=%s and datetime<=%s"
                ]

                timevalues = []
                cursor.execute(sql[0], stationId)
                utils = cursor.fetchall()

                for util in utils:
                    meta_struct = copy.deepcopy(pastdata_struct)
                    meta_struct['data'] = []
                    meta_struct['stationId'] = stationId
                    meta_struct['varId'] = util[1]

                    cursor.execute(sql[1], (util[1]))
                    variable = cursor.fetchone()

                    meta_struct['title'] = variable[0]

                    cursor.execute(sql[2], (util[0], startDate, endDate))
                    timeseries = cursor.fetchall()

                    for i, timeval in enumerate(timeseries):
                        timeseries_meta_struct = copy.deepcopy(timeseries_struct)
                        timeseries_meta_struct['time'] = timeval[0]
                        timeseries_meta_struct['value'] = timeval[1]
                        meta_struct['data'].append(timeseries_meta_struct)

                    timevalues.append(meta_struct)



                return timevalues

        except Exception as error:
            traceback.print_exc(error)

    def get_event_id(self, meta_data):

        event_id = None
        hashfactor = hashlib.sha256()

        hash_data = meta_data
        hashfactor.update(json.dumps(hash_data, sort_keys=True).encode("ascii"))
        possible_id = hashfactor.hexdigest()
        print(possible_id)
        try:
            with self.connection.cursor() as cursor:
                sql = "SELECT 1 FROM soilmoistplatform.summary WHERE id=%s"

                cursor.execute(sql, possible_id)
                is_exist = cursor.fetchone()
                if is_exist is not None:
                    event_id = possible_id
        except Exception as error:
            traceback.print_exc(error)
        finally:
            return event_id
    def get_create_event_id(self, meta_data, startdatetime):

        hash_data = meta_data

        hashfactor = hashlib.sha256()

        hashfactor.update(json.dumps(hash_data, sort_keys=True).encode("ascii"))
        event_id = hashfactor.hexdigest()
        try:
            with self.connection.cursor() as cursor:
                sql = [
                    "SELECT clustername FROM soilmoistplatform.cluster WHERE serialnumber=%s",
                    "SELECT idvariable FROM soilmoistplatform.variables WHERE variabletype=%s",
                    "SELECT id as `unit_id` FROM `unit` WHERE `unit`=%s",
                    "SELECT id as `type_id` FROM `type` WHERE `type`=%s",
                    "SELECT id as `source_id` FROM `source` WHERE `source`=%s"
                ]

                cursor.execute(sql[0], (meta_data['serialnumber']))
                cluster = cursor.fetchone()
                cursor.execute(sql[1], (meta_data['variable']))
                variable_id = cursor.fetchone()


                sql = "INSERT INTO soilmoistplatform.summary (id, cluster, station, variable, startdatetime) VALUES (%s, %s, %s, %s, %s)"

                sql_values = (
                    event_id,
                    cluster[0],
                    meta_data['station'],
                    variable_id,
                    startdatetime
                )
                cursor.execute(sql, sql_values)
                self.connection.commit()

        except DatabaseConstrainAdapterError as ae:
            logging.warning(ae.message)
            raise ae
        except Exception as error:
            traceback.print_exc()
            raise error

        return event_id


    def insert_timeseries(self, event_id, timeseries, upsert=False):

        row_count = 0
        try:
            with self.connection.cursor() as cursor:
                sql = "INSERT INTO soilmoistplatform.data (id, datetime, value) VALUES (%s, %s, %s)"


                if upsert:
                    sql = "INSERT INTO soilmoistplatform.data (id, datetime, value) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE value=VALUES(value)"

                sql_values = (
                    event_id,
                    timeseries[0],
                    timeseries[1]
                )

                row_count = cursor.execute(sql, sql_values)
                self.connection.commit()

                sql = "UPDATE soilmoistplatform.summary SET enddatetime=(SELECT MAX(datetime) from soilmoistplatform.data WHERE id=%s) " +\
                      "WHERE id=%s"
                cursor.execute(sql, (event_id, event_id))
                self.connection.commit()

        except Exception as error:
            traceback.print_exc(error)
        finally:
            return row_count