from flask import Blueprint, request, jsonify
import json
import os
from os.path import join as pjoin
import main
import copy
from datetime import datetime
from mySqlAdapter import adapter

DATE_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
root_dir = os.path.dirname(os.path.realpath(__file__))
config = json.loads(open(pjoin(root_dir, '../config/config.json')).read())
print(config)
MYSQL_HOST = config['MYSQL_HOST']
MYSQL_USER = config['MYSQL_USER']
MYSQL_DB = config['MYSQL_DB']
MYSQL_PASSWORD = config['MYSQL_PASSWORD']

dbAdapter = adapter.mySqlAdapter(host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_PASSWORD, db=MYSQL_DB)

output_api = Blueprint('output_api', __name__)


@output_api.route('/api', methods=['GET'])
def index():
    clusternames = dbAdapter.get_cluster()
    list = []
    for clustername in clusternames:
        cluster_summary = dbAdapter.get_cluster_station_id(clustername)
        print("CCCCCCCC")
        print(cluster_summary)
        list.extend(cluster_summary)

    print(list)
    return jsonify(list)
@output_api.route('/api/pastdata', methods=['GET'])
def get_station_data():


    stationId = request.args.get('staId', None)
    startDateObj = datetime.strptime(request.args.get('staD', None), "%Y-%m-%d")
    endDateObj = datetime.strptime(request.args.get('endD', None), "%Y-%m-%d")

    startDate = datetime.strftime(startDateObj, DATE_TIME_FORMAT)
    endDate = datetime.strftime(endDateObj, DATE_TIME_FORMAT)

    past_timeseries = dbAdapter.get_timeseries(stationId, startDate, endDate)

    return jsonify(past_timeseries)







