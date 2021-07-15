import eventlet
import json
from flask import Flask, render_template
from flask_mqtt import Mqtt
from flask_socketio import SocketIO
from flask_bootstrap import Bootstrap
from flask_cors import CORS, cross_origin
from datetime import datetime
import base64
import copy
import os
from os.path import join as pjoin
import logging
from mySqlAdapter import adapter
from route import api

eventlet.monkey_patch()

app = Flask(__name__)
app.config['SECRET'] = 'my secret key'
app.config['TEMPLATES_AUTO_RELOAD'] = True
app.config['CORS_HEADERS'] = 'Content-Type'
app.config['MQTT_BROKER_URL'] = 'eu.thethings.network'
app.config['MQTT_BROKER_PORT'] = 1883
app.config['MQTT_USERNAME'] = 'shasini_mkr1300_plat01'
app.config['MQTT_PASSWORD'] = 'ttn-account-v2.DoiOPDqARLsYcdCL94cAzrWqDTytAYaPgJ60uMdXdy0'
app.config['MQTT_KEEPALIVE'] = 5
app.config['MQTT_TLS_ENABLED'] = False
CORS(app, resources={r"/*": {"origins": "*"}})

mqtt = Mqtt(app)
socketio = SocketIO(app, cors_allowed_origins="*")
bootstrap = Bootstrap(app)

DATE_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
# data_struct = {
#     'time': '',
#     'serialnumber': '',
#     'station': '',
#     'data': {'soilMoisture': '', 'temperature': ''}
# }

data_struct = {
    'serialnumber': '',
    'station': '',
    'data': {'soilMoisture': '', 'temperature': '', 'time': ''}
}

@app.route('/')
def hello_world():
    return 'Hello, World!'

@mqtt.on_connect()
def handle_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    #client.subscribe("shasini_mkr1300_plat01/devices/shasini_mkrwan_1300/up")
    #mqtt.subscribe('home/mytopic')
    mqtt.subscribe("shasini_mkr1300_plat01/devices/shasini_mkrwan_1300/up")

@mqtt.on_message()
#@cross_origin(origin="*")
def handle_mqtt_message(client, userdata, msg):

    print(msg.topic + " " + str(msg.payload))
    payload = msg.payload
    json_dict = json.loads(payload)

    cluster = json_dict["hardware_serial"]
    print(cluster)
    payload_raw = json_dict["payload_raw"]
    print(payload_raw)
    time_raw = json_dict["metadata"]["time"]
    print(time_raw)
    time_raw = datetime.strptime(time_raw[:-4], '%Y-%m-%dT%H:%M:%S.%f')
    time = time_raw.strftime(DATE_TIME_FORMAT)


    #print("Payload is {}".format(payload_raw))

    payload_val = base64.b64decode(payload_raw)
    print(payload_val)
    print(payload_val[0])
    print(payload_val[1])

    soilMoistPerc = payload_val[0]
    temp = ((payload_val[2] << 8) | (payload_val[1])) / 100
    print(soilMoistPerc)
    print(temp)
    #payload = payload_val.decode('ascii')

    #print("Decoded Payload is {}".format(payload))
    #payload_str = str(payload)
    #payload_list = payload_str.split(",")


    data_struct_meta = copy.deepcopy(data_struct)
    # data_struct_meta['time'] = time
    data_struct_meta['data']['time'] = time
    data_struct_meta['serialnumber'] = cluster
    data_struct_meta['station'] = 'st01'
    data_struct_meta['data']['soilMoisture'] = soilMoistPerc
    data_struct_meta['data']['temperature'] = temp

    print(data_struct_meta)

    socketio.emit('cluster_data', data=data_struct_meta)
    #save_timeseries(data_struct_meta)


@mqtt.on_log()
def handle_logging(client, userdata, level, buf):
    print(level, buf)

def save_timeseries(data_struct_meta):
    root_dir = os.path.dirname(os.path.realpath(__file__))
    config = json.loads(open(pjoin(root_dir, 'config/config.json')).read())
    print(config)
    MYSQL_HOST = config['MYSQL_HOST']
    MYSQL_USER = config['MYSQL_USER']
    MYSQL_DB = config['MYSQL_DB']
    MYSQL_PASSWORD = config['MYSQL_PASSWORD']

    dbAdapter = adapter.mySqlAdapter(host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_PASSWORD, db=MYSQL_DB)
    print(data_struct_meta)
    meta_struct = {
        'serialnumber': '',
        'station': '',
        'variable': ''
    }
    clustername = dbAdapter.get_cluster(data_struct_meta['serialnumber'])

    eventid_meta_struct = copy.deepcopy(meta_struct)
    eventid_meta_struct['serialnumber'] = data_struct_meta['serialnumber']
    eventid_meta_struct['station'] = data_struct_meta['station']

    if clustername is not None:
        #timeseries = []
        for variable in data_struct_meta['data'][:2]:
            #timeseries = []
            eventid_meta_struct['variable'] = variable
            event_id_exist = dbAdapter.get_event_id(eventid_meta_struct)
            print(event_id_exist)

            if event_id_exist is None:
                event_id = dbAdapter.get_create_event_id(eventid_meta_struct, data_struct_meta['data']['time'])
                print(event_id)

            else:
                event_id =event_id_exist


            timeseries = [data_struct_meta['data']['time'], data_struct_meta['data'][variable]]
            row_count = dbAdapter.insert_timeseries(event_id, timeseries)

            print(row_count)

app.register_blueprint(api.output_api)

if __name__ == '__main__':
    socketio.run(app, host='0.0.0.0', port=5000, use_reloader=False, debug=True)