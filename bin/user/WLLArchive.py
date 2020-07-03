import json
import requests

import urllib.request

import sys
import time
import mysql.connector
import weewx.drivers
import weewx.engine
import weewx.units
import weewx

#Libs for weatherlink.com request
import collections
import hashlib
import hmac
import time
import datetime
import math

from weeutil.weeutil import timestamp_to_string, option_as_list
from weewx.engine import StdService

try:
    import weeutil.logger
    import logging
    log = logging.getLogger(__name__)
    def logdbg(msg):
        log.debug(msg)
    def loginf(msg):
        log.info(msg)
    def logerr(msg):
        log.error(msg)
except ImportError:
    import syslog
    def logmsg(level, msg):
        syslog.syslog(level, 'WLLArchive: %s:' % msg)
    def logdbg(msg):
        logmsg(syslog.LOG_DEBUG, msg)
    def loginf(msg):
        logmsg(syslog.LOG_INFO, msg)
    def logerr(msg):
        logmsg(syslog.LOG_ERR, msg)


class WLLArchive(StdService):

    def __init__(self, engine, config_dict):

        super(WLLArchive, self).__init__(engine, config_dict)

        try:

            logdbg("WLLArchive is loaded")

            self.max_tries = int(config_dict['WLLArchive'].get('max_tries', 5))
            self.time_out = int(config_dict['WLLArchive'].get('time_out', 10))
            self.retry_wait = int(config_dict['WLLArchive'].get('retry_wait', 10))
            self.poll_interval = float(config_dict['WLLArchive'].get('poll_interval', 2))
            self.ntries = 0

            self.dict_sensor_type = {'iss':{46,48},
                                    'extraTemp1':{55},
            }


            self.length_json = None

            # Add for multiple sensor
            device_id = (config_dict['WLLArchive'].get('device_id',str("1-iss")))
            self.dict_device_id = dict((int(k), v) for k, v in (e.split(':') for e in device_id.split('-')))


            # Add for weatherlink request
            self.wl_apikey = (config_dict['WLLArchive'].get('wl_apikey', "ABC123"))
            self.wl_apisecret = (config_dict['WLLArchive'].get('wl_apisecret', "ABC123"))
            self.wl_stationid = (config_dict['WLLArchive'].get('wl_stationid', "ABC123"))
            self.wl_archive_interval = int(config_dict['WLLArchive'].get('wl_archive_interval', 15))


            #self.db_manager = weewx.manager.open_manager_with_config(config_dict, 'wx_binding')
            self.db_manager = self.engine.db_binder.get_manager('wx_binding')
            self.unit_db_weewx = config_dict['StdConvert']['target_unit']
            self.unit_db_input_data = None

            if self.unit_db_weewx == "METRIC":  self.unit_db_input_data = weewx.METRIC
            if self.unit_db_weewx == "METRICWX":  self.unit_db_input_data = weewx.METRICWX
            if self.unit_db_weewx == "US":  self.unit_db_input_data = weewx.US

            # If we got this far, it's ok to start intercepting events:
            self.bind(weewx.PRE_LOOP, self.new_archive_record)

        except KeyError as e:
            syslog.syslog(syslog.LOG_INFO, "Unable to implement WLLArchive %s" % e)


    def get_timestamp_wl_archive(self, wl_archive_interval):

        timestamp_wl_archive = int(math.floor((time.time() - 60) / (wl_archive_interval * 60)) * (wl_archive_interval * 60))

        return timestamp_wl_archive

    def get_timestamp_by_time(self, timestamp, wl_archive_interval):

        timestamp_wl_archive = int(math.floor((timestamp - 60) / (wl_archive_interval * 60)) * (wl_archive_interval * 60))

        return timestamp_wl_archive


    def request_wl(self,start_timestamp, end_timestamp):

        # Due to limit size on Weatherlink, if result timestamp is more thant 24h, split the request

        try:

            start_timestamp = self.get_timestamp_by_time(start_timestamp, self.wl_archive_interval)

            result_timestamp = end_timestamp - start_timestamp

            if result_timestamp >= 86400:

                new_start_timestamp = result_timestamp - 86400

                start_timestamp = start_timestamp + new_start_timestamp

                logdbg("Impossible to request data > 24H. Request new data to Weatherlink from {} to {} ...".format(start_timestamp, end_timestamp))


            wl_packet = None

            parameters = {
              "api-key": str(self.wl_apikey),
              "api-secret": str(self.wl_apisecret),
              "end-timestamp": str(end_timestamp),
              "start-timestamp": str(start_timestamp),
              "station-id": str(self.wl_stationid),
              "t": int(time.time())
            }

            parameters = collections.OrderedDict(sorted(parameters.items()))

            for key in parameters:
              print("Parameter name: \"{}\" has value \"{}\"".format(key, parameters[key]))

            apiSecret = parameters["api-secret"];
            parameters.pop("api-secret", None);

            data = ""
            for key in parameters:
              data = data + key + str(parameters[key])

            apiSignature = hmac.new(
              apiSecret.encode('utf-8'),
              data.encode('utf-8'),
              hashlib.sha256
            ).hexdigest()

            url_apiv2_wl = "https://api.weatherlink.com/v2/historic/{}?api-key={}&t={}&start-timestamp={}&end-timestamp={}&api-signature={}".format(parameters["station-id"], parameters["api-key"], parameters["t"], parameters["start-timestamp"], parameters["end-timestamp"], apiSignature)
            logdbg("URL API Weatherlink is {} ".format(url_apiv2_wl))

            wl_session = requests.session()
            data_request_url = wl_session.get(url_apiv2_wl, timeout=self.time_out)
            logdbg("OK Wl 1")
            data_wl = data_request_url.json()
            logdbg("OK Wl 2")

            start_timestamp = int(start_timestamp + (60 * self.wl_archive_interval))
            logdbg("StartTimeStamp is : %d" %start_timestamp)


            self.length_json = len(data_wl['sensors'])
            logdbg(self.length_json)

            while start_timestamp <= end_timestamp:

                outTemp = None
                outHumidity = None
                dewpoint = None
                heatindex = None
                windchill = None
                windSpeed = None
                windDir = None
                windGust = None
                windGustDir = None
                barometer = None
                pressure = None
                rain = None
                rainRate = None
                inTemp = None
                inHumidity = None
                inDewpoint = None

                length_json_count = 0
                length_json = self.length_json - 1

                while length_json_count <= length_json:

                    for device_id, device in self.dict_device_id.items():

                        for sensor_type_id in self.dict_sensor_type[self.dict_device_id[device_id]]:

                            for s in data_wl['sensors']:

                                if s['sensor_type'] == sensor_type_id:

                                    for s in data_wl['sensors'][length_json_count]['data']:

                                        if 'tx_id' in s and s['tx_id'] == device_id:

                                            if s['ts'] == start_timestamp:

                                                if 'temp_last' in s:

                                                    logdbg(length_json_count)
                                                    logdbg("test1")

                                                    outTemp = s['temp_last']

                                                if 'hum_last' in s:

                                                    outHumidity = s['hum_last']

                                                if 'dew_point_last' in s:

                                                    dewpoint = s['dew_point_last']

                                                if 'rain_size' in s:

                                                    rainSize = s['rain_size']
                                                    logdbg(rainSize)
                                                
                                                if 'heat_index_last' in s:
                                                    heatindex = s['heat_index_last']

                                                if 'wind_chill_last' in s:

                                                    windchill = s['wind_chill_last']

                                                if 'wind_speed_avg' in s:

                                                    windSpeed = s['wind_speed_avg']

                                                if 'wind_dir_of_prevail' in s:

                                                    windDir = s['wind_dir_of_prevail']

                                                if 'wind_speed_hi' in s:

                                                    windGust = s['wind_speed_hi']

                                                if 'wind_speed_hi_dir' in s:

                                                    windGustDir = s['wind_speed_hi_dir']

                                                if rainSize is not None:

                                                    if rainSize == 1:

                                                        if 'rain_rate_hi_in' in s:

                                                            rainRate = s['rain_rate_hi_in']

                                                        if 'rainfall_in' in s:

                                                            rain = s['rainfall_in']

                                                    elif rainSize == 2:

                                                        if 'rain_rate_hi_mm' in s:
               
                                                            rainRate = s['rain_rate_hi_mm']

                                                        if 'rainfall_mm' in s:

                                                            rain = s['rainfall_mm']

                                                    #elif rainSize == 3:

                                                        # What about this value ? Is not implement on weatherlink.com ?

                            for s in data_wl['sensors']:

                                if s['sensor_type'] == 242:

                                    for s in data_wl['sensors'][length_json_count]['data']:

                                        if s['ts'] == start_timestamp:

                                            if 'bar_sea_level' in s:

                                                barometer = s['bar_sea_level']

                                            if 'bar_absolute' in s:

                                                pressure = s['bar_absolute']

                            for s in data_wl['sensors']:

                                if s['sensor_type'] == 243:

                                    for s in data_wl['sensors'][length_json_count]['data']:

                                        if s['ts'] == start_timestamp:

                                            if 'temp_in_last' in s:

                                                inTemp = s['temp_in_last']
                                                logdbg(inTemp)

                                            if 'hum_in_last' in s:

                                                inHumidity = s['hum_in_last']
                                                logdbg(inHumidity)

                                            if 'dew_point_in' in s:

                                                inDewpoint = s['dew_point_in']
                                                logdbg(inDewpoint)

                    length_json_count += 1

                #logdbg("Values received in JSON from Weatherlink : %s" % log_packet_before_transformed)

                if self.unit_db_input_data == weewx.METRIC or self.unit_db_input_data == weewx.METRICWX:

                    if outTemp is not None:

                        outTemp = (float(outTemp) - 32) * 5/9
                        outTemp = round(outTemp,2)

                    if dewpoint is not None:

                        dewpoint = (float(dewpoint) - 32) * 5/9
                        dewpoint = round(dewpoint,2)

                    if heatindex is not None:

                        heatindex = (float(heatindex) - 32) * 5/9
                        heatindex = round(heatindex,2)

                    if windchill is not None:

                        windchill = (float(windchill) - 32) * 5/9
                        windchill = round(windchill,2)

                    if inTemp is not None:

                        inTemp = (float(inTemp) - 32) * 5/9
                        inTemp = round(inTemp,2)

                    if inDewpoint is not None:

                        inDewpoint = (float(inDewpoint) - 32) * 5/9
                        inDewpoint = round(inDewpoint,2)

                    if barometer is not None:
                    
                        barometer = float(barometer) * 33.864
                        barometer = round(barometer,2)

                    if pressure is not None:

                        pressure = float(pressure) * 33.864
                        pressure = round(pressure,2)


                if windSpeed is not None:

                    if self.unit_db_input_data == weewx.METRIC:

                        windSpeed = float(windSpeed) * 1.609344
                        windSpeed = round(windSpeed,2)

                    if self.unit_db_input_data == weewx.METRICWX:

                        windSpeed = float(windSpeed) / 2.237
                        windSpeed = round(windSpeed,2)

                if windGust is not None:

                    if self.unit_db_input_data == weewx.METRIC:

                        windGust = float(windGust) * 1.609344
                        windGust = round(windGust,2)

                    if self.unit_db_input_data == weewx.METRICWX:

                        windGust = float(windGust) / 2.237
                        windGust = round(windGust,2)

                if windDir is not None:

                    windDir = round(windDir,0)

                if windGustDir is not None:

                    windGustDir = round(windGustDir,0)

                if rain is not None:

                    if rainSize == 2 and self.unit_db_input_data == weewx.METRIC:

                        rain = float(rain) / 10

                if rainRate is not None:

                    if rainSize == 2 and self.unit_db_input_data == weewx.METRIC:

                        rainRate = float(rainRate) / 10

              
                wl_packet = {'dateTime': int(start_timestamp),
                                   'usUnits': self.unit_db_input_data,
                                   'interval': self.wl_archive_interval,
                                   'outTemp': outTemp,
                                   'outHumidity': outHumidity,
                                   'dewpoint': dewpoint,
                                   'heatindex': heatindex,
                                   'windchill': windchill,
                                   'windSpeed' : windSpeed,
                                   'windDir' : windDir,
                                   'windGust' : windGust,
                                   'windGustDir' : windGustDir,
                                   'barometer' : barometer,
                                   'pressure' : pressure,
                                   'rain' : rain,
                                   'rainRate' : rainRate,
                                   'inTemp':  inTemp,
                                   'inHumidity':  inHumidity,
                                   'inDewpoint' : inDewpoint,
                                   }

                if wl_packet is not None:

                    start_timestamp = int(start_timestamp + (60 * self.wl_archive_interval))
                    yield wl_packet

                else:

                    raise Exception('No data in Weatherlink packet')

            # Keep this line for futur use
            '''if self.poll_interval: 
                time.sleep(self.poll_interval)'''

        except Exception as e:
            loginf("Failure to get data %s - try %s - (%s)" % (url_apiv2_wl,self.ntries, e))
            self.ntries += 1
            if self.poll_interval:
                time.sleep(self.poll_interval)

    def new_archive_record(self, event):

        while self.ntries < self.max_tries:

            try:

                last_good_stamp = self.db_manager.lastGoodStamp()
                now_timestamp_wl = self.get_timestamp_wl_archive(self.wl_archive_interval)

                # Add 60 secondes timestamp for the WLL archive new data

                if last_good_stamp is not None and (last_good_stamp + 60 < now_timestamp_wl):

                    logdbg("Request archive from {} to {}".format(last_good_stamp, now_timestamp_wl))

                    for _packet_wl in self.request_wl(last_good_stamp, now_timestamp_wl):

                        self.db_manager.addRecord(_packet_wl)

                else:

                    return

                
                if self.poll_interval:
                    time.sleep(self.poll_interval)

            except (weewx.WeeWxIOError) as e:
                logerr("Failed attempt %d of %d to get LOOP data: %s" %
                       (self.ntries, self.max_tries, e))
                self.ntries += 1
                time.sleep(self.retry_wait)
        else:
            msg = "Max retries (%d) exceeded for LOOP data" % self.max_tries
            logerr(msg)
            raise weewx.RetriesExceeded(msg)

