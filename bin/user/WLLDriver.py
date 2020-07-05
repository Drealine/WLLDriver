#!/usr/bin/python3

TEST_URL="http://192.168.1.18:80/v1/current_conditions"


# and now for the driver itself......

DRIVER_NAME = "WLLDriver"
DRIVER_VERSION = "0.1"

import json
import requests
import socket
import urllib.request

import sys
import time
import mysql.connector
import weewx.drivers
import weewx.engine
import weewx.units

#Libs for weatherlink.com request
import collections
import hashlib
import hmac
import time
import datetime
import math

from socket import *


# support both new and old formats for weewx logging
# ref: https://github.com/weewx/weewx/wiki/WeeWX-v4-and-logging
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
        syslog.syslog(level, 'WLLDriver: %s:' % msg)
    def logdbg(msg):
        logmsg(syslog.LOG_DEBUG, msg)
    def loginf(msg):
        logmsg(syslog.LOG_INFO, msg)
    def logerr(msg):
        logmsg(syslog.LOG_ERR, msg)

def loader(config_dict, engine):
    return WLLDriver(**config_dict[DRIVER_NAME], **config_dict)

class WLLDriver(weewx.drivers.AbstractDevice):

    # These settings contain default values you should set in weewx.conf
    # The quick poll_interval default here lets us run --test-driver and see
    # info quicker, but in general this shouldn't be faster than 60 secs
    # in your weewx.conf settings

    def __init__(self, **stn_dict):
        
        self.vendor = "Davis"
        self.product = "WeatherLinkLive"
        self.model = "WLLDriver"
        self.max_tries = int(stn_dict.get('max_tries', 5))
        self.time_out = int(stn_dict.get('time_out', 10))
        self.retry_wait = int(stn_dict.get('retry_wait', 10))
        self.poll_interval = float(stn_dict.get('poll_interval', 10))
        self.ntries = 0
        self.rain_previous_period = None
        self.udp_countdown = 0

        self.udp_enable = int(stn_dict.get('udp_enable',0))

        self.hostname = (stn_dict.get('hostname', "127.0.0.1"))

        self.url_current_conditions = "http://{}/v1/current_conditions".format(self.hostname)
        self.url_realtime_broadcast = "http://{}/v1/real_time?duration=36000".format(self.hostname)

        self.update_packet = None

        # Add for multiple sensor
        device_id = (stn_dict.get('device_id',str("1:iss")))
        self.length_dict_device_id = None

        self.dict_device_id = dict((int(k), v) for k, v in (e.split(':') for e in device_id.split('-')))

        self.unit_db_weewx = stn_dict['StdConvert']['target_unit']
        self.unit_db_input_data = None

        if self.unit_db_weewx == "METRIC":  self.unit_db_input_data = weewx.METRIC
        if self.unit_db_weewx == "METRICWX":  self.unit_db_input_data = weewx.METRICWX
        if self.unit_db_weewx == "US":  self.unit_db_input_data = weewx.US

        self.comsocket = socket(AF_INET, SOCK_DGRAM)
        self.comsocket.bind(('0.0.0.0', 22222))
        self.comsocket.setsockopt(SOL_SOCKET, SO_BROADCAST, 1)

        loginf("driver is %s" % DRIVER_NAME)
        loginf("driver version is %s" % DRIVER_VERSION)
        loginf("polling interval is %s" % self.poll_interval)


    def data_decode_wll(self, data, type_of_packet):

        try:

            if type_of_packet == 'current_conditions' and data['data'] == None:

                raise Exception('No data in WLL packet')

            else:

                rain_this_period = 0

                self.length_dict_device_id = len(self.dict_device_id)

                for device_id, device in self.dict_device_id.items():

                    length_dict_device_id_count = 1
                    length_dict_device_id = self.length_dict_device_id

                    while length_dict_device_id_count <= length_dict_device_id:

                        if type_of_packet == 'current_conditions':

                            datetime = data['data']['ts']

                            for s in data['data']['conditions']:

                                if s['data_structure_type'] == 1 :

                                    if s['txid'] == device_id:

                                        if self.dict_device_id[device_id] == 'iss' or 'iss+' or 'extraTemp{}'.format(length_dict_device_id_count):

                                            if 'temp' in s and s['temp'] is not None:

                                                if self.dict_device_id[device_id] == 'iss' or 'iss+':
                                    
                                                    outTemp = s['temp']

                                                elif self.dict_device_id[device_id] == 'extraTemp{}'.format(length_dict_device_id_count):

                                                    extraTemp['extraTemp{}'.format(length_dict_device_id_count)] = s['temp']

                                        if self.dict_device_id[device_id] == 'iss' or 'iss+' or 'extraHumid{}'.format(length_dict_device_id_count):

                                            if 'hum' in s and s['hum'] is not None:

                                                if self.dict_device_id[device_id] == 'iss' or 'iss+':
                                    
                                                    outHumidity = s['hum']

                                                elif self.dict_device_id[device_id] == 'extraHum{}'.format(length_dict_device_id_count):

                                                    extraHum['extraTemp{}'.format(length_dict_device_id_count)] = s['hum']


                                        if self.dict_device_id[device_id] == 'iss' or 'iss+':

                                            if 'dew_point' in s and s['dew_point'] is not None:

                                                dewpoint = s['dew_point']

                                            if 'heat_index' in s and s['heat_index'] is not None:
                                            
                                                heatindex = s['heat_index']
                                            
                                            if  'wind_chill' in s and s['wind_chill'] is not None:    

                                                windchill = s['wind_chill']

                                        if self.dict_device_id[device_id] == 'iss' or 'iss+' or 'extra_Anenometer':

                                            if 'wind_speed_last' in s:

                                                windSpeed = s['wind_speed_last']

                                            if 'wind_dir_last' in s:

                                                windDir = s['wind_dir_last']
                                            
                                            if 'wind_speed_hi_last_10_min' in s:

                                                windGust = s['wind_speed_hi_last_10_min']

                                            if 'wind_dir_scalar_avg_last_10_min' in s:
                                            
                                                windGustDir = s['wind_dir_scalar_avg_last_10_min']

                                        if self.dict_device_id[device_id] == 'iss' or 'iss+' or 'extra_RainGauge':

                                            if 'rain_rate_last' in s and s['rain_rate_last'] is not None:

                                                rainRate = s['rain_rate_last']

                                            if 'rainfall_daily' in s and s['rainfall_daily'] is not None:

                                                rainFall_Daily = s['rainfall_daily']

                                            if 'rain_size' in s and s['rain_size'] is not None:

                                                rainSize = s['rain_size']

                                # Next lines are not extra, so no need ID

                                elif s['data_structure_type'] == 2 :
                                    # temp_1 to 4
                                    # moist_soil_1 to 4
                                    # wet_leaf_1 to 2
                                    # rx_state
                                    # trans_battery_flag
                                    pass
                                elif s['data_structure_type'] == 3 :
                                    barometer = s['bar_sea_level']
                                    pressure = s['bar_absolute']
                                elif s['data_structure_type'] == 4 :
                                    inTemp = s['temp_in']
                                    inHumidity = s['hum_in']
                                    inDewpoint = s['dew_point_in']

                        elif type_of_packet == 'realtime_broadcast':


                            datetime = data['ts']

                            for s in data['conditions']:

                                if s['data_structure_type'] == 1 :

                                    if s['txid'] == device_id:


                                        if self.dict_device_id[device_id] == 'iss' or 'iss+' or 'extra_Anenometer':

                                            if 'wind_speed_last' in s:

                                                windSpeed = s['wind_speed_last']

                                            if 'wind_dir_last' in s:

                                                windDir = s['wind_dir_last']
                                            
                                            if 'wind_speed_hi_last_10_min' in s:

                                                windGust = s['wind_speed_hi_last_10_min']

                                            if 'wind_dir_at_hi_speed_last_10_min' in s:
                                            
                                                windGustDir = s['wind_dir_at_hi_speed_last_10_min']

                                        if self.dict_device_id[device_id] == 'iss' or 'iss+' or 'extra_RainGauge':

                                            if 'rain_rate_last' in s and s['rain_rate_last'] is not None:

                                                rainRate = s['rain_rate_last']

                                            if 'rainfall_daily' in s and s['rainfall_daily'] is not None:

                                                rainFall_Daily = s['rainfall_daily']

                                            if 'rain_size' in s and s['rain_size'] is not None:

                                                rainSize = s['rain_size']


                        length_dict_device_id_count += 1


            if rainSize is not None:

                if rainSize == 1:

                    rainmultiplier = 0.01

                elif rainSize == 2:

                    rainmultiplier = 0.2

                elif rainSize == 3:

                    rainmultiplier = 0.1


            if rainFall_Daily is not None: 

                if self.rain_previous_period is not None:
                    rain_this_period = (rainFall_Daily - self.rain_previous_period)*rainmultiplier
                    self.rain_previous_period = rainFall_Daily
                    logdbg("Rain this period: " + str(rain_this_period))

                else:

                    self.rain_previous_period = rainFall_Daily
                    logdbg("Rainfall set OK")

            if rainRate is not None:

                rainRate = rainRate * rainmultiplier
                logdbg("Set Previous period rain to: " + str(self.rain_previous_period))


            if self.unit_db_input_data == weewx.METRIC or self.unit_db_input_data == weewx.METRICWX:

                if type_of_packet == 'current_conditions':

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

                if type_of_packet == 'current_conditions' or 'realtime_broadcast':

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

                    if rain_this_period is not None:

                        if rainSize == 2 and self.unit_db_input_data == weewx.METRIC:

                            rain_this_period = float(rain_this_period) / 10

                    if rainRate is not None:

                        if rainSize == 2 and self.unit_db_input_data == weewx.METRIC:

                            rainRate = float(rainRate) / 10

            if type_of_packet == 'current_conditions':

                self.update_packet = {'dateTime': datetime,
                       'usUnits': self.unit_db_input_data,
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
                       'rain' : rain_this_period,
                       'rainRate' : rainRate,
                       'inTemp':  inTemp,
                       'inHumidity':  inHumidity,
                       'inDewpoint' : inDewpoint,
                       }

            elif type_of_packet == 'realtime_broadcast':

                self.update_packet = {'dateTime': datetime,
                       'usUnits': self.unit_db_input_data,
                       'windSpeed' : windSpeed,
                       'windDir' : windDir,
                       'windGust' : windGust,
                       'windGustDir' : windGustDir,
                       'rain' : rain_this_period,
                       'rainRate' : rainRate,
                       }

            if self.update_packet is not None:

                # got the data ok so reset the flag counter
                logdbg("Packet received from WLL module {}:".format(self.update_packet))
                yield self.update_packet
                self.ntries = 0

            else:
                
                raise Exception('No data in WLL packet')

        except Exception as e:
            loginf("Failure to get data %s - try %s - (%s)" % (self.url_current_conditions,self.ntries, e))
            self.ntries += 1
            if self.poll_interval:
                time.sleep(self.retry_wait)
        

    def get_current_conditions_wll(self):

         while self.ntries < self.max_tries:

            _packet = None

            try:

                data = requests.get(url=self.url_current_conditions, timeout=self.time_out)

                if data is not None:

                    data = data.json()
                    self.ntries = 0
                    return data

            except Exception as e:
                loginf("Failure to get data %s - try %s - (%s)" % (self.url_current_conditions,self.ntries, e))
                self.ntries += 1
                if self.retry_wait:
                    time.sleep(self.retry_wait)


    def request_realtime_broadcast(self):

        while self.ntries < self.max_tries:

            if self.udp_countdown - 10 < time.time():

                try:

                    r = requests.get(url=self.url_realtime_broadcast, timeout=self.time_out)

                    if r is not None:

                        data = r.json()

                        if data['data'] is not None:

                            self.udp_countdown = time.time() + data['data']['duration']

                            return


                except Exception as e:
                    loginf("Failure to get realtime data %s - try %s - (%s)" % (self.url_realtime_broadcast,self.ntries, e))
                    self.ntries += 1
                    if self.retry_wait:
                        time.sleep(self.retry_wait)
            else:

                return


    def get_realtime_broadcast(self):

        if self.udp_countdown - self.poll_interval > time.time():

            try:

                data, wherefrom = self.comsocket.recvfrom(2048)
                realtime_data = json.loads(data.decode("utf-8"))

                if realtime_data is not None:


                    return realtime_data

            except Exception as e:
                    loginf("Failure to get realtime data %s" % (e))
                    self.request_realtime_broadcast()

                    

    # the hardware does not define a model so use what is in the __init__ settings
    @property
    def hardware_name(self):
        return self.model


    def genLoopPackets(self):

        while self.ntries < self.max_tries:


            try:

                conditions_data = self.get_current_conditions_wll()
                

                if conditions_data is not None:

                    for _packet_wll in self.data_decode_wll(conditions_data, 'current_conditions'):

                            yield _packet_wll

                if self.udp_enable == 0:

                    if self.poll_interval:

                        time.sleep(self.poll_interval)


                if self.udp_enable == 1:

                    timeout_udp_broadcast = time.time() + self.poll_interval

                    self.request_realtime_broadcast()

                    while time.time() < timeout_udp_broadcast:

                        realtime_data = self.get_realtime_broadcast()

                        if realtime_data is not None:

                            for _realtime_packet in self.data_decode_wll(realtime_data, 'realtime_broadcast'):

                                yield _realtime_packet

                self.ntries = 0

            except (weewx.WeeWxIOError) as e:
                logerr("Failed attempt %d of %d to get LOOP data: %s" %
                       (self.ntries, self.max_tries, e))

                self.ntries += 1
                time.sleep(self.retry_wait)
        else:
            msg = "Max retries (%d) exceeded for LOOP data" % self.max_tries
            logerr(msg)
            raise weewx.RetriesExceeded(msg)


#==============================================================================
# Main program
#
# To test this driver, do the following:
#   PYTHONPATH=/home/weewx/bin python3 /home/weewx/bin/user/WLLDriver.py
#
#==============================================================================

if __name__ == "__main__":
    usage = """%prog [options] [--help]"""

    def main():
        try:
            import logging
            import weeutil.logger
            log = logging.getLogger(__name__)
            weeutil.logger.setup('WLLDriver', {} )
        except ImportError:
            import syslog
            syslog.openlog('WLLDriver', syslog.LOG_PID | syslog.LOG_CONS)

        import optparse
        parser = optparse.OptionParser(usage=usage)
        parser.add_option('--test-driver', dest='td', action='store_true',
                          help='test the driver')
        (options, args) = parser.parse_args()

        if  options.td:
            test_driver()

    def test_driver():
        import weeutil.weeutil
        driver = WLLDriver()
        print("testing driver")
        for pkt in driver.genLoopPackets():
            print((weeutil.weeutil.timestamp_to_string(pkt['dateTime']), pkt))

    main()

#---- that's all folks ----
