#!/usr/bin/python3

TEST_URL="http://192.168.1.18:80/v1/current_conditions"


# and now for the driver itself......

DRIVER_NAME = "WLLDriver"
DRIVER_VERSION = "1"

import json
import requests

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

from mysql.connector import Error

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
        self.retry_wait = int(stn_dict.get('retry_wait', 10))
        self.poll_interval = float(stn_dict.get('poll_interval', 2))
        self.url = stn_dict.get('url', TEST_URL)
        self.rain_previous_period = 0

        # Add for weatherlink request
        self.wl_apikey = (stn_dict.get('wl_apikey', "ABC123"))
        self.wl_apisecret = (stn_dict.get('wl_apisecret', "ABC123"))
        self.wl_stationid = (stn_dict.get('wl_stationid', "ABC123"))

        self.wl_archive_interval = int(stn_dict.get('wl_archive_interval', 15))
        self.db_manager = weewx.manager.open_manager_with_config(stn_dict, 'wx_binding')

        self.ntries = 0

        self.metric_db_weewx = stn_dict['StdConvert']['target_unit']

        loginf("driver is %s" % DRIVER_NAME)
        loginf("driver version is %s" % DRIVER_VERSION)
        loginf("polling interval is %s" % self.poll_interval)


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
            data_request_url = wl_session.get(url_apiv2_wl)
            data_wl = data_request_url.json()

            start_timestamp = int(start_timestamp + (60 * self.wl_archive_interval))
            logdbg("StartTimeStamp is : %d" %start_timestamp)

            while start_timestamp <= end_timestamp:

                for s in data_wl['sensors']:

                    if s['sensor_type'] == 48:

                        for s in data_wl['sensors'][0]['data']:

                            if s['ts'] == start_timestamp:

                                outTemp = s['temp_last']
                                outHumidity = s['hum_last']
                                dewpoint = s['dew_point_last']
                                heatindex = s['dew_point_last']
                                windchill = s['wind_chill_last']
                                windSpeed = s['wind_speed_avg']
                                windDir = s['wind_dir_of_prevail']
                                windGust = s['wind_speed_hi']
                                windGustDir = s['wind_speed_hi_dir']
                                rainRate = s['rain_rate_hi_mm']
                                rain = s['rainfall_mm']

                for s in data_wl['sensors']:

                    if s['sensor_type'] == 242:

                        for s in data_wl['sensors'][2]['data']:

                            if s['ts'] == start_timestamp:

                                barometer = s['bar_sea_level']
                                pressure = s['bar_absolute']

                for s in data_wl['sensors']:

                    if s['sensor_type'] == 243:

                        for s in data_wl['sensors'][3]['data']:

                            if s['ts'] == start_timestamp:

                                inTemp = s['temp_in_last']
                                inHumidity = s['hum_in_last']
                                inDewpoint = s['dew_point_in']

                _packet_before = {'outTemp': outTemp,
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

                #logdbg("Values received in JSON from Weatherlink : %s" % log_packet_before_transformed)

                if _packet_before['outTemp'] is not None:

                        outTemp = (float(outTemp) - 32) * 5/9
                        outTemp = round(outTemp,2)

                if _packet_before['dewpoint'] is not None:

                    dewpoint = (float(dewpoint) - 32) * 5/9
                    dewpoint = round(dewpoint,2)

                if _packet_before['heatindex'] is not None:

                    heatindex = (float(heatindex) - 32) * 5/9
                    heatindex = round(heatindex,2)

                if _packet_before['windchill'] is not None:

                    windchill = (float(windchill) - 32) * 5/9
                    windchill = round(windchill,2)

                if _packet_before['inTemp'] is not None:

                    inTemp = (float(inTemp) - 32) * 5/9
                    inTemp = round(inTemp,2)

                if _packet_before['inDewpoint'] is not None:

                    inDewpoint = (float(inDewpoint) - 32) * 5/9
                    inDewpoint = round(inDewpoint,2)

                if _packet_before['inDewpoint'] is not None:
                
                    barometer = float(barometer) * 33.864
                    barometer = round(barometer,2)

                if _packet_before['pressure'] is not None:

                    pressure = float(pressure) * 33.864
                    pressure = round(pressure,2)

                if _packet_before['windSpeed'] is not None:

                    windSpeed = float(windSpeed) * 1.609344
                    windSpeed = round(windSpeed,2)

                if _packet_before['windGust'] is not None:

                    windGust = float(windGust) * 1.609344
                    windGust = round(windGust,2)

                if _packet_before['windDir'] is not None:

                    windDir = round(windDir,0)

                if _packet_before['windGustDir'] is not None:

                    windGustDir = round(windGustDir,0)

                # Do this for weewx table in METRIC

                if _packet_before['rain'] is not None:

                    rain = float(rain) / 10

                if _packet_before['rain'] is not None:

                    rainRate = float(rainRate) / 10

              
                wl_packet = {'dateTime': int(start_timestamp),
                                   'usUnits': weewx.METRIC,
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


    def request_wll(self):

        _packet = None

        try:

            r = requests.get(url=self.url)
            data = r.json()

            if data['data'] == None:

                raise Exception('No data in WLL packet')

            else:

                for s in data['data']['conditions']:

                    rainmultiplier = 0.2 # Set this value for European Rain Collector
                    rain_this_period = 0

                    # keep these in the order defined in the Davis doc for readability
                    if s['data_structure_type'] == 1 :
                        outTemp = s['temp']
                        outHumidity = s['hum']
                        dewpoint = s['dew_point']
                        heatindex = s['heat_index']
                        windchill = s['wind_chill']
                        windSpeed = s['wind_speed_last']
                        windDir = s['wind_dir_last']
                        windGust = s['wind_speed_hi_last_2_min']
                        windGustDir = s['wind_dir_scalar_avg_last_2_min']
                        rainRate = s['rain_rate_last']
                        rainFall_Daily = s['rainfall_daily']

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


                if rainFall_Daily is not None: 

                    if self.rain_previous_period is not None:
                        rain_this_period = (rainFall_Daily - self.rain_previous_period)*rainmultiplier
                        self.rain_previous_period = rainFall_Daily
                        logdbg("Rain this period: " + str(rain_this_period))

                if rainRate is not None:

                    rainRate = rainRate * rainmultiplier
                    logdbg("Set Previous period rain to: " + str(self.rain_previous_period))

                _packet_before = {'outTemp': outTemp,
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


                #logdbg("Values received in JSON from WLL : %s" % log_packet_before_transformed)

                if _packet_before['outTemp'] is not None:

                    outTemp = (float(outTemp) - 32) * 5/9
                    outTemp = round(outTemp,2)

                if _packet_before['dewpoint'] is not None:

                    dewpoint = (float(dewpoint) - 32) * 5/9
                    dewpoint = round(dewpoint,2)

                if _packet_before['heatindex'] is not None:

                    heatindex = (float(heatindex) - 32) * 5/9
                    heatindex = round(heatindex,2)

                if _packet_before['windchill'] is not None:

                    windchill = (float(windchill) - 32) * 5/9
                    windchill = round(windchill,2)

                if _packet_before['inTemp'] is not None:

                    inTemp = (float(inTemp) - 32) * 5/9
                    inTemp = round(inTemp,2)

                if _packet_before['inDewpoint'] is not None:

                    inDewpoint = (float(inDewpoint) - 32) * 5/9
                    inDewpoint = round(inDewpoint,2)

                if _packet_before['inDewpoint'] is not None:
                
                    barometer = float(barometer) * 33.864
                    barometer = round(barometer,2)

                if _packet_before['pressure'] is not None:

                    pressure = float(pressure) * 33.864
                    pressure = round(pressure,2)

                if _packet_before['windSpeed'] is not None:

                    windSpeed = float(windSpeed) / 2.237
                    windSpeed = round(windSpeed,2)

                if _packet_before['windGust'] is not None:

                    windGust = float(windGust) / 2.237
                    windGust = round(windGust,2)

                if _packet_before['windDir'] is not None:

                    windDir = round(windDir,0)

                if _packet_before['windGustDir'] is not None:

                    windGustDir = round(windGustDir,0)

                _packet = {'dateTime': int(time.time() + 0.5),
                           'usUnits': weewx.METRICWX,
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

                if _packet is not None:

                    # got the data ok so reset the flag counter
                    logdbg("Packet received from WLL module {}:".format(_packet))
                    yield _packet

                else:
                    
                    raise Exception('No data in WLL packet')

        except Exception as e:
            loginf("Failure to get data %s - try %s - (%s)" % (self.url,self.ntries, e))
            self.ntries += 1

    # the hardware does not define a model so use what is in the __init__ settings
    @property
    def hardware_name(self):
        return self.model


    def genLoopPackets(self):

        while self.ntries < self.max_tries:

            try:

                last_good_stamp = self.db_manager.lastGoodStamp()
                now_timestamp_wl = self.get_timestamp_wl_archive(self.wl_archive_interval)

                if last_good_stamp < now_timestamp_wl:

                    logdbg("Request archive from {} to {}".format(last_good_stamp, now_timestamp_wl))

                    for _packet_wl in self.request_wl(last_good_stamp, now_timestamp_wl):

                        self.db_manager.addRecord(_packet_wl)

                else:

                    for _packet_wll in self.request_wll():

                        yield _packet_wll

                    self.ntries = 0
                
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
