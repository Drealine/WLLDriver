# WLLDriver
Created this driver to make request to WeatherLinkLive module including archive from Weatherlink.com when data be lost on Weewx.

Configuration : 

- Copy driver WLLDriver.py on /usr/share/weewx/user
- Change on weewx.conf station_type = WLLDriver
- Know your station ID by following this link : https://weatherlink.github.io/v2-api/authentication
default API request is : https://api.weatherlink.com/v2/stations?api-key=YOURAPIKEY&api-signature=YOURAPISIGNATURE&t=CURRENTTIMESTAMP
- Setting driver by set parameters : 

```
[WLLDriver]
    driver = user.WLLDriver
    max_tries = 50 #Max tries before Weewx raise an exception and finished the loop
    retry_wait = 10 #Time to retry between each
    poll_interval = 5 #The time to sleep between 2 requests
    url = http://toto.com:80/v1/current_conditions #Just replace toto.com by your IP.
    wl_apikey = NN #Create an API Key on your Weatherlink account
    wl_apisecret = NN #By creating API Key, you've also an API Secret
    wl_stationid = NN  #You can view your station ID by following this link : 
    wl_archive_interval = 5 #Be carefull by set this because it depending on your subscription on Weatherlink.com. Please use the same that config on weewx.conf
```

Credits : 

Thank to @vinceskahan on Github who give me examples to make this driver : https://github.com/vinceskahan/weewx-weatherlinklive-json
