#! /usr/bin/python3

import paho.mqtt.client as mqtt
import configparser
import operator
import json
import time
import uptime
import datetime
import re

# global variables
cards = {}
tele = {}
cache = [ {}, {}, {}, {}, {}, {}, {}, {} ]

# read config
config = configparser.ConfigParser()
config.read('config.ini')
if 'MQTT' in config:
    for key in ['TOPIC', 'SERVER', 'PORT', 'QOS', 'TIMEOUT', 'USER', 'PASS']:
        if not config['MQTT'][key]:
            print("Missing or empty config entry MQTT/" + key)
            raise("Missing or empty config entry MQTT/" + key)
else:
    print("Missing config section MQTT")  
    raise("Missing config section MQTT")  

if 'CARDS' in config:
    for key in config['CARDS']:
        if config['CARDS'][key]:
            match = re.match(r'^STACK([0-7])$', key, re.IGNORECASE)
            if match:
                cards[int(match.group(1))] = config['CARDS'][key]
    if not len(cards):
        print("Missing config section CARDS")
        raise("Missing config section CARDS")
else:
    print("Missing config section CARDS")
    raise("Missing config section CARDS")

if 'WATCHDOG' in config:
    for key in ['TIMEOUT', 'BOOT', 'RESET']:
        if not config['WATCHDOG'][key]:
            print("Missing or empty config entry WATCHDOG/" + key)
            raise("Missing or empty config entry WATCHDOG/" + key)
else:
    print("Missing config section WATCHDOG")
    raise("Missing config section WATCHDOG")

if 'HEARTBEAT' in config:
    for key in ['TIMEOUT', 'TOPIC_CHALLENGE', 'TOPIC_RESPONSE']:
        if not config['HEARTBEAT'][key]:
            print("Missing or empty config entry HEARTBEAT/" + key)
            raise("Missing or empty config entry HEARTBEAT/" + key)
else:
    print("Missing config section HEARTBEAT")  
    raise("Missing config section HEARTBEAT")  


for stack in cards.keys():
    if cards[stack] == "megaind":
        try:
            import megaind
        except ImportError:
            raise Exception("Can't import megaind library, is it installed?")
        else:
            cache[stack] = { "response": { "0_10": [ 0, 0, 0, 0 ], "4_20": [ 0, 0, 0, 0 ], "pwm": [ 0, 0, 0, 0 ], "led": [ 0, 0, 0, 0 ], "opto_rce": [ 0, 0, 0, 0 ], "opto_fce": [ 0, 0, 0, 0 ]}, "input": { "0_10": [ 0, 0, 0, 0 ], "pm0_10": [ 0, 0, 0, 0 ], "4_20": [ 0, 0, 0, 0 ], "opto": [ 0, 0, 0, 0 ], "opto_count": [ 0, 0, 0, 0 ] } }
    elif cards[stack] == "megabas":
        try:
            import megabas
        except ImportError:
            raise Exception("Can't import megabas library, is it installed?")
        else:
            cache[stack] = { "response": { "0_10": [ 0, 0, 0, 0 ], "triac": [ 0, 0, 0, 0 ], "cont_rce": [ 0, 0, 0, 0, 0, 0, 0, 0 ], "cont_fce": [ 0, 0, 0, 0, 0, 0, 0, 0 ] }, "input": { "0_10": [ 0, 0, 0, 0, 0, 0, 0, 0 ], "1k": [ 0, 0, 0, 0, 0, 0, 0, 0  ], "10k": [ 0, 0, 0, 0, 0, 0, 0, 0  ], "cont": [ 0, 0, 0, 0, 0, 0, 0, 0  ], "cont_count": [ 0, 0, 0, 0, 0, 0, 0, 0  ] } }
    elif cards[stack] == "8relind":
        try:
            import lib8relind
        except ImportError:
            raise Exception("Can't import lib8relind library, is it installed?")
        else:
            cache[stack] = { "response": { "relay": [ 0, 0, 0, 0, 0, 0, 0, 0 ] } }
    elif cards[stack] == "8inputs":
        try:
            import lib8inputs
        except ImportError:
            raise Exception("Can't import lib8inputs library, is it installed?")
        else:
            cache[stack] = { "input": { "opto": [ 0, 0, 0, 0, 0, 0, 0, 0 ] } }
    elif cards[stack] == "rtd":
        try:
            import librtd
        except ImportError:
            raise Exception("Can't import librtd library, is it installed?")
        else:
            cache[stack] = { "input": { "rtd": [ 0, 0, 0, 0, 0, 0, 0, 0 ] } }
    else:
        print("Uknown card type " + cards[stack])
        raise NotImplementedError("Uknown card type " + cards[stack])


def get_megaind(stack, init):
    for channel in range(1,5):
        value = megaind.get0_10Out(stack, channel)
        if init or value != cache[stack]["response"]["0_10"][channel - 1]:
            client.publish(config['MQTT']['TOPIC'] + '/megaind/' + str(stack) + '/response/0_10/' + str(channel), value, int(config['MQTT']['QOS']))
            cache[stack]["response"]["0_10"][channel - 1] = value

        value = megaind.get4_20Out(stack, channel)
        if init or value != cache[stack]["response"]["4_20"][channel - 1]:
            client.publish(config['MQTT']['TOPIC'] + '/megaind/' + str(stack) + '/response/4_20/' + str(channel), value, int(config['MQTT']['QOS']))
            cache[stack]["response"]["4_20"][channel - 1] = value

        value = megaind.getOdPWM(stack, channel)
        if init or value != cache[stack]["response"]["pwm"][channel - 1]:
            client.publish(config['MQTT']['TOPIC'] + '/megaind/' + str(stack) + '/response/pwm/' + str(channel), value, int(config['MQTT']['QOS']))
            cache[stack]["response"]["pwm"][channel - 1] = value

        value = megaind.getLed(stack, channel)
        if init or value != cache[stack]["response"]["led"][channel - 1]:
            client.publish(config['MQTT']['TOPIC'] + '/megaind/' + str(stack) + '/response/led/' + str(channel), value, int(config['MQTT']['QOS']))
            cache[stack]["response"]["led"][channel - 1] = value

        value = megaind.getOptoRisingCountEnable(stack, channel)
        if init or value != cache[stack]["response"]["opto_rce"][channel - 1]:
            client.publish(config['MQTT']['TOPIC'] + '/megaind/' + str(stack) + '/response/opto_rce/' + str(channel), value, int(config['MQTT']['QOS']))
            cache[stack]["response"]["opto_rce"][channel - 1] = value

        value = megaind.getOptoFallingCountEnable(stack, channel)
        if init or value != cache[stack]["response"]["opto_fce"][channel - 1]:
            client.publish(config['MQTT']['TOPIC'] + '/megaind/' + str(stack) + '/response/opto_fce/' + str(channel), value, int(config['MQTT']['QOS']))
            cache[stack]["response"]["opto_fce"][channel - 1] = value

        value = round(megaind.get0_10In(stack, channel), 2)
        if init or value != cache[stack]["input"]["0_10"][channel - 1]:
            client.publish(config['MQTT']['TOPIC'] + '/megaind/' + str(stack) + '/input/0_10/' + str(channel), value, int(config['MQTT']['QOS']))
            cache[stack]["input"]["0_10"][channel - 1] = value

        value = round(megaind.getpm10In(stack, channel), 2)
        if init or value != cache[stack]["input"]["pm0_10"][channel - 1]:
            client.publish(config['MQTT']['TOPIC'] + '/megaind/' + str(stack) + '/input/pm0_10/' + str(channel), value, int(config['MQTT']['QOS']))
            cache[stack]["input"]["pm0_10"][channel - 1] = value

        value = megaind.get4_20In(stack, channel)
        if init or value != cache[stack]["input"]["4_20"][channel - 1]:
            client.publish(config['MQTT']['TOPIC'] + '/megaind/' + str(stack) + '/input/4_20/' + str(channel), value, int(config['MQTT']['QOS']))
            cache[stack]["input"]["4_20"][channel - 1] = value

        value = megaind.getOptoCh(stack, channel)
        if init or value != cache[stack]["input"]["opto"][channel - 1]:
            client.publish(config['MQTT']['TOPIC'] + '/megaind/' + str(stack) + '/input/opto/' + str(channel), value, int(config['MQTT']['QOS']))
            cache[stack]["input"]["opto"][channel - 1] = value

        value = megaind.getOptoCount(stack, channel)
        if init or value != cache[stack]["input"]["opto_count"][channel - 1]:
            client.publish(config['MQTT']['TOPIC'] + '/megaind/' + str(stack) + '/input/opto_count/' + str(channel), value, int(config['MQTT']['QOS']))
            cache[stack]["input"]["opto_count"][channel - 1] = value


def set_megaind(stack, output, channel, value):
    if output == "0_10":
        try:
            value = float(value)
            megaind.set0_10Out(stack, channel, value)
            value == megaind.get0_10Out(stack, channel)
            client.publish(config['MQTT']['TOPIC'] + '/megaind/' + str(stack) + '/response/0_10/' + str(channel), value, int(config['MQTT']['QOS']))
        except:
            print("Can't set megaind stack: " + str(stack) + ", response: 0_10, channel: " + str(channel) + " to value: " + str(value))
            raise("Can't set megaind stack: " + str(stack) + ", response: 0_10, channel: " + str(channel) + " to value: " + str(value))
        else:
            cache[stack]["response"]["0_10"][channel - 1] = value
    elif output == "4_20":
        try:
            value = float(value)
            megaind.set4_20Out(stack, channel, value)
            value == megaind.get0_10Out(stack, channel)
            client.publish(config['MQTT']['TOPIC'] + '/megaind/' + str(stack) + '/response/4_20/' + str(channel), value, int(config['MQTT']['QOS']))
        except:
            print("Can't set megaind stack: " + str(stack) + ", response: 4_20, channel: " + str(channel) + " to value: " + str(value))
            raise("Can't set megaind stack: " + str(stack) + ", response: 4_20, channel: " + str(channel) + " to value: " + str(value))
        else:
            cache[stack]["response"]["4_20"][channel - 1] = value
    elif output == "pwm":
        try:
            value = float(value)
            megaind.setOdPWM(stack, channel, value)
            value = megaind.get0_10Out(stack, channel)
            client.publish(config['MQTT']['TOPIC'] + '/megaind/' + str(stack) + '/response/pwm/' + str(channel), value, int(config['MQTT']['QOS']))
        except:
            print("Can't set megaind stack: " + str(stack) + ", response: pwm, channel: " + str(channel) + " to value: " + str(value))
            raise("Can't set megaind stack: " + str(stack) + ", response: pwm, channel: " + str(channel) + " to value: " + str(value))
        else:
            cache[stack]["response"]["pwm"][channel - 1] = value
    elif output == "led":
        try:
            value = int(value)
            megaind.setLed(stack, channel, value)
            value = megaind.get0_10Out(stack, channel)
            client.publish(config['MQTT']['TOPIC'] + '/megaind/' + str(stack) + '/response/led/' + str(channel), value, int(config['MQTT']['QOS']))
        except:
            print("Can't set megaind stack: " + str(stack) + ", response: led, channel: " + str(channel) + " to value: " + str(value))
            raise("Can't set megaind stack: " + str(stack) + ", response: led, channel: " + str(channel) + " to value: " + str(value))
        else:
            cache[stack]["response"]["led"][channel - 1] = value
    elif output == "opto_rce":
        try:
            value = int(value)
            megaind.setOptoRisingCountEnable(stack, channel, value)
            value = megaind.getOptoRisingCountEnable(stack, channel)
            client.publish(config['MQTT']['TOPIC'] + '/megaind/' + str(stack) + '/response/opto_rce/' + str(channel), value, int(config['MQTT']['QOS']))
        except:
            print("Can't set megaind stack: " + str(stack) + ", response: opto_rce, channel: " + str(channel) + " to value: " + str(value))
            raise("Can't set megaind stack: " + str(stack) + ", response: opto_rce, channel: " + str(channel) + " to value: " + str(value))
        else:
            cache[stack]["response"]["opto_rce"][channel - 1] = value
    elif output == "opto_fce":
        try:
            value = int(value)
            megaind.setOptoFallingCountEnable(stack, channel, value)
            value = megaind.getOptoFallingCountEnable(stack, channel)
            client.publish(config['MQTT']['TOPIC'] + '/megaind/' + str(stack) + '/response/opto_fce/' + str(channel), value, int(config['MQTT']['QOS']))
        except:
            print("Can't set megaind stack: " + str(stack) + ", response: opto_fce, channel: " + str(channel) + " to value: " + str(value))
            raise("Can't set megaind stack: " + str(stack) + ", response: opto_fce, channel: " + str(channel) + " to value: " + str(value))
        else:
            cache[stack]["response"]["opto_fce"][channel - 1] = value
    elif output == "opto_rst" and bool(value):
        try:
            megaind.rstOptoCount(stack, channel)
            value = megaind.get0_10Out(stack, channel)
            client.publish(config['MQTT']['TOPIC'] + '/megaind/' + str(stack) + '/output/opto_rst/' + str(channel), 0, int(config['MQTT']['QOS']))
            client.publish(config['MQTT']['TOPIC'] + '/megaind/' + str(stack) + '/input/opto_count/' + str(channel), value, int(config['MQTT']['QOS']))
        except:
            print("Can't set megaind stack: " + str(stack) + ", output: opto_rst, channel: " + str(channel) + " to value: 0")
            raise("Can't set megaind stack: " + str(stack) + ", output: opto_rst, channel: " + str(channel) + " to value: 0")
        else:
            cache[stack]["input"]["opto_count"][channel - 1] = value
    else:
        print("Can't set megaind stack: " + str(stack) + ", topic: " + output + ", channel: " + str(channel) + " to value: 0")
        raise("Can't set megaind stack: " + str(stack) + ", topic: " + output + ", channel: " + str(channel) + " to value: 0")


def reset_megaind(stack):
    for channel in range(1,5):
        for output in ( '4_20', '0_10', 'pwm', 'led' ):
            set_megaind(stack, output, channel, 0)


def tele_megaind(stack):
    if megaind.getPowerVolt(stack) < 5:
        return False
    tele["master"] = "megaind" + str(stack)
    tele["fw"] = megaind.getFwVer(stack)
    tele["power_in"] = megaind.getPowerVolt(stack)
    tele["power_rsp"] = megaind.getRaspVolt(stack)
    tele["cpu_temp"] = megaind.getCpuTemp(stack)
    tele["wtd_resets"] = megaind.wdtGetResetCount(stack)
    return True


def watchdog_megaind(stack, mode):
    if megaind.getPowerVolt(stack) < 5:
        return False
    if mode == 1:
        if megaind.wdtGetPeriod(stack) != int(config['WATCHDOG']['TIMEOUT']):
            megaind.wdtSetPeriod(stack, int(config['WATCHDOG']['TIMEOUT']))
        if megaind.wdtGetDefaultPeriod(stack) != int(config['WATCHDOG']['BOOT']):
            megaind.wdtSetDefaultPeriod(stack, int(config['WATCHDOG']['BOOT']))
        if megaind.wdtGetOffInterval(stack) != int(config['WATCHDOG']['RESET']):
            megaind.wdtSetOffInterval(stack, int(config['WATCHDOG']['RESET']))
    elif mode == 2:
        #megaind.wdtSetPeriod(stack, 65000)
        print("megabas.wdtSetPeriod")
    else:
        megaind.wdtReload(stack)
    return True


def get_megabas(stack, init):
    triacs = megabas.getTriacs(stack)
    for channel in range(1,5):
        value = megabas.getUOut(stack, channel)
        if init or value != cache[stack]["response"]["0_10"][channel - 1]:
            client.publish(config['MQTT']['TOPIC'] + '/megabas/' + str(stack) + '/response/0_10/' + str(channel), value, int(config['MQTT']['QOS']))
            cache[stack]["response"]["0_10"][channel - 1] = value
        
        if triacs & (1 << channel - 1):
            value = 1
        else:
            value = 0
        if init or value != cache[stack]["response"]["triac"][channel - 1]:
            client.publish(config['MQTT']['TOPIC'] + '/megabas/' + str(stack) + '/response/triac/' + str(channel), value, int(config['MQTT']['QOS']))
            cache[stack]["response"]["triac"][channel - 1] = value

    for channel in range(1,9):
        value = round(megabas.getUIn(stack, channel),2)
        if init or value != cache[stack]["input"]["0_10"][channel - 1]:
            client.publish(config['MQTT']['TOPIC'] + '/megabas/' + str(stack) + '/input/0_10/' + str(channel), value, int(config['MQTT']['QOS']))
            cache[stack]["input"]["0_10"][channel - 1] = value

        value = round(megabas.getRIn1K(stack, channel),2)
        if init or value != cache[stack]["input"]["1k"][channel - 1]:
            client.publish(config['MQTT']['TOPIC'] + '/megabas/' + str(stack) + '/input/1k/' + str(channel), value, int(config['MQTT']['QOS']))
            cache[stack]["input"]["0_10"][channel - 1] = value

        value = round(megabas.getRIn10K(stack, channel), 2)
        if init or value != cache[stack]["input"]["10k"][channel - 1]:
            client.publish(config['MQTT']['TOPIC'] + '/megabas/' + str(stack) + '/input/10k/' + str(channel), value, int(config['MQTT']['QOS']))
            cache[stack]["input"]["10k"][channel - 1] = value

        value = megabas.getContactCh(stack, channel)
        if init or value != cache[stack]["input"]["cont"][channel - 1]:
            client.publish(config['MQTT']['TOPIC'] + '/megabas/' + str(stack) + '/input/cont/' + str(channel), value, int(config['MQTT']['QOS']))
            cache[stack]["input"]["cont"][channel - 1] = value

        value = megabas.getContactCounter(stack, channel)
        if init or value != cache[stack]["input"]["cont_count"][channel - 1]:
            client.publish(config['MQTT']['TOPIC'] + '/megabas/' + str(stack) + '/input/cont_count/' + str(channel), value, int(config['MQTT']['QOS']))
            cache[stack]["input"]["cont_count"][channel - 1] = value

        value = megabas.getContactCountEdge(stack, channel)
        if value == 1:
            raising = 1
            falling = 0
        elif value == 2:
            raising = 0
            falling = 1
        elif value == 3:
            raising = 1
            falling = 1
        else:
            raising = 0
            falling = 0
        if init or raising != cache[stack]["response"]["cont_rce"][channel - 1]:
            client.publish(config['MQTT']['TOPIC'] + '/megabas/' + str(stack) + '/response/cont_rce/' + str(channel), value, int(config['MQTT']['QOS']))
            cache[stack]["response"]["cont_rce"][channel - 1] = raising
        if init or falling != cache[stack]["response"]["cont_fce"][channel - 1]:
            client.publish(config['MQTT']['TOPIC'] + '/megabas/' + str(stack) + '/response/cont_fce/' + str(channel), value, int(config['MQTT']['QOS']))
            cache[stack]["response"]["cont_fce"][channel - 1] = falling

    megabas.getTriacs(stack)

def set_megabas(stack, output, channel, value):
    if output == "0_10":
        try:
            megabas.setUOut(stack, channel, value)
            value = megabas.getUOut(stack, channel)
            client.publish(config['MQTT']['TOPIC'] + '/megabas/' + str(stack) + '/response/0_10/' + str(channel), value, int(config['MQTT']['QOS']))
        except:
            print("Can't set megabas stack: " + str(stack) + ", response: 0_10, channel: " + str(channel) + " to value: " + str(value))
            raise("Can't set megabas stack: " + str(stack) + ", response: 0_10, channel: " + str(channel) + " to value: " + str(value))
        else:
            cache[stack]["response"]["0_10"][channel - 1] = value
    elif output == "triac":
        try:
            megabas.setTriac(stack, channel, value)
            triacs = megabas.getTriacs(stack)
            if triacs & (1 << channel - 1):
                value = 1
            else:
                value = 0
            client.publish(config['MQTT']['TOPIC'] + '/megabas/' + str(stack) + '/response/triac/' + str(channel), value, int(config['MQTT']['QOS']))
        except:
            print("Can't set megabas stack: " + str(stack) + ", response: triac, channel: " + str(channel) + " to value: " + str(value))
            raise("Can't set megabas stack: " + str(stack) + ", response: triac, channel: " + str(channel) + " to value: " + str(value))
        else:
            cache[stack]["response"]["triac"][channel - 1] = value
    elif output == "cont_rce":
        if value == 0 and cache[stack]["response"]["cont_fce"][channel - 1] == 1:
            value = 2
        elif value == 1 and cache[stack]["response"]["cont_fce"][channel - 1] == 0:
            value = 1
        elif value == 1 and cache[stack]["response"]["cont_fce"][channel - 1] == 1:
            value = 3
        else:
            value = 0
        try:
            megabas.setContactCountEdge(stack, channel, value)
            value = megabas.getContactCountEdge(stack, channel)
            if value == 1 or value == 3:
                value = 1
            else:
                value = 0
            client.publish(config['MQTT']['TOPIC'] + '/megabas/' + str(stack) + '/input/cont_rce/' + str(channel), value, int(config['MQTT']['QOS']))
        except:
            print("Can't set megabas stack: " + str(stack) + ", input: cont_rce, channel: " + str(channel) + " to value: " + str(value))
            raise("Can't set megabas stack: " + str(stack) + ", input: cont_rce, channel: " + str(channel) + " to value: " + str(value))
        else:
            cache[stack]["response"]["cont_rce"][channel - 1] = value
    elif output == "cont_fce":
        if value == 0 and cache[stack]["response"]["cont_rce"][channel - 1] == 1:
            value = 1
        elif value == 1 and cache[stack]["response"]["cont_rce"][channel - 1] == 0:
            value = 2
        elif value == 1 and cache[stack]["response"]["cont_rce"][channel - 1] == 1:
            value = 3
        else:
            value = 0
        try:
            megabas.setContactCountEdge(stack, channel, value)
            value = megabas.getContactCountEdge(stack, channel)
            if value == 2 or value == 3:
                value = 1
            else:
                value = 0
            client.publish(config['MQTT']['TOPIC'] + '/megabas/' + str(stack) + '/input/cont_fce/' + str(channel), value, int(config['MQTT']['QOS']))
        except:
            print("Can't set megabas stack: " + str(stack) + ", input: cont_fce, channel: " + str(channel) + " to value: " + str(value))
            raise("Can't set megabas stack: " + str(stack) + ", input: cont_fce, channel: " + str(channel) + " to value: " + str(value))
        else:
            cache[stack]["response"]["cont_fce"][channel - 1] = value
    else:
        print("Can't set megabas stack: " + str(stack) + ", topic: " + output + ", channel: " + str(channel) + " to value: 0")
        raise("Can't set megabas stack: " + str(stack) + ", topic: " + output + ", channel: " + str(channel) + " to value: 0")


def reset_megabas(stack):
    for channel in range(1,5):
        for output in ( '0_10', 'triac' ):
            set_megabas(stack, output, channel, 0)


def tele_megabas(stack):
    if megabas.getInVolt(stack) < 5:
        return False
    tele["master"] = "megabas" + str(stack)
    tele["fw"] = megabas.getVer(stack)
    tele["power_in"] = megabas.getInVolt(stack)
    tele["power_rsp"] = megabas.getRaspVolt(stack)
    tele["cpu_temp"] = megabas.getCpuTemp(stack)
    tele["wtd_resets"] = megabas.wdtGetResetCount(stack)
    return True


def watchdog_megabas(stack, mode):
    if megabas.getInVolt(stack) < 5:
        return False
    if mode == 1:
        if megabas.wdtGetPeriod(stack) != int(config['WATCHDOG']['TIMEOUT']):
            megabas.wdtSetPeriod(stack, int(config['WATCHDOG']['TIMEOUT']))
        if megabas.wdtGetDefaultPeriod(stack) != int(config['WATCHDOG']['BOOT']):
            megabas.wdtSetDefaultPeriod(stack, int(config['WATCHDOG']['BOOT']))
        if megabas.wdtGetOffInterval(stack) != int(config['WATCHDOG']['RESET']):
            megabas.wdtSetOffInterval(stack, int(config['WATCHDOG']['RESET']))
    elif mode == 2:
        #megabas.wdtSetPeriod(stack, 65000)
        print("megabas.wdtSetPeriod")
    else:
        megabas.wdtReload(stack)
    return True


def get_8relind(stack, init):
    for channel in range(1,9):
        value = lib8relind.get(stack, channel)
        if init or value != cache[stack]["response"]["relay"][channel - 1]:
            client.publish(config['MQTT']['TOPIC'] + '/8relind/' + str(stack) + '/response/relay/' + str(channel), value, int(config['MQTT']['QOS']))
            cache[stack]["response"]["relay"][channel - 1] = value


def set_8relind(stack, output, channel, value):
    if output == "relay":
        try:
            value = int(value)
            lib8relind.set(stack, channel, value)
            value = lib8relind.get(stack, channel)
            client.publish(config['MQTT']['TOPIC'] + '/8relind/' + str(stack) + '/response/relay/' + str(channel), value, int(config['MQTT']['QOS']))
        except:
            print("Can't set 8relind stack: " + str(stack) + ", response: relay, channel: " + str(channel) + " to value: " + str(value))
            raise("Can't set 8relind stack: " + str(stack) + ", response: relay, channel: " + str(channel) + " to value: " + str(value))
        else:
            cache[stack]["response"]["relay"][channel - 1] = value
    else:
        print("Can't set 8relind stack: " + str(stack) + ", topic: " + output + ", channel: " + str(channel) + " to value: " + str(value))
        raise("Can't set 8relind stack: " + str(stack) + ", topic: " + output + ", channel: " + str(channel) + " to value: " + str(value))


def reset_8relind(stack):
    for channel in range(1,9):
        set_8relind(stack, 'relay', channel, 0)


def get_8inputs(stack, init):
    for channel in range(1,9):
        value = lib8inputs.get_opto(stack, channel)
        if init or value != cache[stack]["input"]["opto"][channel - 1]:
            client.publish(config['MQTT']['TOPIC'] + '/8inputs/' + str(stack) + '/input/opto/' + str(channel), value, int(config['MQTT']['QOS']))
            cache[stack]["input"]["opto"][channel - 1] = value


def get_rtd(stack, init):
    for channel in range(1,9):
        value = librtd.get(stack, channel)
        if init or value != cache[stack]["input"]["rtd"][channel - 1]:
            client.publish(config['MQTT']['TOPIC'] + '/rtd/' + str(stack) + '/input/rtd/' + str(channel), value, int(config['MQTT']['QOS']))
            cache[stack]["input"]["rtd"][channel - 1] = value


def cards_init():
    client.subscribe(config['MQTT']['TOPIC'] + '/tele/CMND/+', int(config['MQTT']['QOS']))
    cards_tele(1)
    for stack in cards.keys():
        if cards[stack] == "megaind":
            client.subscribe(config['MQTT']['TOPIC'] + '/' + cards[stack] + '/' + str(stack) + '/output/#', int(config['MQTT']['QOS']))
            get_megaind(stack, 1)
            watchdog_megaind(stack, 1)
        elif cards[stack] == "megabas":
            client.subscribe(config['MQTT']['TOPIC'] + '/' + cards[stack] + '/' + str(stack) + '/output/#', int(config['MQTT']['QOS']))
            get_megabas(stack, 1)
            watchdog_megabas(stack, 1)
        elif cards[stack] == "8relind":
            client.subscribe(config['MQTT']['TOPIC'] + '/' + cards[stack] + '/' + str(stack) + '/output/#', int(config['MQTT']['QOS']))
            get_8relind(stack, 1)
        elif cards[stack] == "8inputs":
            get_8inputs(stack, 1)
        elif cards[stack] == "rtd":
            get_rtd(stack, 1)
        else:
            print("Uknown card type " + cards[stack])
            raise NotImplementedError("Uknown card type " + cards[stack])
    client.subscribe(config['MQTT']['TOPIC'] + '/' + config['HEARTBEAT']['TOPIC_CHALLENGE'])


def cards_update(mode):
    if cards_tele(mode):
        mode = 1
    else:
        mode = 0
    for stack in cards.keys():
        if cards[stack] == "megaind":
            get_megaind(stack, mode)
        elif cards[stack] == "megabas":
            get_megabas(stack, mode)
        elif cards[stack] == "8relind":
            get_8relind(stack, mode)
        elif cards[stack] == "8inputs":
            get_8inputs(stack, mode)
        elif cards[stack] == "rtd":
            get_rtd(stack, mode)
        else:
            print("Uknown card type " + cards[stack])
            raise NotImplementedError("Uknown card type " + cards[stack])


def cards_unsubscribe():
    client.unsubscribe(config['MQTT']['TOPIC'] + '/tele/CMND/+', int(config['MQTT']['QOS']))
    for stack in cards.keys():
        if cards[stack] == "megaind":
            watchdog_megaind(stack, 2)
        elif cards[stack] == "megabas":
            watchdog_megabas(stack, 2)
        client.unsubscribe(config['MQTT']['TOPIC'] + '/' + cards[stack] + '/#', int(config['MQTT']['QOS']))


def cards_tele(mode):
    global last_tele
    now = time.time()
    if now - last_tele > 300 or mode == 1:
        get_time()
        for stack in cards.keys():
            if cards[stack] == "megaind":
                if tele_megaind(stack):
                    break
            elif cards[stack] == "megabas":
                if tele_megabas(stack):
                    break
        client.publish(config['MQTT']['TOPIC'] + '/tele/STATE', json.dumps(tele), int(config['MQTT']['QOS']))
        last_tele = now
        return True
    else:
        return False


def cards_watchdog():
    global last_watchdog
    now = int(time.time())
    if now - last_watchdog > int(config['WATCHDOG']['TIMEOUT']) / 3:
        for stack in cards.keys():
            if cards[stack] == "megaind":
                watchdog_megaind(stack, 0)
            elif cards[stack] == "megabas":
                watchdog_megabas(stack, 0)
        last_watchdog = now
        return True
    else:
        return False


def check_heartbeat(mode):
    global last_heartbeat
    now = int(time.time())
    if mode == 1:
        last_heartbeat = now
        client.publish(config['MQTT']['TOPIC'] + '/' + config['HEARTBEAT']['TOPIC_RESPONSE'], str(now), int(config['MQTT']['QOS']))
        return True
    elif int(config['HEARTBEAT']['TIMEOUT']) > 0 and last_heartbeat >= 0 and now - last_heartbeat > int(config['HEARTBEAT']['TIMEOUT']):
        for stack in cards.keys():
            if cards[stack] == "megaind":
                reset_megaind(stack)
            elif cards[stack] == "megabas":
                reset_megabas(stack)
            elif cards[stack] == "8relind":
                reset_8relind(stack)
        last_heartbeat = -1
        return False
    else:
        return True


def get_time():
    result = ""
    time = uptime.uptime()
    result = "%01d" % int(time / 86400)
    time = time % 86400
    result = result + "T" + "%02d" % (int(time / 3600))
    time = time % 3600
    tele["Uptime"] = result + ":" + "%02d" % (int(time / 60)) + ":" + "%02d" % (time % 60)
    tele["Time"] = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    if rc != 0:
        print('MQTT unexpected connect return code ' + str(rc))
    else:
        print('MQTT client connected')
        client.connected_flag = 1


def on_disconnect(client, userdata, rc):
    client.connected_flag = 0
    if rc != 0:
        print('MQTT unexpected disconnect return code ' + str(rc))
        print('MQTT client disconnected')


# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    match = re.match(r'^' + config['MQTT']['TOPIC'] + '\/tele/CMND\/(state)$', str(msg.topic))
    match1 = re.match(r'^' + config['MQTT']['TOPIC'] + '\/megaind\/([0-7])\/output\/(0_10|4_20|pwm|led|opto_rce|opto_fce|opto_rst)\/([1-4])$', str(msg.topic))
    match2 = re.match(r'^' + config['MQTT']['TOPIC'] + '\/megabas\/([0-7])\/output\/(0_10|triac|opto_rce|opto_fce)\/([1-4])$', str(msg.topic))
    match3 = re.match(r'^' + config['MQTT']['TOPIC'] + '\/8relind\/([0-7])\/output\/(relay)/([1-8])$', str(msg.topic))
    
    if match:
        topic = match.group(1)
        payload = str(msg.payload.decode("utf-8"))
        if topic == "state" and payload == "":
            cards_update(1)
    else:
        value = msg.payload.decode("utf-8")
        match_i = re.match(r'^(\d+)$', msg.payload.decode("utf-8"))
        match_f = re.match(r'^(\d+\.\d+)$', msg.payload.decode("utf-8"))
        if match_i:
            value = int(match_i.group(1))
        elif match_f:
            value = float(match_f.group(1))
        else:
            print('Unknown value: ' + msg.topic + ', Message: ' + str(value))
            raise NotImplementedError('Unknown topic: ' + msg.topic + ', Message: ' + str(value))
            return False
        if match1:
            stack = int(match1.group(1))
            output = match1.group(2)
            channel = int(match1.group(3))
            set_megaind(stack, output, channel, value)
        elif match2:
            stack = int(match2.group(1))
            output = match2.group(2)
            channel = int(match2.group(3))
            set_megabas(stack, output, channel, value)
        elif match3:
            stack = int(match3.group(1))
            output = match3.group(2)
            channel = int(match3.group(3))
            set_8relind(stack, output, channel, value)
        else:
            print('Unknown topic: ' + msg.topic + ', Message: ' + str(value))
            raise NotImplementedError('Unknown topic: ' + msg.topic + ', Message: ' + str(value))


# Add connection flags
mqtt.Client.connected_flag = 0
mqtt.Client.reconnect_count = 0

# Imain loop
run = 1
last_heartbeat = int(time.time())
while run:
    try:
        # Init counters
        last_tele = 0
        last_watchdog = 0
        # Heartbeat check
        check_heartbeat(0)
        # Create mqtt client
        client = mqtt.Client()
        client.connected_flag = 0
        client.reconnect_count = 0
        # Register LWT message
        client.will_set(config['MQTT']['TOPIC'] + '/tele/LWT', payload="Offline", qos=0, retain=True)
        # Register connect callback
        client.on_connect = on_connect
        # Register disconnect callback
        client.on_disconnect = on_disconnect
        # Registed publish message callback
        client.on_message = on_message
        # Set access token
        client.username_pw_set(config['MQTT']['USER'], config['MQTT']['PASS'])
        # Run receive thread
        client.loop_start()
        # Connect to broker
        client.connect(config['MQTT']['SERVER'], int(config['MQTT']['PORT']), int(config['MQTT']['TIMEOUT']))
        time.sleep(1)
        while not client.connected_flag:
            print("MQTT waiting to connect")
            client.reconnect_count += 1
            if client.reconnect_count > 10:
                print("MQTT restarting connection!")
                raise("MQTT restarting connection!")
            time.sleep(1)
        # Sent LWT update
        client.publish(config['MQTT']['TOPIC'] + '/tele/LWT',payload="Online", qos=0, retain=True)
        # init cards inputs and subscribe for output topics
        cards_init()
        # Run sending thread
        while True:
            if client.connected_flag:
                cards_update(0)
                cards_watchdog()
                if not check_heartbeat(0):
                    print("Missing heartbeat, all cards outputs reseted!")
                    raise("Missing heartbeat, all cards outputs reseted!")
            else:
                print("MQTT connection lost!")
                raise("MQTT connection lost!")
            time.sleep(1)
    except KeyboardInterrupt:
        # Gracefull shutwdown
        run = 0
        client.loop_stop()
        if client.connected_flag:
            cards_unsubscribe()
            client.disconnect()
    except:
        client.loop_stop()
        if client.connected_flag:
            client.disconnect()
        del client
        time.sleep(5)
