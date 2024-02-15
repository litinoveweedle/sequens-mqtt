#! /usr/bin/python3

import paho.mqtt.client as mqtt
import operator
import json
import time
import uptime
import datetime
import re


DEVICE = 'hvac_control'
MQTT_SERVER = '192.168.3.2'
MQTT_PORT = 1883
MQTT_QOS = 1
MQTT_TIMEOUT = 5
MQTT_USER = 'mqttuser'
MQTT_PASS = 'mqttpass'

WATCHDOG_TIMEOUT = 120
WATCHDOG_BOOT = 300
WATCHDOG_RESET = 10

#Sequens Microsystems card installed and stack numbers
CARDS = [ "megaind", "8relind", "8inputs" ]
#CARDS = [ "megaind", "8relind", "8inputs", "rtd" ]


tele = {}
cache = [ {}, {}, {}, {}, {}, {}, {}, {} ]

for stack, card in enumerate(CARDS):
    if card == "megaind":
        try:
            import megaind
        except ImportError:
            raise Exception("Can't import megaind library, is it installed?")
        else:
            cache[stack] = { "response": { "0_10": [ 0, 0, 0, 0 ], "4_20": [ 0, 0, 0, 0 ], "pwm": [ 0, 0, 0, 0 ], "led": [ 0, 0, 0, 0 ], "opto_rce": [ 0, 0, 0, 0 ], "opto_fce": [ 0, 0, 0, 0 ]}, "input": { "0_10": [ 0, 0, 0, 0 ], "pm0_10": [ 0, 0, 0, 0 ], "4_20": [ 0, 0, 0, 0 ], "opto": [ 0, 0, 0, 0 ], "opto_count": [ 0, 0, 0, 0 ] } }
    elif card == "megabas":
        try:
            import megabas
        except ImportError:
            raise Exception("Can't import megabas library, is it installed?")
        else:
            cache[stack] = { "response": { "0_10": [ 0, 0, 0, 0 ], "triac": [ 0, 0, 0, 0 ], "cont_rce": [ 0, 0, 0, 0, 0, 0, 0, 0 ], "cont_fce": [ 0, 0, 0, 0, 0, 0, 0, 0 ] }, "input": { "0_10": [ 0, 0, 0, 0, 0, 0, 0, 0 ], "1k": [ 0, 0, 0, 0, 0, 0, 0, 0  ], "10k": [ 0, 0, 0, 0, 0, 0, 0, 0  ], "cont": [ 0, 0, 0, 0, 0, 0, 0, 0  ], "cont_count": [ 0, 0, 0, 0, 0, 0, 0, 0  ] } }
    elif card == "8relind":
        try:
            import lib8relind
        except ImportError:
            raise Exception("Can't import lib8relind library, is it installed?")
        else:
            cache[stack] = { "response": { "relay": [ 0, 0, 0, 0, 0, 0, 0, 0 ] } }
    elif card == "8inputs":
        try:
            import lib8inputs
        except ImportError:
            raise Exception("Can't import lib8inputs library, is it installed?")
        else:
            cache[stack] = { "input": { "opto": [ 0, 0, 0, 0, 0, 0, 0, 0 ] } }
    elif card == "rtd":
        try:
            import librtd
        except ImportError:
            raise Exception("Can't import librtd library, is it installed?")
        else:
            cache[stack] = { "input": { "rtd": [ 0, 0, 0, 0, 0, 0, 0, 0 ] } }
    else:
        print("Uknown card type " + card)
        raise NotImplementedError("Uknown card type " + card)


def get_megaind(stack, init):
    for channel in range(1,5):
        value = megaind.get0_10Out(stack, channel)
        if init or value != cache[stack]["response"]["0_10"][channel - 1]:
            client.publish(DEVICE + '/megaind/' + str(stack) + '/response/0_10/' + str(channel), value, MQTT_QOS)
            cache[stack]["response"]["0_10"][channel - 1] = value

        value = megaind.get4_20Out(stack, channel)
        if init or value != cache[stack]["response"]["4_20"][channel - 1]:
            client.publish(DEVICE + '/megaind/' + str(stack) + '/response/4_20/' + str(channel), value, MQTT_QOS)
            cache[stack]["response"]["4_20"][channel - 1] = value

        value = megaind.getOdPWM(stack, channel)
        if init or value != cache[stack]["response"]["pwm"][channel - 1]:
            client.publish(DEVICE + '/megaind/' + str(stack) + '/response/pwm/' + str(channel), value, MQTT_QOS)
            cache[stack]["response"]["pwm"][channel - 1] = value

        value = megaind.getLed(stack, channel)
        if init or value != cache[stack]["response"]["led"][channel - 1]:
            client.publish(DEVICE + '/megaind/' + str(stack) + '/response/led/' + str(channel), value, MQTT_QOS)
            cache[stack]["response"]["led"][channel - 1] = value

        value = megaind.getOptoRisingCountEnable(stack, channel)
        if init or value != cache[stack]["response"]["opto_rce"][channel - 1]:
            client.publish(DEVICE + '/megaind/' + str(stack) + '/response/opto_rce/' + str(channel), value, MQTT_QOS)
            cache[stack]["response"]["opto_rce"][channel - 1] = value

        value = megaind.getOptoFallingCountEnable(stack, channel)
        if init or value != cache[stack]["response"]["opto_fce"][channel - 1]:
            client.publish(DEVICE + '/megaind/' + str(stack) + '/response/opto_fce/' + str(channel), value, MQTT_QOS)
            cache[stack]["response"]["opto_fce"][channel - 1] = value

        value = round(megaind.get0_10In(stack, channel), 2)
        if init or value != cache[stack]["input"]["0_10"][channel - 1]:
            client.publish(DEVICE + '/megaind/' + str(stack) + '/input/0_10/' + str(channel), value, MQTT_QOS)
            cache[stack]["input"]["0_10"][channel - 1] = value

        value = round(megaind.getpm10In(stack, channel), 2)
        if init or value != cache[stack]["input"]["pm0_10"][channel - 1]:
            client.publish(DEVICE + '/megaind/' + str(stack) + '/input/pm0_10/' + str(channel), value, MQTT_QOS)
            cache[stack]["input"]["pm0_10"][channel - 1] = value

        value = megaind.get4_20In(stack, channel)
        if init or value != cache[stack]["input"]["4_20"][channel - 1]:
            client.publish(DEVICE + '/megaind/' + str(stack) + '/input/4_20/' + str(channel), value, MQTT_QOS)
            cache[stack]["input"]["4_20"][channel - 1] = value

        value = megaind.getOptoCh(stack, channel)
        if init or value != cache[stack]["input"]["opto"][channel - 1]:
            client.publish(DEVICE + '/megaind/' + str(stack) + '/input/opto/' + str(channel), value, MQTT_QOS)
            cache[stack]["input"]["opto"][channel - 1] = value

        value = megaind.getOptoCount(stack, channel)
        if init or value != cache[stack]["input"]["opto_count"][channel - 1]:
            client.publish(DEVICE + '/megaind/' + str(stack) + '/input/opto_count/' + str(channel), value, MQTT_QOS)
            cache[stack]["input"]["opto_count"][channel - 1] = value


def set_megaind(stack, output, channel, value):
    if output == "0_10":
        try:
            value = float(value)
            megaind.set0_10Out(stack, channel, value)
            value == megaind.get0_10Out(stack, channel)
            client.publish(DEVICE + '/megaind/' + str(stack) + '/response/0_10/' + str(channel), value, MQTT_QOS)
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
            client.publish(DEVICE + '/megaind/' + str(stack) + '/response/4_20/' + str(channel), value, MQTT_QOS)
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
            client.publish(DEVICE + '/megaind/' + str(stack) + '/response/pwm/' + str(channel), value, MQTT_QOS)
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
            client.publish(DEVICE + '/megaind/' + str(stack) + '/response/led/' + str(channel), value, MQTT_QOS)
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
            client.publish(DEVICE + '/megaind/' + str(stack) + '/response/opto_rce/' + str(channel), value, MQTT_QOS)
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
            client.publish(DEVICE + '/megaind/' + str(stack) + '/response/opto_fce/' + str(channel), value, MQTT_QOS)
        except:
            print("Can't set megaind stack: " + str(stack) + ", response: opto_fce, channel: " + str(channel) + " to value: " + str(value))
            raise("Can't set megaind stack: " + str(stack) + ", response: opto_fce, channel: " + str(channel) + " to value: " + str(value))
        else:
            cache[stack]["response"]["opto_fce"][channel - 1] = value
    elif output == "opto_rst" and bool(value):
        try:
            megaind.rstOptoCount(stack, channel)
            value = megaind.get0_10Out(stack, channel)
            client.publish(DEVICE + '/megaind/' + str(stack) + '/output/opto_rst/' + str(channel), 0, MQTT_QOS)
            client.publish(DEVICE + '/megaind/' + str(stack) + '/input/opto_count/' + str(channel), value, MQTT_QOS)
        except:
            print("Can't set megaind stack: " + str(stack) + ", output: opto_rst, channel: " + str(channel) + " to value: 0")
            raise("Can't set megaind stack: " + str(stack) + ", output: opto_rst, channel: " + str(channel) + " to value: 0")
        else:
            cache[stack]["input"]["opto_count"][channel - 1] = value
    else:
        print("Can't set megaind stack: " + str(stack) + ", topic: " + output + ", channel: " + str(channel) + " to value: 0")
        raise("Can't set megaind stack: " + str(stack) + ", topic: " + output + ", channel: " + str(channel) + " to value: 0")


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
        if megaind.wdtGetPeriod(stack) != WATCHDOG_TIMEOUT:
            megaind.wdtSetPeriod(stack, WATCHDOG_TIMEOUT)
        if megaind.wdtGetDefaultPeriod(stack) != WATCHDOG_BOOT:
            megaind.wdtSetDefaultPeriod(stack, WATCHDOG_BOOT)
        if megaind.wdtGetOffInterval(stack) != WATCHDOG_RESET:
            megaind.wdtSetOffInterval(stack, WATCHDOG_RESET)
    elif mode == 2:
        #megaind.wdtSetPeriod(stack, 65000)
        print("megabas.wdtSetPeriod")
    else:
        megaind.wdtReload(stack)
    return True


def get_megabas(stack, init):
    for channel in range(1,5):
        value = megabas.getUOut(stack, channel)
        if init or value != cache[stack]["response"]["0_10"][channel - 1]:
            client.publish(DEVICE + '/megabas/' + str(stack) + '/response/0_10/' + str(channel), value, MQTT_QOS)
            cache[stack]["response"]["0_10"][channel - 1] = value

        value = megabas.getOdPWM(stack, channel)
        if init or value != cache[stack]["response"]["triac"][channel - 1]:
            client.publish(DEVICE + '/megabas/' + str(stack) + '/response/pwm/' + str(channel), value, MQTT_QOS)
            cache[stack]["response"]["pwm"][channel - 1] = value

    for channel in range(1,8):
        value = round(megabas.getUIn(stack, channel),2)
        if init or value != cache[stack]["input"]["0_10"][channel - 1]:
            client.publish(DEVICE + '/megabas/' + str(stack) + '/input/0_10/' + str(channel), value, MQTT_QOS)
            cache[stack]["input"]["0_10"][channel - 1] = value

        value = round(megabas.getRIn1K(stack, channel),2)
        if init or value != cache[stack]["input"]["1k"][channel - 1]:
            client.publish(DEVICE + '/megabas/' + str(stack) + '/input/1k/' + str(channel), value, MQTT_QOS)
            cache[stack]["input"]["0_10"][channel - 1] = value

        value = round(megabas.getRIn10K(stack, channel), 2)
        if init or value != cache[stack]["input"]["10k"][channel - 1]:
            client.publish(DEVICE + '/megabas/' + str(stack) + '/input/10k/' + str(channel), value, MQTT_QOS)
            cache[stack]["input"]["10k"][channel - 1] = value

        value = megabas.getContactCh(stack, channel)
        if init or value != cache[stack]["input"]["cont"][channel - 1]:
            client.publish(DEVICE + '/megabas/' + str(stack) + '/input/cont/' + str(channel), value, MQTT_QOS)
            cache[stack]["input"]["cont"][channel - 1] = value

        value = megabas.getContactCounter(stack, channel)
        if init or value != cache[stack]["input"]["cont_count"][channel - 1]:
            client.publish(DEVICE + '/megabas/' + str(stack) + '/input/cont_count/' + str(channel), value, MQTT_QOS)
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
            client.publish(DEVICE + '/megabas/' + str(stack) + '/response/cont_rce/' + str(channel), value, MQTT_QOS)
            cache[stack]["response"]["cont_rce"][channel - 1] = raising
        if init or falling != cache[stack]["response"]["cont_fce"][channel - 1]:
            client.publish(DEVICE + '/megabas/' + str(stack) + '/response/cont_fce/' + str(channel), value, MQTT_QOS)
            cache[stack]["response"]["cont_fce"][channel - 1] = falling


def set_megabas(stack, output, channel, value):
    if output == "0_10":
        try:
            megabas.setUOut(stack, channel, value)
            value = megabas.getUOut(stack, channel)
            client.publish(DEVICE + '/megabas/' + str(stack) + '/input/0_10/' + str(channel), value, MQTT_QOS)
        except:
            print("Can't set megabas stack: " + str(stack) + ", input: 0_10, channel: " + str(channel) + " to value: " + str(value))
            raise("Can't set megabas stack: " + str(stack) + ", input: 0_10, channel: " + str(channel) + " to value: " + str(value))
        else:
            cache[stack]["response"]["0_10"][channel - 1] = value
    elif output == "triac":
        try:
            megabas.setTriac(stack, channel, value)
            value = megabas.getTriacs(stack)
            if value & (1 << channel):
                value = 1
            else:
                value = 0
            client.publish(DEVICE + '/megabas/' + str(stack) + '/input/triac/' + str(channel), value, MQTT_QOS)
        except:
            print("Can't set megabas stack: " + str(stack) + ", input: triac, channel: " + str(channel) + " to value: " + str(value))
            raise("Can't set megabas stack: " + str(stack) + ", input: triac, channel: " + str(channel) + " to value: " + str(value))
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
            client.publish(DEVICE + '/megabas/' + str(stack) + '/input/cont_rce/' + str(channel), value, MQTT_QOS)
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
            client.publish(DEVICE + '/megabas/' + str(stack) + '/input/cont_fce/' + str(channel), value, MQTT_QOS)
        except:
            print("Can't set megabas stack: " + str(stack) + ", input: cont_fce, channel: " + str(channel) + " to value: " + str(value))
            raise("Can't set megabas stack: " + str(stack) + ", input: cont_fce, channel: " + str(channel) + " to value: " + str(value))
        else:
            cache[stack]["response"]["cont_fce"][channel - 1] = value
    else:
        print("Can't set megabas stack: " + str(stack) + ", topic: " + output + ", channel: " + str(channel) + " to value: 0")
        raise("Can't set megabas stack: " + str(stack) + ", topic: " + output + ", channel: " + str(channel) + " to value: 0")


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
        if megabas.wdtGetPeriod(stack) != WATCHDOG_TIMEOUT:
            megabas.wdtSetPeriod(stack, WATCHDOG_TIMEOUT)
        if megabas.wdtGetDefaultPeriod(stack) != WATCHDOG_BOOT:
            megabas.wdtSetDefaultPeriod(stack, WATCHDOG_BOOT)
        if megabas.wdtGetOffInterval(stack) != WATCHDOG_RESET:
            megabas.wdtSetOffInterval(stack, WATCHDOG_RESET)
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
            client.publish(DEVICE + '/8relind/' + str(stack) + '/response/relay/' + str(channel), value, MQTT_QOS)
            cache[stack]["response"]["relay"][channel - 1] = value


def set_8relind(stack, output, channel, value):
    if output == "relay":
        try:
            value = int(value)
            lib8relind.set(stack, channel, value)
            value = lib8relind.get(stack, channel)
            client.publish(DEVICE + '/8relind/' + str(stack) + '/response/relay/' + str(channel), value, MQTT_QOS)
        except:
            print("Can't set 8relind stack: " + str(stack) + ", response: relay, channel: " + str(channel) + " to value: " + str(value))
            raise("Can't set 8relind stack: " + str(stack) + ", response: relay, channel: " + str(channel) + " to value: " + str(value))
        else:
            cache[stack]["response"]["relay"][channel - 1] = value
    else:
        print("Can't set 8relind stack: " + str(stack) + ", topic: " + output + ", channel: " + str(channel) + " to value: " + str(value))
        raise("Can't set 8relind stack: " + str(stack) + ", topic: " + output + ", channel: " + str(channel) + " to value: " + str(value))


def get_8inputs(stack, init):
    for channel in range(1,9):
        value = lib8inputs.get_opto(stack, channel)
        if init or value != cache[stack]["input"]["opto"][channel - 1]:
            client.publish(DEVICE + '/8inputs/' + str(stack) + '/input/opto/' + str(channel), value, MQTT_QOS)
            cache[stack]["input"]["opto"][channel - 1] = value


def get_rtd(stack, init):
    for channel in range(1,9):
        value = librtd.get(stack, channel)
        if init or value != cache[stack]["input"]["rtd"][channel - 1]:
            client.publish(DEVICE + '/rtd/' + str(stack) + '/input/rtd/' + str(channel), value, MQTT_QOS)
            cache[stack]["input"]["rtd"][channel - 1] = value


def cards_init():
    client.subscribe(DEVICE + '/tele/CMND/+', MQTT_QOS)
    cards_tele(1)
    for stack, card in enumerate(CARDS):
        if card == "megaind":
            client.subscribe(DEVICE + '/' + card + '/' + str(stack) + '/output/#', MQTT_QOS)
            get_megaind(stack, 1)
            watchdog_megaind(stack, 1)
        elif card == "megabas":
            client.subscribe(DEVICE + '/' + card + '/' + str(stack) + '/output/#', MQTT_QOS)
            get_megabas(stack, 1)
            watchdog_megabas(stack, 1)
        elif card == "8relind":
            client.subscribe(DEVICE + '/' + card + '/' + str(stack) + '/output/#', MQTT_QOS)
            get_8relind(stack, 1)
        elif card == "8inputs":
            get_8inputs(stack, 1)
        elif card == "rtd":
            get_rtd(stack, 1)
        else:
            print("Uknown card type " + card)
            raise NotImplementedError("Uknown card type " + card)


def cards_update(mode):
    if cards_tele(mode):
        mode = 1
    else:
        mode = 0
    for stack, card in enumerate(CARDS):
        if card == "megaind":
            get_megaind(stack, mode)
        elif card == "megabas":
            get_megabas(stack, mode)
        elif card == "8relind":
            get_8relind(stack, mode)
        elif card == "8inputs":
            get_8inputs(stack, mode)
        elif card == "rtd":
            get_rtd(stack, mode)
        else:
            print("Uknown card type " + card)
            raise NotImplementedError("Uknown card type " + card)


def cards_unsubscribe():
    client.unsubscribe(DEVICE + '/tele/CMND/+', MQTT_QOS)
    for stack, card in enumerate(CARDS):
        if card == "megaind":
            watchdog_megaind(stack, 2)
        elif card == "megabas":
            watchdog_megabas(stack, 2)
        client.unsubscribe(DEVICE + '/' + card + '/#', MQTT_QOS)


def cards_tele(mode):
    global last_tele
    now = time.time()
    if now - last_tele > 300 or mode == 1:
        get_time()
        for stack, card in enumerate(CARDS):
            if card == "megaind":
                if tele_megaind(stack):
                    break
            elif card == "megabas":
                if tele_megabas(stack):
                    break
        client.publish(DEVICE + '/tele/STATE', json.dumps(tele), MQTT_QOS)
        last_tele = now
        return True
    else:
        return False


def cards_watchdog():
    global last_watchdog
    now = time.time()
    if now - last_watchdog > WATCHDOG_TIMEOUT / 3:
        for stack, card in enumerate(CARDS):
            if card == "megaind":
                watchdog_megaind(stack, 0)
            elif card == "megabas":
                watchdog_megabas(stack, 0)
        last_watchdog = now
        return True
    else:
        return False


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
    match = re.match(r'^' + DEVICE + '\/tele/CMND\/(state)$', str(msg.topic))
    match1 = re.match(r'^' + DEVICE + '\/megaind\/([0-7])\/output\/(0_10|4_20|pwm|led|opto_rce|opto_fce|opto_rst)\/([1-4])$', str(msg.topic))
    match2 = re.match(r'^' + DEVICE + '\/megabas\/([0-7])\/output\/(0_10|triac|opto_rce|opto_fce)\/([1-4])$', str(msg.topic))
    match3 = re.match(r'^' + DEVICE + '\/8relind\/([0-7])\/output\/(relay)/([1-8])$', str(msg.topic))
    
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
while run:
    try:
        # Init counters
        last_tele = 0
        last_watchdog = 0
        # Create mqtt client
        client = mqtt.Client()
        client.connected_flag = 0
        client.reconnect_count = 0
        # Register LWT message
        client.will_set(DEVICE + '/tele/LWT', payload="Offline", qos=0, retain=True)
        # Register connect callback
        client.on_connect = on_connect
        # Register disconnect callback
        client.on_disconnect = on_disconnect
        # Registed publish message callback
        client.on_message = on_message
        # Set access token
        client.username_pw_set(MQTT_USER, MQTT_PASS)
        # Run receive thread
        client.loop_start()
        # Connect to broker
        client.connect(MQTT_SERVER, MQTT_PORT, MQTT_TIMEOUT)
        time.sleep(1)
        while not client.connected_flag:
            print("MQTT waiting to connect")
            client.reconnect_count += 1
            if client.reconnect_count > 10:
                print("MQTT restarting connection!")
                raise("MQTT restarting connection!")
            time.sleep(1)
        # Sent LWT update
        client.publish(DEVICE + '/tele/LWT',payload="Online", qos=0, retain=True)
        # init cards inputs and subscribe for output topics
        cards_init()
        # Run sending thread
        while True:
            if client.connected_flag:
                cards_update(0)
                cards_watchdog()
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
