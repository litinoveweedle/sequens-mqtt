# Sequent Microsystems cards to MTTQ bridge

This is simple Python3 service to act as bridge between RPi various automation cards from [Sequent Microsystems](https://sequentmicrosystems.com/) and MQTT server. It provides both read and write access to the cards. It supports stacking. To make it work you need to install appropriate Python3 libraries for your cards and configure stacking ID in the config part of the code.

Supported cards:
- megaind: [Industrial Automation Stackable Card for Raspberry Pi](https://sequentmicrosystems.com/collections/all-io-cards/products/industrial-raspberry-pi)
- megabas: [Building Automation Stackable Card for Raspberry Pi](https://sequentmicrosystems.com/products/building-automation-8-layer-stackable-hat-v4-for-raspberry-pi)
- rtd: [RTD Data Acquisition Stackable Card for Raspberry Pi](https://sequentmicrosystems.com/collections/all-io-cards/products/rtd-data-acquisition-card-for-rpi).
- 8relind: [8-RELAYS Stackable Card for Raspberry Pi](https://sequentmicrosystems.com/collections/industrial-automation/products/8-relays-stackable-card-for-raspberry-pi)
- 8inputs: [Eight Universal Inputs 8-Layer Stackable HAT for Raspberry Pi](https://sequentmicrosystems.com/collections/all-io-cards/products/eight-universal-inputs-br-8-layer-stackable-card-br-for-raspberry-pi-1)

[Sequent Microsystems Python libraries](https://github.com/SequentMicrosystems)

clone and install library (example is for 8inputs card):
>git clone https://github.com/SequentMicrosystems/8inputs-rpi.git
cd 8inputs-rpi/python/8inputs/
sudo python3 setup.py install

You will also need to install Paho mqtt library:
>sudo pip3 install paho-mqtt

