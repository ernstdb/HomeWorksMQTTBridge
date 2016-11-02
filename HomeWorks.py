# The MIT License (MIT)
# Copyright (c) 2016 Ernst den Broeder

# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
# documentation files (the "Software"), to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and
# to permit persons to whom the Software is furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all copies or substantial portions of
# the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
# THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
# TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE."

import serial
import re
import logging
import time
import threading
import socket
import queue
import daemon
import paho.mqtt.client as mqtt

K_TYPE_PATTERN = re.compile('K([1-8]{1})([0-9]{2})([0-9]{2})([+-]{1})\r\n')
L_TYPE_PATTERN = re.compile('L([1-8]{1})([0-9]{2})([0-9A-F]{4})\r\n')
D_TYPE_PATTERN = re.compile('D([1-8]{1})([0-9]{2})([1-8]{1})([0-9]{2,4})\r\n')
DEVICE_QUERY_REPLY = re.compile('!([1-8]{1})([0-9]{2})([0-9]{2})\r\n')
DEVICE_RANGE_QUERY_REPLY = re.compile('!([0-9]{6,})\r\n')
KEYPAD_QUERY_REPLY = re.compile('L([0-9A-F]{4}\r\n)')
VERSION_PATTERN = re.compile('[0-9]{1}-[0-9]{1}\r\n')

# outgoing MQTT message that we need to send
#   homeworks/panel/1/keypad/1/button/1/status  <1 or 0>
#   homeworks/panel/1/device/4/brightness/status  <value>
#   homeworks/panel/1/device/4/fade/status  <value>
#   homeworks/panel/1/device/4/status  <ON or OFF>


# incoming MQTT messages that we need to respond to
#   homeworks/panel/1/keypad/1/button/1  <ON or OFF>
#   homeworks/panel/1/device/4/switch  <ON or OFF>
#   homeworks/panel/1/device/4/brightness/set  <value>
#   homeworks/panel/1/device/4/fade/set  <value>
MQTT_SIMULATE_BUTTON = re.compile('homeworks/panel/([1-8]{1})/keypad/([0-9]+)/button/([0-9]+)')
MQTT_DEVICE_SWITCH = re.compile('homeworks/panel/([1-8]{1})/device/([0-9]+)/switch')
MQTT_DEVICE_BRIGHTNESS_SET = re.compile('homeworks/panel/([1-8]{1})/device/([0-9]+)/brightness/set')
MQTT_DEVICE_FADE_SET = re.compile('homeworks/panel/([1-8]{1})/device/([0-9]+)/fade/set')

MAX_RECONNECT_WAIT = 10


class HomeWorksError(Exception):
    pass


class HomeWorksVersion(HomeWorksError):
    pass


def _raise_on_error(result):
    if result != 0:
        raise HomeWorksError('Error talking to MQTT: {}'.format(result))


def _match_topic(subscription, topic):
    """Test if topic matches subscription."""
    if subscription.endswith('#'):
        return (subscription[:-2] == topic or
                topic.startswith(subscription[:-1]))

    sub_parts = subscription.split('/')
    topic_parts = topic.split('/')

    return (len(sub_parts) == len(topic_parts) and
            all(a == b for a, b in zip(sub_parts, topic_parts) if a != '+'))


class HomeWorks:
    def __init__(self, serial_port, broker, port=1883, client_id=None, keepalive=60, username=None, password=None,
                 certificate=None, client_key=None, client_cert=None, tls_insecure=None, protocol=mqtt.MQTTv311):

        self.topics = {}
        self.progress = {}

        if client_id is None:
            self._mqtt_client = mqtt.Client(protocol=protocol)
        else:
            self._mqtt_client = mqtt.Client(client_id, protocol=protocol)

        if username is not None:
            self._mqtt_client.username_pw_set(username, password)

        if certificate is not None:
            self._mqtt_client.tls_set(certificate, certfile=client_cert, keyfile=client_key)

        if tls_insecure is not None:
            self._mqtt_client.tls_insecure_set(tls_insecure)

        self._mqtt_client.on_subscribe = self._mqtt_on_subscribe
        self._mqtt_client.on_unsubscribe = self._mqtt_on_unsubscribe
        self._mqtt_client.on_connect = self._mqtt_on_connect
        self._mqtt_client.on_disconnect = self._mqtt_on_disconnect
        self._mqtt_client.on_message = self._mqtt_on_message

        self._mqtt_client.connect(broker, port, keepalive)

        # RS232 connection initialization to physical HomeWorks panel
        if isinstance(serial, serial.Serial):
            self.serial_channel = serial_port
        else:
            self.serial_channel = serial.Serial(serial_port)
        self.serial_channel.baudrate = 9600
        self.serial_channel.bytesize = serial.EIGHTBITS
        self.serial_channel.parity = serial.PARITY_NONE
        self.serial_channel.stopbits = serial.STOPBITS_ONE
        self.serial_channel.xonxoff = False
        self.serial_channel.timeout = 5

        self._serial_thread = None

        # Message queues
        self._serial_alive_q = queue.Queue()
        self._rx_real_time_msg_q = queue.Queue()
        self._rx_msg_q = queue.Queue()
        self._tx_msg_q = queue.Queue()
        self._keypad_rt_q = queue.Queue()
        self._led_rt_q = queue.Queue()
        self._device_rt_q = queue.Queue()
        self._keypad_awaiting_status_reply_q = queue.Queue()
        self._keypad_status_reply_q = queue.Queue()
        self._device_query_reply_q = queue.Queue()
        self._device_range_query_reply_q = queue.Queue()

        # HomeWorks inventory
        self.panel_inventory = {}
        for panel in range(1, 2):
            self.panel_inventory[panel] = {}
            self.panel_inventory[panel]["devices"] = {}
            for device in range(1, 49):
                self.panel_inventory[panel]["devices"][device] = Device(homeworks=self)
                self.panel_inventory[panel]["devices"][device].panel = panel
                self.panel_inventory[panel]["devices"][device].device = device
                logging.debug("created {}".format(self.panel_inventory[panel]["devices"][device]))
            self.panel_inventory[panel]["keypads"] = {}
            for keypad in range(1, 17):
                self.panel_inventory[panel]["keypads"][keypad] = Keypad(homeworks=self)
                self.panel_inventory[panel]["keypads"][keypad].panel = panel
                self.panel_inventory[panel]["keypads"][keypad].keypad = keypad
                logging.debug("created {}".format(self.panel_inventory[panel]["keypads"][keypad]))
            self._tx_msg_q.put('?{}01>{}48'.format(panel, panel))

        self._alive = False

    def mqtt_publish(self, topic, payload, qos, retain):
        self._mqtt_client.publish(topic, payload, qos, retain)

    def mqtt_start(self):
        self._mqtt_client.loop_start()

    def mqtt_stop(self):
        self._mqtt_client.disconnect()
        self._mqtt_client.loop_stop()

    def mqtt_subscribe(self, topic, qos):
        assert isinstance(topic, str)

        if topic in self.topics:
            return
        result, mid = self._mqtt_client.subscribe(topic, qos)
        _raise_on_error(result)
        self.progress[mid] = topic
        self.topics[topic] = None

    def mqtt_unsubscribe(self, topic):
        result, mid = self._mqtt_client.unsubscribe(topic)
        _raise_on_error(result)
        self.progress[mid] = topic

    def _mqtt_on_connect(self, client, userdata, _flags, result_code):
        if result_code != 0:
            logging.error('Unable to connect to the MQTT broker: %s', {
                1: 'Incorrect protocol version',
                2: 'Invalid client identifier',
                3: 'Server unavailable',
                4: 'Bad username or password',
                5: 'Not authorised'
            }.get(result_code, 'Unknown reason'))
            self._mqtt_client.disconnect()
            return

        old_topics = self.topics

        self.topics = {key: value for key, value in self.topics.items()
                       if value is None}

        for topic, qos in old_topics.items():
            # qos is None if we were in process of subscribing
            if qos is not None:
                self.mqtt_subscribe(topic, qos)

    def _mqtt_on_subscribe(self, client, userdata, mid, granted_qos):
        topic = self.progress.pop(mid, None)
        if topic is None:
            return
        self.topics[topic] = granted_qos[0]

    def _mqtt_on_unsubscribe(self, client, userdata, mid, granted_qos):
        topic = self.progress.pop(mid, None)
        if topic is None:
            return
        self.topics.pop(topic, None)

    def _mqtt_on_disconnect(self, client, userdata, result_code):
        self.progress = {}
        self.topics = {key: value for key, value in self.topics.items()
                       if value is not None}

        # Remove None values from topic list
        for key in list(self.topics):
            if self.topics[key] is None:
                self.topics.pop(key)

        # When disconnected because of calling disconnect()
        if result_code == 0:
            return

        tries = 0
        wait_time = 0

        while True:
            try:
                if self._mqtt_client.reconnect() == 0:
                    logging.info('Successfully reconnected to the MQTT server')
                    break
            except socket.error:
                pass

            wait_time = min(2**tries, MAX_RECONNECT_WAIT)
            logging.warning(
                'Disconnected from MQTT (%s). Trying to reconnect in %ss',
                result_code, wait_time)
            # It is ok to sleep here as we are in the MQTT thread.
            time.sleep(wait_time)
            tries += 1

    def _mqtt_on_message(self, client, userdata, msg):
        logging.debug("received mqtt message on %s: %s",
                      msg.topic, msg.payload.decode('utf-8'))
        if re.search(MQTT_SIMULATE_BUTTON, msg.topic):
            self._mqtt_on_message_keypad(client, userdata, msg)
        elif re.search(MQTT_DEVICE_FADE_SET, msg.topic):
            self._mqtt_on_message_device_fade_set(client, userdata, msg)
        elif re.search(MQTT_DEVICE_BRIGHTNESS_SET, msg.topic):
            self._mqtt_on_message_device_brightness_set(client, userdata, msg)
        elif re.search(MQTT_DEVICE_SWITCH, msg.topic):
            self._mqtt_on_message_device_switch(client, userdata, msg)

    def _mqtt_on_message_keypad(self, client, userdata, msg):
        m = re.search(MQTT_SIMULATE_BUTTON, msg.topic)
        # logging.debug("KEYPAD BUTTON topic: payload={}".format(msg.payload))
        panel = int(m.group(1))
        keypad = int(m.group(2))
        button = int(m.group(3))
        if 1 <= keypad <= 16 and 1 <= button <= 15:
            if msg.payload.decode() == "ON" and \
                    self.panel_inventory[panel]["keypads"][keypad].get_button(button) == "OFF":
                self.panel_inventory[panel]["keypads"][keypad].simulate_button_push(button)
            elif msg.payload.decode() == "OFF" and \
                    self.panel_inventory[panel]["keypads"][keypad].get_button(button) == "ON":
                self.panel_inventory[panel]["keypads"][keypad].simulate_button_push(button)
            elif msg.payload.decode() == "PUSH":
                self.panel_inventory[panel]["keypads"][keypad].simulate_button_push(button)

    def _mqtt_on_message_device_fade_set(self, client, userdata, msg):
        m = re.search(MQTT_DEVICE_FADE_SET, msg.topic)
        # logging.debug("DEVICE FADE: payload={}".format(msg.payload))
        panel = int(m.group(1))
        device = int(m.group(2))
        if 1 <= panel <= 8 and 1 <= device <= 48:
            try:
                fade_rate = int(msg.payload.decode())
                self.panel_inventory[panel]["devices"][device].fade_rate = fade_rate
                self.panel_inventory[panel]["devices"][device].publish_fade_rate()
            except Exception as err:
                logging.error("Exception {} on payload for {} = {}".format(err, msg.topic, msg.payload))

    def _mqtt_on_message_device_brightness_set(self, client, userdata, msg):
        m = re.search(MQTT_DEVICE_BRIGHTNESS_SET, msg.topic)
        # logging.debug("DEVICE BRIGHTNESS: payload={}".format(msg.payload))
        panel = int(m.group(1))
        device = int(m.group(2))
        if 1 <= panel <= 8 and 1 <= device <= 48:
            try:
                brightness = int(msg.payload.decode())
                self.panel_inventory[panel]["devices"][device].brightness = brightness
                self.panel_inventory[panel]["devices"][device].publish_brightness()
            except Exception as err:
                logging.error("Exception {} on payload for {} = {}".format(err, msg.topic, msg.payload))

    def _mqtt_on_message_device_switch(self, client, userdata, msg):
        m = re.search(MQTT_DEVICE_SWITCH, msg.topic)
        # logging.debug("DEVICE SWITCH: payload={}".format(msg.payload))
        panel = int(m.group(1))
        device = int(m.group(2))
        if 1 <= panel <= 8 and 1 <= device <= 48:
            try:
                if msg.payload.decode() == "ON":
                    self.panel_inventory[panel]["devices"][device].on()
                    self.panel_inventory[panel]["devices"][device].publish_status()
                elif msg.payload.decode() == "OFF":
                    self.panel_inventory[panel]["devices"][device].off()
                    self.panel_inventory[panel]["devices"][device].publish_status()
            except Exception as err:
                logging.error("Exception {} on payload for {} = {}".format(err, msg.topic, msg.payload))

    def serial_start(self):
        self._alive = True
        self._serial_thread = threading.Thread(target=self._serial_main)
        self._serial_thread.daemon = True
        self._serial_thread.start()
        logging.debug("serial_start() completed")

    def serial_stop(self):
        self._alive = False
        self._serial_alive_q.put(None)
        self._serial_thread.join()
        logging.debug("serial_stop() completed")

    def _serial_main(self):
        timestamp = time.time()
        while self._alive:
            time.sleep(0.01)
            if (time.time()-timestamp) > 60:
                logging.debug("_serial_main() is alive")
                timestamp = time.time()
            try:
                while self.serial_channel.in_waiting > 0:
                    try:
                        msg = self.serial_channel.readline().decode()
                        if re.search(K_TYPE_PATTERN, msg):
                            self._keypad_rt_q.put(msg)
                            # logging.debug("_serial_main() received (K type): {}".format(msg.strip()))
                        elif re.search(L_TYPE_PATTERN, msg):
                            self._led_rt_q.put(msg)
                            # logging.debug("_serial_main() received (L type): {}".format(msg.strip()))
                        elif re.search(D_TYPE_PATTERN, msg):
                            self._device_rt_q.put(msg)
                            # logging.debug("_serial_main() received (D type): {}".format(msg.strip()))
                        elif re.search(DEVICE_QUERY_REPLY, msg):
                            self._device_query_reply_q.put(msg)
                            # logging.debug("_serial_main() received (DQR type): {}".format(msg.strip()))
                        elif re.search(DEVICE_RANGE_QUERY_REPLY, msg):
                            self._device_range_query_reply_q.put(msg)
                            # logging.debug("_serial_main() received (DRQR type): {}".format(msg.strip()))
                        elif re.search(KEYPAD_QUERY_REPLY, msg):
                            m = re.search(KEYPAD_QUERY_REPLY, msg)
                            if m:
                                # logging.debug("_serial_main() received (KQR type): {}".format(msg.strip()))
                                self._keypad_status_reply_q.put(int(m.group(1), 16))
                        elif re.search(VERSION_PATTERN, msg):
                            self._rx_msg_q.put(msg)
                        else:
                            logging.debug("_serial_main() received (and ignored): {}".format(msg.strip()))
                    except UnicodeDecodeError:
                        pass

                if not self._tx_msg_q.empty():
                    msg = self._tx_msg_q.get()
                    self.serial_channel.write('{}\r'.format(msg).encode())
                    logging.debug("_serial_main() transmitted: {}".format(msg.encode()))
            except Exception as err:
                logging.error("_serial_main() died... {}".format(err))

    def hw_send(self, s):
        self._tx_msg_q.put(s)

    def hw_send_receive(self, s, timeout=5):
        self.hw_send(s)
        try:
            return self._rx_msg_q.get(block=True, timeout=timeout)
        except QueueEmpty:
            return ""

    def hw_version_check(self):
        response = self.hw_send_receive('V', timeout=30)
        if not re.search('[0-9]{1}-[0-9]{1}', response.strip()):
            logging.error("timeout trying to check HomeWorks version")
            raise HomeWorksError()

        version = [int(v) for v in response.strip().split('-')]
        if version < [7, 0]:
            logging.error("HomeWorks software version needs to be 7-0 or higher")
            raise HomeWorksVersion("HomeWorks software version needs to be 7-0 or higher")
        else:
            logging.debug("HomeWorks software version: {}".format(version))
            logging.debug("enable real time messages")
            self._tx_msg_q.put('M+++')

    def hw_main(self):
        self.serial_start()
        self.hw_version_check()
        self.mqtt_start()
        self.mqtt_subscribe("homeworks/panel/+/keypad/+/button/+", qos=0)
        self.mqtt_subscribe("homeworks/panel/+/device/+/switch", qos=0)
        self.mqtt_subscribe("homeworks/panel/+/device/+/brightness/set", qos=0)
        self.mqtt_subscribe("homeworks/panel/+/device/+/fade/set", qos=0)

        # main loop that reacts to events
        next_device = self._hw_next_device()
        next_keypad = self._hw_next_keypad()
        toggle = True
        timestamp = time.time()
        count = 0
        while self._alive:
            time.sleep(0.01)
            if (time.time() - timestamp) > 5:
                timestamp += 5
                count += 1
                if count > 12:
                    count = 0
                    logging.debug("hw_main() is alive")
                if toggle:
                    toggle = False
                    try:
                        panel, device = next(next_device)
                        x = self.panel_inventory[panel]["devices"][device]
                        if x.exists is False:
                            del self.panel_inventory[panel]["devices"][device]
                        else:
                            x.timestamp = time.time()
                            x.poll()
                    except KeyError:
                        logging.debug("KeyError in device poll loop")
                else:
                    toggle = True
                    try:
                        panel, keypad = next(next_keypad)
                        x = self.panel_inventory[panel]["keypads"][keypad]
                        x.timestamp = time.time()
                        x.poll()
                    except KeyError:
                        logging.debug("KeyError in keypad poll loop")

            if self._keypad_rt_q.qsize() > 0:
                msg = KMessage(self._keypad_rt_q.get())
                logging.debug(msg)
                # don't do anything else, a LED update will be used to change Keypad obj state

            if self._led_rt_q.qsize() > 0:
                msg = LMessage(self._led_rt_q.get())
                logging.debug(msg)
                self.panel_inventory[msg.panel]["keypads"][msg.keypad].decode_leds_from_int(msg.leds)
                self.panel_inventory[msg.panel]["keypads"][msg.keypad].publish_keypad_status()

            if self._device_rt_q.qsize() > 0:
                msg = DMessage(self._device_rt_q.get())
                logging.debug(msg)
                self._update_device(msg)

            if self._device_query_reply_q.qsize() > 0:
                msg = DeviceQueryReply(self._device_query_reply_q.get())
                logging.debug(msg)
                self._update_device(msg)

            if self._device_range_query_reply_q.qsize() > 0:
                msg = DeviceRangeQueryReply(self._device_range_query_reply_q.get())
                logging.debug(msg)
                self._update_device(msg)

            if self._keypad_status_reply_q.qsize() > 0 and self._keypad_awaiting_status_reply_q.qsize() > 0:
                    keypad = self._keypad_awaiting_status_reply_q.get()
                    value = self._keypad_status_reply_q.get()
                    keypad.decode_leds_from_int(value)
                    logging.debug(keypad)
                    keypad.publish_keypad_status()

        self.mqtt_stop()

    def _update_device(self, msg):
        if isinstance(msg, DMessage) or isinstance(msg, DeviceQueryReply):
            panel = msg.panel
            device_number = msg.device
            if device_number not in self.panel_inventory[panel]["devices"].keys():
                d = Device(homeworks=self)
                d.panel = panel
                d.device = device_number
                logging.debug("created {}".format(d))
                self.panel_inventory[panel]["devices"][device_number] = d
            # update device database
            self.panel_inventory[panel]["devices"][device_number].update_brightness_from_real_time_message(
                msg.brightness_raw
            )
            self.panel_inventory[panel]["devices"][device_number].timestamp = time.time()
            # publish MQTT update
            self.panel_inventory[panel]["devices"][device_number].publish_status()
            self.panel_inventory[panel]["devices"][device_number].publish_brightness()
        elif isinstance(msg, DeviceRangeQueryReply):
            m = re.search(DEVICE_RANGE_QUERY_REPLY, msg.data)
            chars = m.group(1)
            while len(chars) > 0:
                m = re.search('([1-8]{1})([0-9]{2})', chars[:3])
                chars = chars[3:]
                panel = int(m.group(1))
                device_number = int(m.group(2))
                if 48 < device_number <= 64:
                    logging.debug("skipping panel {} device {:02d}".format(panel, device_number))
                    chars = chars[2:]  # relays and Grafik eye devices
                    continue
                elif device_number > 64:
                    logging.debug("skipping panel {} device {:02d}".format(panel, device_number))
                    chars = chars[4:]  # Serena shades return 4 bytes
                    continue
                value = int(chars[:2])
                chars = chars[2:]
                if device_number not in self.panel_inventory[panel]["devices"].keys():
                    d = Device(homeworks=self)
                    d.panel = panel
                    d.device = device_number
                    logging.debug("created {}".format(d))
                    self.panel_inventory[panel]["devices"][device_number] = d
                # update device database
                self.panel_inventory[panel]["devices"][device_number].update_brightness_from_real_time_message(value)
                self.panel_inventory[panel]["devices"][device_number].timestamp = time.time()
                # publish MQTT update
                self.panel_inventory[panel]["devices"][device_number].publish_status()
                self.panel_inventory[panel]["devices"][device_number].publish_brightness()

    def update_keypad(self, msg):
        if isinstance(msg, KMessage):
            panel = msg.panel
            keypad_number = msg.keypad
            if keypad_number not in self.panel_inventory[panel]["keypads"].keys():
                k = Keypad(homeworks=self)
                k.panel = panel
                k.keypad = keypad_number
                logging.debug("created {}".format(k))
                self.panel_inventory[panel]["keypads"] = k
            self.panel_inventory[panel]["keypads"].set_button(msg.button, msg.state)
            self.panel_inventory[panel]["keypads"].timestamp = time.time()
            # publish MQTT update
            self.panel_inventory[panel]["keypads"].publish_button_status(msg.button)

    def _hw_next_device(self):
        while True:
            for panel in self.panel_inventory.keys():
                for device in self.panel_inventory[panel]["devices"].copy():
                    yield (panel, device)

    def _hw_next_keypad(self):
        while True:
            for panel in self.panel_inventory.keys():
                for keypad in self.panel_inventory[panel]["keypads"].copy():
                    yield (panel, keypad)


class Keypad:
    def __init__(self, homeworks: HomeWorks):
        self._homeworks = homeworks
        self.exists = False
        self._panel = 0
        self._keypad = 0
        self._timestamp = time.time()
        self._button_state = []
        for i in range(15):
            self._button_state.append("OFF")
        # logging.debug("created {}".format(self))

    def __repr__(self):
        state = ''
        for button in reversed(range(1, 16)):
            if self.get_button(button) == "ON":
                state += '1'
            else:
                state += '0'
        return 'Keypad object: panel {} keypad {} state (LEDS) {}'.format(
            self.panel,
            self.keypad,
            state
        )

    def decode_leds_from_int(self, value):
        for button in range(1, 16):
            if value & 1:
                self.set_button(button, "ON")
            else:
                self.set_button(button, "OFF")
            value >>= 1

    @property
    def timestamp(self):
        return self._timestamp

    @timestamp.setter
    def timestamp(self, value):
        self._timestamp = value

    @property
    def panel(self):
        return self._panel

    @panel.setter
    def panel(self, value):
        if 1 <= value <= 8:
            self._panel = value

    @property
    def keypad(self):
        return self._keypad

    @keypad.setter
    def keypad(self, value):
        if 1 <= value <= 16:
            self._keypad = value

    def get_button(self, number):
        return self._button_state[number - 1]

    def set_button(self, number, state):
        if state == 1 or state == "ON" or state is True:
            self._button_state[number - 1] = "ON"
        elif state == 0 or state == "OFF" or state is False:
            self._button_state[number - 1] = "OFF"

    def simulate_button_push(self, button):
        self._homeworks.hw_send('B{:d}{:02d}{:02d}'.format(self.panel, self.keypad, button))

    def publish_keypad_status(self):
        for i in range(1, 16):
            self.publish_button_status(i)

    def publish_button_status(self, button):
        topic = "homeworks/panel/{}/keypad/{}/button/{}/status".format(
            self.panel,
            self.keypad,
            button
        )
        self._homeworks.mqtt_publish(topic=topic,
                                     payload=self._button_state[button-1],
                                     qos=0,
                                     retain=True)

    def poll(self):
        # logging.debug("sending LED query")
        # self._homeworks.keypads_waiting_for_query_reply.put(self)
        self._homeworks._keypad_awaiting_status_reply_q.put(self)
        self._homeworks.hw_send('I{}{:02d}'.format(self.panel, self.keypad))


class Device:
    def __init__(self, homeworks: HomeWorks):
        self._homeworks = homeworks
        self._exists = False
        self._panel = 0
        self._bus = 0
        self._device = 0
        self._status = "OFF"  # ON / OFF -- control panel only understands brightness so behaviour is modeled
        self._brightness = 0
        self._brightness_recall = 31
        self._fade_rate = 0
        self._timestamp = time.time()
        # logging.debug("created {}".format(self))

    def __repr__(self):
        return 'Device object: panel {} bus {:02d} device {} state {} brightness {} fade_rate {}'.format(
            self._panel,
            self._bus,
            self._device,
            self._status,
            self._brightness,
            self._fade_rate
        )

    @property
    def exists(self):
        return self._exists

    @exists.setter
    def exists(self, value):
        self._exists = value

    @property
    def timestamp(self):
        return self._timestamp

    @timestamp.setter
    def timestamp(self, value):
        self._timestamp = value

    @property
    def seconds_since_last_update(self):
        return time.time() - self.timestamp

    @property
    def bus(self):
        return self._bus

    @bus.setter
    def bus(self, value):
        self._bus = value

    @property
    def device_raw(self):
        return self._device

    @device_raw.setter
    def device_raw(self, value):
        self._device = value

    @property
    def device(self):
        return (self._bus - 1) * 4 + self._device

    @device.setter
    def device(self, value):
        self._device = value % 4
        if self._device == 0:
            self._bus = int(value / 4)
            self._device = 4
        else:
            self._bus = int(value / 4) + 1

    @property
    def panel(self):
        return self._panel

    @panel.setter
    def panel(self, value):
        self._panel = value

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, value):
        if value == 1 or value is True or value == "ON":
            self._status = "ON"
        else:
            self._status = "OFF"

    @property
    def brightness_raw(self):
        return self._brightness

    @brightness_raw.setter
    def brightness_raw(self, value):
        logging.info("Device({}, {}).brightness_raw called with value {}".format(self.panel, self.device, value))
        self._set_brightness(value, update_device=True)

    def update_brightness_from_real_time_message(self, value):
        logging.info("Device({}, {}).update_from_RT_message called with value {}".format(self.panel, self.device, value))
        if 0 < value <= 31:
            self.status = "ON"
        self._set_brightness(value, update_device=False)

    def _set_brightness(self, value, update_device):
        # normal dimmer range is 0 - 31
        # normal switch range is 0 or 31
        # no device present returns a value of 99 by Homeworks panel
        if value == 99:
            self._exists = False
        else:
            self._exists = True

            # retain the brightness value for fake ON / OFF state
            if value > 0:
                self._brightness_recall = value
            self._brightness = value

            # special case, if brightness set to 0 the state is OFF
            if value == 0:
                self.status = "OFF"
                if update_device:
                    self.command()

            # if the device is already ON, update it
            if self.status == "ON":
                if update_device:
                    self.command()

    @property
    def brightness(self):
        return round(self.brightness_raw * 8.2258)

    @brightness.setter
    def brightness(self, value):
        # logging.info("Device().brightness called with value {}".format(value))
        self.brightness_raw = int(value / 8.2258)

    @property
    def fade_rate_raw(self):
        return self._fade_rate

    @fade_rate_raw.setter
    def fade_rate_raw(self, value):
        self._fade_rate = value

        # if the device is already ON, update it (there could be a very slow fade in progress that needs to update)
        if self.status == "ON":
            self.command()

    @property
    def fade_rate(self):
        thresholds = [0, 2, 4, 8, 15, 30, 60, 120, 480, 900, 1800, 3600]
        return thresholds[self.fade_rate_raw]

    @fade_rate.setter
    def fade_rate(self, value):
        # code  rate     code  rate
        #  00   0 sec     06   1 min
        #  01   2 sec     07   2 min
        #  02   4 sec     08   8 min
        #  03   8 sec     09   15 min
        #  04   15 sec    10   30 min
        #  05   30 sec    11   60 min
        thresholds = [0, 2, 4, 8, 15, 30, 60, 120, 480, 900, 1800, 3600]
        code = -1
        for x in thresholds:
            code += 1
            if value <= x:
                break
        self.fade_rate_raw = code

    def publish_status(self):
        if self.exists:
            self._homeworks.mqtt_publish(topic="homeworks/panel/{}/device/{}/status".format(self.panel, self.device),
                                         payload=self.status,
                                         qos=0,
                                         retain=True)

    def publish_brightness(self):
        if self.exists:
            self._homeworks.mqtt_publish(topic="homeworks/panel/{}/device/{}/brightness/status".format(self.panel, self.device),
                                         payload=self.brightness,
                                         qos=0,
                                         retain=True)

    def publish_fade_rate(self):
        if self.exists:
            self._homeworks.mqtt_publish(topic="homeworks/panel/{}/device/{}/fade/status".format(self.panel, self.device),
                                         payload=self.fade_rate,
                                         qos=0,
                                         retain=True)

    def poll(self):
        self._homeworks.hw_send('?{}{:02d}'.format(self.panel, self.device))

    def on(self):
        self._status = "ON"
        self._brightness = self._brightness_recall
        self.command()

    def off(self):
        self._status = "OFF"
        if self._brightness > 0:
            self._brightness_recall = self._brightness
        self._brightness = 0
        self.command()

    def command(self, fade_rate=None, intensity=None):
        # tell Homeworks panel to update device
        if fade_rate is None:
            fade_rate = self.fade_rate
        if intensity is None:
            intensity = self.brightness_raw
        self._homeworks.hw_send('N00{:02d}{:02d}{}{:02d}'.format(
            fade_rate,
            intensity,
            self.panel,
            self.device
        ))


class KMessage:
    def __init__(self, data):
        self._panel = 0
        self._keypad = 0
        self._button = 0
        self._state = None

        m = re.search(K_TYPE_PATTERN, data)
        if m:
            self._panel = int(m.group(1))
            self._keypad = int(m.group(2))
            self._button = int(m.group(3))
            if m.group(4) == "+":
                self._state = True
            else:
                self._state = False

    def __repr__(self):
        s = 'real time keypad message: panel {} keypad {:02d} _button {:02d} '.format(
            self._panel,
            self._keypad,
            self._button
        )
        if self._state is True:
            s += 'pressed'
        elif self._state is False:
            s += 'released'
        else:
            s += 'unknown'

        return s

    @property
    def panel(self):
        return self._panel

    @property
    def keypad(self):
        return self._keypad

    @property
    def button(self):
        return self._button

    @property
    def state(self):
        return self._state


class LMessage:
    def __init__(self, data):
        self._panel = 0
        self._keypad = 0
        self._led_state = 0

        m = re.search(L_TYPE_PATTERN, data)
        if m:
            self._panel = int(m.group(1))
            self._keypad = int(m.group(2))
            self._led_state = int(m.group(3), 16)

    def __repr__(self):
        return 'real time LED message: panel {} keypad {:02d} led {:015b}'.format(
            self._panel,
            self._keypad,
            self._led_state
        )

    @property
    def panel(self):
        return self._panel

    @property
    def keypad(self):
        return self._keypad

    @property
    def leds(self):
        return self._led_state


class DMessage:
    def __init__(self, data):
        self._panel = 0
        self._bus = 0
        self._device = 0
        self._brightness = 0

        m = re.search(D_TYPE_PATTERN, data)
        if m:
            self._panel = int(m.group(1))
            self._bus = int(m.group(2))
            self._device = int(m.group(3))
            self._brightness = int(m.group(4))

    def __repr__(self):
        return 'real time device message: panel {} device {} brightness {}'.format(
            self._panel,
            self.device,
            self._brightness
        )

    @property
    def device(self):
        return (self._bus - 1) * 4 + self._device

    @property
    def panel(self):
        return self._panel

    @property
    def brightness_raw(self):
        return self._brightness


class DeviceQueryReply:
    def __init__(self, data):
        self._panel = 0
        self._device = 0
        self._brightness = 0

        m = re.search(DEVICE_QUERY_REPLY, data)
        if m:
            self._panel = int(m.group(1))
            self._device = int(m.group(2))
            self._brightness = int(m.group(3))

    def __repr__(self):
        return 'device query reply message: panel {} device {} brightness {}'.format(
            self._panel,
            self._device,
            self._brightness
        )

    @property
    def device(self):
        return self._device

    @property
    def panel(self):
        return self._panel

    @property
    def brightness_raw(self):
        return self._brightness


class DeviceRangeQueryReply:
    def __init__(self, data):
        self.data = data

    def __repr__(self):
        return 'device range query reply message: {}'.format(self.data.strip())


with daemon.DaemonContext():
    # logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)
    h = HomeWorks(serial_port='/dev/ttyUSB0', broker='localhost', username='pi', password='homeworks')
    # h.hw_version_check()
    h.hw_main()
    h.serial_stop()
