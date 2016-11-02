# HomeWorksMQTTBridge
A Python based RS232 to MQTT bridge for Lutron HomeWorks (the original version from mid-1990's)

Quick and dirty instructions:
- copy HomeWorks.py to somewhere (e.g. /opt/homeworks/)
- install python3
- ensure these python3 packages are available:
  - pyserial
  - python-daemon
  - paho-mqtt

## serial port enumeration and MQTT settings
Find the following line in HomeWorks.py and edit to suite.
> h = HomeWorks(serial_port='/dev/ttyUSB0', broker='localhost', username='pi', password='homeworks')<br/>

## have HomeWorks.py autostart at boot (e.g. RaspberryPi/Debian)
Copy homeworks.service to /etc/systemd/system<br/>
- sudo systemctl daemon-reload
- sudo systemctl enable homeworks.service
