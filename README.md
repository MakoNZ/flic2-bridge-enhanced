# Flic 2 MQTT Bridge v2.1.0 for XIAO ESP32-C6

An ESP32-C6 bridge for Flic 2 buttons using the official Shortcut Labs Flic 2 C protocol module.

The bridge handles BLE communication, pairing persistence, MQTT publishing, remote diagnostics, and ArduinoOTA updates. It is designed for the Seeed Studio XIAO ESP32-C6.

## Main design

* Up to 16 Flic pairing records are persisted in NVS.
* Up to 3 NimBLE clients are created and pooled/reused between buttons.
* NimBLE clients are never deleted at runtime on ESP32-C6.
* `esp_timer_get_time()` supplies the monotonic Flic steady clock.
* NVS writes are coalesced and deferred instead of writing flash from BLE callback paths.
* MQTT button events, status updates, and remote logs are queued and sent from the main loop.
* ArduinoOTA provides wireless firmware updates.
* A sketch-local dual-OTA partition table provides 1,966,080 bytes per application slot on 4 MiB flash.
* Credentials are stored in git-ignored `secrets.h`.
* A short BLE startup grace period avoids unnecessary connection failures immediately after boot.

## MQTT identity

A Flic's serial number is used as its stable MQTT identity. Internal database slot numbers are deliberately **not** part of the MQTT topic hierarchy.

For a button with serial:

```text
<FLIC_SERIAL>
```

the bridge publishes:

```text
flic/<FLIC_SERIAL>/status
flic/<FLIC_SERIAL>/event
```

`status` is retained. `event` is not retained.

The internal database slot remains present in JSON payloads for diagnostics, but MQTT consumers should use the serial-number topic as the stable device identity.

### Example status payload

```json
{
  "slot": 1,
  "serial": "<FLIC_SERIAL>",
  "name": "",
  "firmware": 11,
  "battery_mv": 3164,
  "battery_pct": 100,
  "paired": true,
  "connected": true,
  "state": "SESSION_ACTIVE",
  "connection_attempts": 0
}
```

### Example event payload

```json
{
  "slot": 1,
  "serial": "<FLIC_SERIAL>",
  "event": "SINGLE_CLICK",
  "class": 2,
  "count": 1807,
  "queued": false
}
```

Battery percentage is an estimate derived from CR2032 voltage. `battery_mv` is retained in the status JSON and is the more useful diagnostic value.

## Bridge topics

The bridge also publishes global operational topics:

```text
flic/bridge/status
flic/bridge/info
flic/bridge/metrics
flic/bridge/log
```

### `flic/bridge/status`

Retained bridge availability.

```text
online
```

The MQTT Last Will publishes:

```text
offline
```

if the bridge disconnects unexpectedly.

### `flic/bridge/info`

Retained firmware and device information.

### `flic/bridge/metrics`

Retained health metrics published periodically, including uptime, heap usage, Wi-Fi RSSI, MQTT state, known Flics, connected Flics, queue usage, and firmware storage information.

### `flic/bridge/log`

Non-retained JSON operational logs using INFO, WARN, and ERROR levels.

## Gladys Assistant JSON extraction

Gladys Assistant can map multiple features from the same retained status JSON topic.

For example:

```text
flic/<FLIC_SERIAL>/status
```

can be used with JSON property paths such as:

```text
battery_pct
battery_mv
connected
state
firmware
connection_attempts
```

Button events are available from:

```text
flic/<FLIC_SERIAL>/event
```

using the JSON property:

```text
event
```

This avoids publishing a separate MQTT scalar topic for every feature.

A useful Gladys external-ID convention is:

```text
mqtt:flic:<FLIC_SERIAL>
```

with feature IDs such as:

```text
mqtt:flic:<FLIC_SERIAL>:event
mqtt:flic:<FLIC_SERIAL>:battery_pct
mqtt:flic:<FLIC_SERIAL>:battery_mv
mqtt:flic:<FLIC_SERIAL>:connected
```

## Required Arduino libraries

* NimBLE-Arduino 2.x
* PubSubClient
* ESP32 Arduino core with XIAO ESP32-C6 support

`WiFi`, `Preferences`, `ArduinoOTA`, and `esp_timer` are provided by the ESP32 Arduino core.

## Configuration

Copy:

```text
secrets.example.h
```

to:

```text
secrets.h
```

and configure the local Wi-Fi, MQTT, and OTA credentials.

`secrets.h` is excluded from Git and should never be committed.

## Partition scheme and first wired flash

The included `partitions.csv` defines two OTA application slots of 1,966,080 bytes each.

Arduino's size checker still uses the maximum application size associated with the **Tools → Partition Scheme** menu selection, even when the sketch-local `partitions.csv` supplies the actual flash layout.

For this project, set:

```text
Huge APP (3MB No OTA / 1MB SPIFFS)
```

The menu name is misleading in this case. The local `partitions.csv` supplies the actual dual-OTA layout. Selecting Huge APP only prevents Arduino's pre-upload size check from rejecting the firmware against the smaller default application limit.

The first flash after changing the partition layout must be performed over USB.

Subsequent firmware updates can use ArduinoOTA.

The NVS location, namespace, database structure, and database format remain compatible with earlier v2.x builds.

## OTA

The bridge advertises ArduinoOTA using:

```text
flic2-bridge.local:3232
```

The OTA password is stored in local `secrets.h`.

### Linux firewall considerations

ArduinoOTA sends the initial request to the ESP32, after which the ESP32 opens a TCP connection back to the uploader.

Using a fixed uploader callback port makes firewall configuration easier.

For example, to use TCP port `3233` with firewalld:

```bash
sudo firewall-cmd --zone=<zone> --add-port=3233/tcp --permanent
sudo firewall-cmd --reload
```

If the Arduino IDE or ESP32 core fails to substitute the discovered OTA port and passes `{upload.port.properties.port}` literally, a `platform.local.txt` override can force known OTA ports:

```text
tools.esp_ota.upload.pattern={cmd} -i {upload.port.address} -p 3232 -P 3233 "--auth={upload.field.password}" -f "{build.path}/{build.project_name}.bin"
```

Restart the Arduino IDE after creating or modifying `platform.local.txt`.

Because `platform.local.txt` resides inside the installed ESP32 board package directory, it may need to be recreated after upgrading the ESP32 Arduino core.

## Cleaning obsolete slot-based retained topics

Earlier builds may have published retained slot-oriented topics such as:

```text
flic/1/status
flic/1/battery_mv
flic/1/battery_pct
```

Current builds use Flic serial numbers instead.

To remove obsolete retained values from an MQTT broker:

```bash
for slot in {1..16}; do
  for suffix in status battery_mv battery_pct; do
    mosquitto_pub \
      -h <MQTT_HOST> \
      -r \
      -n \
      -t "flic/$slot/$suffix"
  done
done
```

Add MQTT authentication options if required by your broker.

Old `event` topics do not need cleanup because event messages are not retained.

## Monitoring

To monitor all Flic bridge MQTT traffic:

```bash
mosquitto_sub \
  -h <MQTT_HOST> \
  -v \
  -t 'flic/#'
```

Add MQTT authentication options if required.

Normal builds compile out packet dumps and verbose serial diagnostics. Operational logging remains available through:

```text
flic/bridge/log
```

Pairing material and configured credentials are never intentionally logged.

## Security

Never commit `secrets.h`.

The repository contains `secrets.example.h` as a configuration template. Wi-Fi, MQTT, and OTA credentials should only be stored in the local ignored `secrets.h` file.

If credentials were previously hard-coded into a sketch that was published or shared, rotate them.


## License

This project is licensed under the GNU General Public License, version 3
or (at your option) any later version (`GPL-3.0-or-later`).

The bundled Flic 2 protocol module files:

- `flic2.c`
- `flic2.h`
- `flic2_crypto.c`
- `flic2_crypto.h`
- `flic2_packets.h`

are Copyright (C) 2022 Shortcut Labs AB and are distributed under the
GNU General Public License version 3 or later.

See `THIRD_PARTY_NOTICES.md` for additional attribution.
