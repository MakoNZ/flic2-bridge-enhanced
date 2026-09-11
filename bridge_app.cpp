/*
 * Flic 2 BLE + MQTT bridge for Seeed Studio XIAO ESP32-C6
 *
 * Copyright (C) 2026 MakoNZ
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <Arduino.h>
#include <ArduinoOTA.h>
#include <NimBLEDevice.h>
#include <Preferences.h>
#include <PubSubClient.h>
#include <WiFi.h>

#include <esp_random.h>
#include <esp_timer.h>
#include <stdarg.h>
#include <string.h>

#include "bridge_app.h"
#include "secrets.h"

extern "C" {
#include "flic2.h"
}

// ============================================================================
// BUILD / CONFIGURATION
// ============================================================================

static const char *BRIDGE_VERSION = "2.1.0";
static const char *BRIDGE_NAME = "Flic2-Bridge";
static const char *OTA_HOSTNAME = "flic2-bridge";

static const char *MQTT_ROOT = "flic";
static const char *MQTT_CLIENT_ID = "flic2-bridge";
static const uint16_t MQTT_PORT = 1883;

static const uint8_t MAX_FLIC_BUTTONS = 16;
static const uint8_t MAX_SIMULTANEOUS_FLICS = 3;
static const uint16_t FLIC_ATT_MTU_MAX = 130;

static const uint32_t RECONNECT_DELAY_MS = 3000;
static const uint32_t WIFI_RECONNECT_DELAY_MS = 5000;
static const uint32_t MQTT_RECONNECT_DELAY_MS = 5000;
static const uint32_t BLE_STARTUP_GRACE_MS = 3000;
static const uint32_t SCAN_TIME_MS = 5000;
static const uint32_t SCAN_RETRY_DELAY_MS = 10000;
static const uint32_t FLIC_BACKOFF_MIN_MS = 3000;
static const uint32_t FLIC_BACKOFF_MAX_MS = 60000;
static const uint32_t DB_PERSIST_DELAY_MS = 500;
static const uint32_t BRIDGE_METRICS_INTERVAL_MS = 30000;

static const uint32_t NVS_FORMAT_VERSION = 1;
static const char *NVS_NAMESPACE = "flic2";
static const char *NVS_DATABASE_KEY = "database";
static const char *NVS_VERSION_KEY = "version";

// Set true for ONE wired firmware boot to erase all stored Flic pairings.
static const bool FACTORY_RESET_FLICS_ON_BOOT = false;

#ifndef FLIC_VERBOSE_SERIAL
#define FLIC_VERBOSE_SERIAL 0
#endif

#ifndef FLIC_REMOTE_LOGGING
#define FLIC_REMOTE_LOGGING 1
#endif

#if FLIC_VERBOSE_SERIAL
#define VLOGF(...) Serial.printf(__VA_ARGS__)
#define VLOGLN(x) Serial.println(x)
#else
#define VLOGF(...) do { } while (0)
#define VLOGLN(x) do { } while (0)
#endif

// ============================================================================
// UUIDs
// ============================================================================

static const char *FLIC_SERVICE_UUID =
    "00420000-8F59-4420-870D-84F3B617E493";
static const char *FLIC_WRITE_UUID =
    "00420001-8F59-4420-870D-84F3B617E493";
static const char *FLIC_NOTIFY_UUID =
    "00420002-8F59-4420-870D-84F3B617E493";

// ============================================================================
// NETWORK
// ============================================================================

static WiFiClient g_wifiClient;
static PubSubClient g_mqtt(g_wifiClient);
static Preferences g_preferences;

static bool g_wifiConnected = false;
static bool g_mqttConnected = false;
static bool g_otaStarted = false;
static volatile bool g_otaInProgress = false;
static uint32_t g_lastWiFiAttemptMs = 0;
static uint32_t g_lastMqttAttemptMs = 0;
static uint32_t g_lastMetricsPublishMs = 0;
static uint32_t g_lastScanMs = 0;
static uint32_t g_bleConnectAfterMs = 0;

// ============================================================================
// DATABASE
// ============================================================================

struct FlicButtonRecord
{
    bool used;
    Flic2DbData data;
    uint8_t address[6]; // Flic/NimBLE wire order (LSB first).
};

static FlicButtonRecord g_buttonDb[MAX_FLIC_BUTTONS];
static portMUX_TYPE g_databaseMux = portMUX_INITIALIZER_UNLOCKED;
static volatile bool g_databaseDirty = false;
static uint32_t g_databaseGeneration = 0;
static volatile uint32_t g_databaseDirtySinceMs = 0;

// ============================================================================
// CONNECTION STATE
// ============================================================================

enum FlicConnectionState : uint8_t
{
    FLIC_CONN_UNUSED,
    FLIC_CONN_DISCOVERED,
    FLIC_CONN_WAITING,
    FLIC_CONN_CONNECTING,
    FLIC_CONN_CONNECTED,
    FLIC_CONN_GATT_READY,
    FLIC_CONN_SESSION_ACTIVE,
    FLIC_CONN_BACKOFF
};

struct FlicConnection
{
    bool used;
    bool connected;
    bool gattReady;
    bool sessionStarted;
    bool connectPending;
    bool paired;
    bool addressKnown;
    bool temporarySlot;
    bool discardOnDisconnect;

    uint8_t slot;
    FlicConnectionState state;

    NimBLEAddress address;
    NimBLEClient *client;

    NimBLERemoteService *service;
    NimBLERemoteCharacteristic *writeChar;
    NimBLERemoteCharacteristic *notifyChar;

    Flic2Button flic;

    bool timerActive;
    double timerDeadline;

    uint32_t lastDisconnectMs;
    uint32_t connectionAttempts;
    uint32_t backoffUntilMs;
};

struct FlicClientPoolEntry
{
    NimBLEClient *client;
    int8_t ownerConnection;
    int8_t lastSlot;
};

static FlicConnection g_connections[MAX_FLIC_BUTTONS];
static FlicClientPoolEntry g_clientPool[MAX_SIMULTANEOUS_FLICS];

// ============================================================================
// OUTBOUND QUEUES
// ============================================================================

enum BridgeLogLevel : uint8_t
{
    BRIDGE_LOG_INFO,
    BRIDGE_LOG_WARN,
    BRIDGE_LOG_ERROR
};

struct PendingLog
{
    uint32_t uptimeMs;
    BridgeLogLevel level;
    char message[144];
};

struct PendingButtonEvent
{
    uint8_t slot;
    Flic2EventButtonEventType eventType;
    uint8_t eventClass;
    uint32_t eventCount;
    bool wasQueued;
    float age;
};

static const uint8_t LOG_QUEUE_SIZE = 12;
static const uint8_t BUTTON_EVENT_QUEUE_SIZE = 20;

static PendingLog g_logQueue[LOG_QUEUE_SIZE];
static PendingButtonEvent g_buttonEventQueue[BUTTON_EVENT_QUEUE_SIZE];

static uint8_t g_logQueueHead = 0;
static uint8_t g_logQueueTail = 0;
static uint8_t g_logQueueCount = 0;
static uint8_t g_buttonEventQueueHead = 0;
static uint8_t g_buttonEventQueueTail = 0;
static uint8_t g_buttonEventQueueCount = 0;
static uint16_t g_buttonStatusDirtyMask = 0;
static uint16_t g_buttonTombstoneMask = 0;
static char g_buttonTombstoneSerial[MAX_FLIC_BUTTONS][12] = {};
static uint32_t g_droppedLogMessages = 0;
static uint32_t g_droppedButtonEvents = 0;
static portMUX_TYPE g_outboundMux = portMUX_INITIALIZER_UNLOCKED;

// ============================================================================
// FORWARD DECLARATIONS
// ============================================================================

static void processFlicEvents(FlicConnection &connection);
static void startScan();
static void enterBackoff(FlicConnection &connection);
static void releaseClient(FlicConnection &connection);
static void scheduleButtonStatus(uint8_t slot);
static void scheduleButtonTombstone(uint8_t slot, const char *serial);

// ============================================================================
// TIME / RANDOM
// ============================================================================

static double steadyTime()
{
    // esp_timer_get_time() is signed 64-bit microseconds since boot and does not
    // suffer the ~49.7 day millis() rollover that breaks a monotonic Flic clock.
    return (double)esp_timer_get_time() / 1000000.0;
}

static double utcTime()
{
    // Flic explicitly permits 0 when no reliable wall clock is available.
    return 0.0;
}

static void secureRandomBytes(uint8_t *buffer, size_t length)
{
    while (length > 0) {
        uint32_t r = esp_random();
        size_t n = length < sizeof(r) ? length : sizeof(r);
        memcpy(buffer, &r, n);
        buffer += n;
        length -= n;
    }
}

// ============================================================================
// SMALL HELPERS
// ============================================================================

static const char *logLevelName(BridgeLogLevel level)
{
    switch (level) {
        case BRIDGE_LOG_WARN: return "WARN";
        case BRIDGE_LOG_ERROR: return "ERROR";
        default: return "INFO";
    }
}

static const char *connectionStateName(FlicConnectionState state)
{
    switch (state) {
        case FLIC_CONN_UNUSED: return "UNUSED";
        case FLIC_CONN_DISCOVERED: return "DISCOVERED";
        case FLIC_CONN_WAITING: return "WAITING";
        case FLIC_CONN_CONNECTING: return "CONNECTING";
        case FLIC_CONN_CONNECTED: return "CONNECTED";
        case FLIC_CONN_GATT_READY: return "GATT_READY";
        case FLIC_CONN_SESSION_ACTIVE: return "SESSION_ACTIVE";
        case FLIC_CONN_BACKOFF: return "BACKOFF";
        default: return "UNKNOWN";
    }
}

static const char *buttonEventTypeName(Flic2EventButtonEventType type)
{
    switch (type) {
        case FLIC2_EVENT_BUTTON_EVENT_TYPE_UP: return "UP";
        case FLIC2_EVENT_BUTTON_EVENT_TYPE_DOWN: return "DOWN";
        case FLIC2_EVENT_BUTTON_EVENT_TYPE_CLICK: return "CLICK";
        case FLIC2_EVENT_BUTTON_EVENT_TYPE_SINGLE_CLICK: return "SINGLE_CLICK";
        case FLIC2_EVENT_BUTTON_EVENT_TYPE_DOUBLE_CLICK: return "DOUBLE_CLICK";
        case FLIC2_EVENT_BUTTON_EVENT_TYPE_HOLD: return "HOLD";
        default: return "UNKNOWN";
    }
}

#if FLIC_VERBOSE_SERIAL
static const char *eventTypeName(Flic2EventType type)
{
    switch (type) {
        case FLIC2_EVENT_TYPE_NONE: return "NONE";
        case FLIC2_EVENT_TYPE_ONLY_DB_UPDATE: return "ONLY_DB_UPDATE";
        case FLIC2_EVENT_TYPE_SET_TIMER: return "SET_TIMER";
        case FLIC2_EVENT_TYPE_ABORT_TIMER: return "ABORT_TIMER";
        case FLIC2_EVENT_TYPE_OUTGOING_PACKET: return "OUTGOING_PACKET";
        case FLIC2_EVENT_TYPE_PAIRED: return "PAIRED";
        case FLIC2_EVENT_TYPE_UNPAIRED: return "UNPAIRED";
        case FLIC2_EVENT_TYPE_PAIRING_FAILED: return "PAIRING_FAILED";
        case FLIC2_EVENT_TYPE_SESSION_FAILED: return "SESSION_FAILED";
        case FLIC2_EVENT_TYPE_REAUTHENTICATED: return "REAUTHENTICATED";
        case FLIC2_EVENT_TYPE_BUTTON_EVENT: return "BUTTON_EVENT";
        case FLIC2_EVENT_TYPE_ALL_QUEUED_BUTTON_EVENTS_PROCESSED:
            return "ALL_QUEUED_EVENTS_PROCESSED";
        case FLIC2_EVENT_TYPE_NAME_UPDATED: return "NAME_UPDATED";
        case FLIC2_EVENT_TYPE_BATTERY_VOLTAGE_UPDATED: return "BATTERY_UPDATED";
        case FLIC2_EVENT_TYPE_CHECK_FIRMWARE_REQUEST: return "CHECK_FIRMWARE_REQUEST";
        case FLIC2_EVENT_TYPE_FIRMWARE_VERSION_UPDATED: return "FIRMWARE_VERSION_UPDATED";
        default: return "UNKNOWN";
    }
}

static void printHex(const uint8_t *data, size_t len)
{
    for (size_t i = 0; i < len; i++) {
        if (data[i] < 0x10) Serial.print('0');
        Serial.print(data[i], HEX);
        if (i + 1 < len) Serial.print(' ');
    }
}
#endif

static void bridgeLog(BridgeLogLevel level, const char *format, ...)
{
    PendingLog item{};
    item.uptimeMs = millis();
    item.level = level;

    va_list args;
    va_start(args, format);
    vsnprintf(item.message, sizeof(item.message), format, args);
    va_end(args);

    Serial.printf("[%s] %s\n", logLevelName(level), item.message);

#if FLIC_REMOTE_LOGGING
    portENTER_CRITICAL(&g_outboundMux);

    if (g_logQueueCount == LOG_QUEUE_SIZE) {
        g_logQueueTail = (uint8_t)((g_logQueueTail + 1) % LOG_QUEUE_SIZE);
        g_logQueueCount--;
        g_droppedLogMessages++;
    }

    g_logQueue[g_logQueueHead] = item;
    g_logQueueHead = (uint8_t)((g_logQueueHead + 1) % LOG_QUEUE_SIZE);
    g_logQueueCount++;

    portEXIT_CRITICAL(&g_outboundMux);
#endif
}

static void jsonEscape(const char *input, char *output, size_t outputSize)
{
    if (!input || !output || outputSize == 0) return;

    size_t out = 0;

    for (size_t i = 0; input[i] != '\0' && out + 2 < outputSize; i++) {
        const char c = input[i];

        switch (c) {
            case '"': output[out++] = '\\'; output[out++] = '"'; break;
            case '\\': output[out++] = '\\'; output[out++] = '\\'; break;
            case '\n': output[out++] = '\\'; output[out++] = 'n'; break;
            case '\r': output[out++] = '\\'; output[out++] = 'r'; break;
            case '\t': output[out++] = '\\'; output[out++] = 't'; break;
            default: output[out++] = c; break;
        }
    }

    output[out] = '\0';
}

static uint8_t flicBatteryPercent(uint16_t mv)
{
    // Approximate CR2032 discharge curve. Battery voltage is useful for trends,
    // while the percentage should be treated as a dashboard estimate.
    if (mv >= 3000) return 100;
    if (mv < 2100) return 0;

    if (mv < 2440) {
        return (uint8_t)(((uint32_t)(mv - 2100) * 6U + 170U) / 340U);
    }

    if (mv < 2740) {
        return (uint8_t)(6U + ((uint32_t)(mv - 2440) * 12U + 150U) / 300U);
    }

    if (mv < 2900) {
        return (uint8_t)(18U + ((uint32_t)(mv - 2740) * 24U + 80U) / 160U);
    }

    return (uint8_t)(42U + ((uint32_t)(mv - 2900) * 58U + 50U) / 100U);
}

static uint8_t countKnownFlics()
{
    uint8_t count = 0;
    portENTER_CRITICAL(&g_databaseMux);
    for (uint8_t i = 0; i < MAX_FLIC_BUTTONS; i++) {
        if (g_buttonDb[i].used) count++;
    }
    portEXIT_CRITICAL(&g_databaseMux);
    return count;
}

// ============================================================================
// BLE ADDRESS HELPERS
// ============================================================================

static bool addressToLittleEndian(const NimBLEAddress &address, uint8_t output[6])
{
    const ble_addr_t *base = address.getBase();
    if (!base) return false;
    memcpy(output, base->val, 6);
    return true;
}

static bool addressesEqual(const uint8_t a[6], const uint8_t b[6])
{
    return memcmp(a, b, 6) == 0;
}

static void printAddress(const uint8_t address[6])
{
    for (int i = 5; i >= 0; i--) {
        if (address[i] < 0x10) Serial.print('0');
        Serial.print(address[i], HEX);
        if (i > 0) Serial.print(':');
    }
}

static bool storedAddressToNimBLEAddress(const uint8_t stored[6], NimBLEAddress &output)
{
    ble_addr_t native{};
    native.type = BLE_ADDR_PUBLIC;
    memcpy(native.val, stored, 6);
    output = NimBLEAddress(native);
    return true;
}

// ============================================================================
// DATABASE
// ============================================================================

static void databaseClearRam()
{
    portENTER_CRITICAL(&g_databaseMux);
    memset(g_buttonDb, 0, sizeof(g_buttonDb));
    g_databaseGeneration++;
    portEXIT_CRITICAL(&g_databaseMux);
}

static void markDatabaseDirty()
{
    portENTER_CRITICAL(&g_databaseMux);
    g_databaseDirty = true;
    g_databaseDirtySinceMs = millis();
    g_databaseGeneration++;
    portEXIT_CRITICAL(&g_databaseMux);
}

static bool databaseLoad()
{
    memset(g_buttonDb, 0, sizeof(g_buttonDb));

    if (!g_preferences.begin(NVS_NAMESPACE, false)) {
        bridgeLog(BRIDGE_LOG_ERROR, "Could not open NVS namespace");
        return false;
    }

    const uint32_t version = g_preferences.getUInt(NVS_VERSION_KEY, 0);
    const size_t expectedSize = sizeof(g_buttonDb);
    const size_t storedSize = g_preferences.getBytesLength(NVS_DATABASE_KEY);

    if (version != NVS_FORMAT_VERSION || storedSize != expectedSize) {
        g_preferences.end();
        bridgeLog(BRIDGE_LOG_INFO,
                  "No compatible Flic database found (version=%lu size=%u expected=%u)",
                  (unsigned long)version,
                  (unsigned)storedSize,
                  (unsigned)expectedSize);
        return false;
    }

    const size_t readSize =
        g_preferences.getBytes(NVS_DATABASE_KEY, g_buttonDb, expectedSize);
    g_preferences.end();

    if (readSize != expectedSize) {
        memset(g_buttonDb, 0, sizeof(g_buttonDb));
        bridgeLog(BRIDGE_LOG_ERROR, "NVS Flic database read failed");
        return false;
    }

    bridgeLog(BRIDGE_LOG_INFO, "Loaded %u stored Flic pairing(s)", countKnownFlics());

#if FLIC_VERBOSE_SERIAL
    for (uint8_t i = 0; i < MAX_FLIC_BUTTONS; i++) {
        if (!g_buttonDb[i].used) continue;
        Serial.printf("Slot %u: %s  Address: ", i + 1, g_buttonDb[i].data.serial_number);
        printAddress(g_buttonDb[i].address);
        Serial.println();
    }
#endif

    return true;
}

static bool databasePersistNow()
{
    static FlicButtonRecord snapshot[MAX_FLIC_BUTTONS];
    uint32_t snapshotGeneration;

    portENTER_CRITICAL(&g_databaseMux);
    memcpy(snapshot, g_buttonDb, sizeof(snapshot));
    snapshotGeneration = g_databaseGeneration;
    portEXIT_CRITICAL(&g_databaseMux);

    if (!g_preferences.begin(NVS_NAMESPACE, false)) {
        bridgeLog(BRIDGE_LOG_ERROR, "Could not open NVS for database write");
        return false;
    }

    const size_t written =
        g_preferences.putBytes(NVS_DATABASE_KEY, snapshot, sizeof(snapshot));
    const size_t versionWritten =
        g_preferences.putUInt(NVS_VERSION_KEY, NVS_FORMAT_VERSION);
    g_preferences.end();

    if (written != sizeof(snapshot) || versionWritten != sizeof(uint32_t)) {
        bridgeLog(BRIDGE_LOG_ERROR, "Failed to persist Flic database");
        return false;
    }

    portENTER_CRITICAL(&g_databaseMux);
    if (g_databaseGeneration == snapshotGeneration) {
        g_databaseDirty = false;
    } else {
        g_databaseDirtySinceMs = millis();
    }
    portEXIT_CRITICAL(&g_databaseMux);

    VLOGLN("Flic database persisted to NVS");
    return true;
}

static void serviceDatabasePersistence()
{
    bool dirty;
    uint32_t dirtySince;

    portENTER_CRITICAL(&g_databaseMux);
    dirty = g_databaseDirty;
    dirtySince = g_databaseDirtySinceMs;
    portEXIT_CRITICAL(&g_databaseMux);

    if (!dirty) return;
    if ((uint32_t)(millis() - dirtySince) < DB_PERSIST_DELAY_MS) return;

    databasePersistNow();
}

static void factoryResetFlicDatabase()
{
    bridgeLog(BRIDGE_LOG_WARN, "Factory reset requested: erasing Flic pairing database");

    char serials[MAX_FLIC_BUTTONS][12] = {};
    portENTER_CRITICAL(&g_databaseMux);
    for (uint8_t slot = 0; slot < MAX_FLIC_BUTTONS; slot++) {
        if (g_buttonDb[slot].used) {
            memcpy(serials[slot],
                   g_buttonDb[slot].data.serial_number,
                   sizeof(serials[slot]));
            serials[slot][sizeof(serials[slot]) - 1] = '\0';
        }
    }
    portEXIT_CRITICAL(&g_databaseMux);

    if (!g_preferences.begin(NVS_NAMESPACE, false)) {
        bridgeLog(BRIDGE_LOG_ERROR, "Could not open NVS for factory reset");
        return;
    }

    const bool result = g_preferences.clear();
    g_preferences.end();
    databaseClearRam();

    portENTER_CRITICAL(&g_databaseMux);
    g_databaseDirty = false;
    portEXIT_CRITICAL(&g_databaseMux);

    if (result) {
        for (uint8_t slot = 0; slot < MAX_FLIC_BUTTONS; slot++) {
            if (serials[slot][0] != '\0') {
                scheduleButtonTombstone(slot, serials[slot]);
            }
        }
    }

    bridgeLog(result ? BRIDGE_LOG_INFO : BRIDGE_LOG_ERROR,
              result ? "Flic database erased" : "Failed to erase Flic database");
}

static int findSlotByAddress(const uint8_t address[6])
{
    int result = -1;

    portENTER_CRITICAL(&g_databaseMux);
    for (uint8_t i = 0; i < MAX_FLIC_BUTTONS; i++) {
        if (g_buttonDb[i].used && addressesEqual(g_buttonDb[i].address, address)) {
            result = i;
            break;
        }
    }
    portEXIT_CRITICAL(&g_databaseMux);

    return result;
}

static bool isSlotReserved(uint8_t slot)
{
    for (uint8_t i = 0; i < MAX_FLIC_BUTTONS; i++) {
        if (g_connections[i].used && g_connections[i].slot == slot) return true;
    }
    return false;
}

static int findFreeSlot()
{
    for (uint8_t i = 0; i < MAX_FLIC_BUTTONS; i++) {
        bool used;
        portENTER_CRITICAL(&g_databaseMux);
        used = g_buttonDb[i].used;
        portEXIT_CRITICAL(&g_databaseMux);

        if (!used && !isSlotReserved(i)) return i;
    }
    return -1;
}

static void databaseDeleteSlot(uint8_t slot)
{
    if (slot >= MAX_FLIC_BUTTONS) return;

    char serial[12] = {};

    portENTER_CRITICAL(&g_databaseMux);
    if (g_buttonDb[slot].used) {
        memcpy(serial, g_buttonDb[slot].data.serial_number, sizeof(serial));
        serial[sizeof(serial) - 1] = '\0';
    }
    memset(&g_buttonDb[slot], 0, sizeof(FlicButtonRecord));
    portEXIT_CRITICAL(&g_databaseMux);

    markDatabaseDirty();
    scheduleButtonTombstone(slot, serial);
    bridgeLog(BRIDGE_LOG_INFO, "Removed Flic slot %u from persistent database", slot + 1);
}

#if FLIC_VERBOSE_SERIAL
static void printDatabaseRecord(uint8_t slot)
{
    if (slot >= MAX_FLIC_BUTTONS) return;

    FlicButtonRecord record{};
    portENTER_CRITICAL(&g_databaseMux);
    record = g_buttonDb[slot];
    portEXIT_CRITICAL(&g_databaseMux);

    if (!record.used) return;

    Serial.printf("Flic DB slot %u serial=%s firmware=%lu battery=%u mV address=",
                  slot + 1,
                  record.data.serial_number,
                  (unsigned long)record.data.firmware_version,
                  record.data.battery_voltage_millivolt);
    printAddress(record.address);
    Serial.println();
    // Pairing material is deliberately never printed.
}
#endif

// ============================================================================
// CONNECTION LOOKUPS / CLIENT POOL
// ============================================================================

static int connectionIndexOf(const FlicConnection &connection)
{
    const ptrdiff_t index = &connection - g_connections;
    if (index < 0 || index >= MAX_FLIC_BUTTONS) return -1;
    return (int)index;
}

static int findConnectionByClient(NimBLEClient *client)
{
    if (!client) return -1;

    for (uint8_t i = 0; i < MAX_SIMULTANEOUS_FLICS; i++) {
        if (g_clientPool[i].client == client && g_clientPool[i].ownerConnection >= 0) {
            return g_clientPool[i].ownerConnection;
        }
    }

    return -1;
}

static int findConnectionByCharacteristic(NimBLERemoteCharacteristic *characteristic)
{
    if (!characteristic) return -1;
    return findConnectionByClient(characteristic->getClient());
}

static int findConnectionBySlot(uint8_t slot)
{
    for (uint8_t i = 0; i < MAX_FLIC_BUTTONS; i++) {
        if (g_connections[i].used && g_connections[i].slot == slot) return i;
    }
    return -1;
}

static int findFreeConnection()
{
    for (uint8_t i = 0; i < MAX_FLIC_BUTTONS; i++) {
        if (!g_connections[i].used) return i;
    }
    return -1;
}

static uint8_t countActiveBleConnections()
{
    uint8_t count = 0;
    for (uint8_t i = 0; i < MAX_FLIC_BUTTONS; i++) {
        if (g_connections[i].used && g_connections[i].connected) count++;
    }
    return count;
}

static int findPoolEntryByClient(NimBLEClient *client)
{
    if (!client) return -1;
    for (uint8_t i = 0; i < MAX_SIMULTANEOUS_FLICS; i++) {
        if (g_clientPool[i].client == client) return i;
    }
    return -1;
}

static void releaseClient(FlicConnection &connection)
{
    NimBLEClient *client = connection.client;
    if (!client) return;

    const int poolIndex = findPoolEntryByClient(client);
    if (poolIndex >= 0) {
        g_clientPool[poolIndex].ownerConnection = -1;
    }

    connection.client = nullptr;
}

static void resetConnectionRecord(FlicConnection &connection)
{
    releaseClient(connection);
    connection = FlicConnection{};
    connection.state = FLIC_CONN_UNUSED;
}

// ============================================================================
// BACKOFF
// ============================================================================

static uint32_t calculateBackoff(uint32_t attempts)
{
    if (attempts == 0) return FLIC_BACKOFF_MIN_MS;

    uint32_t exponent = attempts - 1;
    if (exponent > 5) exponent = 5;

    uint32_t delayMs = FLIC_BACKOFF_MIN_MS << exponent;
    if (delayMs > FLIC_BACKOFF_MAX_MS) delayMs = FLIC_BACKOFF_MAX_MS;
    return delayMs;
}

static void enterBackoff(FlicConnection &connection)
{
    connection.connectPending = false;
    connection.connected = false;
    connection.gattReady = false;
    connection.sessionStarted = false;
    connection.service = nullptr;
    connection.writeChar = nullptr;
    connection.notifyChar = nullptr;
    connection.timerActive = false;
    connection.lastDisconnectMs = millis();
    connection.state = FLIC_CONN_BACKOFF;
    connection.backoffUntilMs = millis() + calculateBackoff(connection.connectionAttempts);

    if (!connection.client || !connection.client->isConnected()) {
        releaseClient(connection);
    }

    scheduleButtonStatus(connection.slot);
}

// ============================================================================
// FLIC INITIALISATION
// ============================================================================

static bool hasStoredPairing(const Flic2DbData &data)
{
    return data.pairing[0] || data.pairing[1] || data.pairing[2] || data.pairing[3];
}

static void initialiseConnectionFlic(FlicConnection &connection)
{
    uint8_t randomSeed[16];
    secureRandomBytes(randomSeed, sizeof(randomSeed));

    FlicButtonRecord record{};
    portENTER_CRITICAL(&g_databaseMux);
    record = g_buttonDb[connection.slot];
    portEXIT_CRITICAL(&g_databaseMux);

    const bool paired = record.used && hasStoredPairing(record.data);

    uint64_t nonce = ((uint64_t)esp_random() << 32) | (uint64_t)esp_random();

    flic2_init(&connection.flic,
               record.address,
               paired ? &record.data : nullptr,
               randomSeed,
               nonce);

    flic2_set_connection_parameters(&connection.flic, 80, 90, 17, 800);
    flic2_set_auto_disconnect_timeout(&connection.flic, 511);

    VLOGF("Initialised Flic slot %u (%s)\n",
          connection.slot + 1,
          paired ? "stored pairing" : "new pairing");
}

static bool setFlicAddress(uint8_t slot, const NimBLEAddress &address)
{
    if (slot >= MAX_FLIC_BUTTONS) return false;

    uint8_t littleEndian[6];
    if (!addressToLittleEndian(address, littleEndian)) return false;

    portENTER_CRITICAL(&g_databaseMux);
    memcpy(g_buttonDb[slot].address, littleEndian, sizeof(littleEndian));
    portEXIT_CRITICAL(&g_databaseMux);
    return true;
}

// ============================================================================
// OUTBOUND QUEUE HELPERS
// ============================================================================

static void scheduleButtonStatus(uint8_t slot)
{
    if (slot >= MAX_FLIC_BUTTONS) return;

    portENTER_CRITICAL(&g_outboundMux);
    g_buttonStatusDirtyMask |= (uint16_t)(1U << slot);
    portEXIT_CRITICAL(&g_outboundMux);
}

static void scheduleButtonTombstone(uint8_t slot, const char *serial)
{
    if (slot >= MAX_FLIC_BUTTONS) return;

    portENTER_CRITICAL(&g_outboundMux);
    g_buttonStatusDirtyMask &= (uint16_t)~(1U << slot);
    memset(g_buttonTombstoneSerial[slot], 0, sizeof(g_buttonTombstoneSerial[slot]));
    if (serial && serial[0] != '\0') {
        strncpy(g_buttonTombstoneSerial[slot],
                serial,
                sizeof(g_buttonTombstoneSerial[slot]) - 1);
    }
    g_buttonTombstoneMask |= (uint16_t)(1U << slot);
    portEXIT_CRITICAL(&g_outboundMux);
}

static void scheduleAllButtonStatus()
{
    uint16_t mask = 0;

    portENTER_CRITICAL(&g_databaseMux);
    for (uint8_t slot = 0; slot < MAX_FLIC_BUTTONS; slot++) {
        if (g_buttonDb[slot].used) mask |= (uint16_t)(1U << slot);
    }
    portEXIT_CRITICAL(&g_databaseMux);

    portENTER_CRITICAL(&g_outboundMux);
    g_buttonStatusDirtyMask |= mask;
    portEXIT_CRITICAL(&g_outboundMux);
}

static void queueButtonEvent(uint8_t slot, const Flic2Event &event)
{
    PendingButtonEvent item{};
    item.slot = slot;
    item.eventType = event.event.button_event.event_type;
    item.eventClass = event.event.button_event.event_class;
    item.eventCount = event.event.button_event.event_count;
    item.wasQueued = event.event.button_event.was_queued;
    item.age = (float)event.event.button_event.age;

    portENTER_CRITICAL(&g_outboundMux);

    if (g_buttonEventQueueCount == BUTTON_EVENT_QUEUE_SIZE) {
        g_buttonEventQueueTail =
            (uint8_t)((g_buttonEventQueueTail + 1) % BUTTON_EVENT_QUEUE_SIZE);
        g_buttonEventQueueCount--;
        g_droppedButtonEvents++;
    }

    g_buttonEventQueue[g_buttonEventQueueHead] = item;
    g_buttonEventQueueHead =
        (uint8_t)((g_buttonEventQueueHead + 1) % BUTTON_EVENT_QUEUE_SIZE);
    g_buttonEventQueueCount++;

    portEXIT_CRITICAL(&g_outboundMux);
}

static bool popButtonEvent(PendingButtonEvent &item)
{
    bool haveItem = false;

    portENTER_CRITICAL(&g_outboundMux);
    if (g_buttonEventQueueCount > 0) {
        item = g_buttonEventQueue[g_buttonEventQueueTail];
        g_buttonEventQueueTail =
            (uint8_t)((g_buttonEventQueueTail + 1) % BUTTON_EVENT_QUEUE_SIZE);
        g_buttonEventQueueCount--;
        haveItem = true;
    }
    portEXIT_CRITICAL(&g_outboundMux);

    return haveItem;
}

static bool popLog(PendingLog &item)
{
    bool haveItem = false;

    portENTER_CRITICAL(&g_outboundMux);
    if (g_logQueueCount > 0) {
        item = g_logQueue[g_logQueueTail];
        g_logQueueTail = (uint8_t)((g_logQueueTail + 1) % LOG_QUEUE_SIZE);
        g_logQueueCount--;
        haveItem = true;
    }
    portEXIT_CRITICAL(&g_outboundMux);

    return haveItem;
}

// ============================================================================
// MQTT TOPICS / PUBLISHERS
// ============================================================================

static void makeBridgeStatusTopic(char *buffer, size_t length)
{
    snprintf(buffer, length, "%s/bridge/status", MQTT_ROOT);
}

static void makeBridgeInfoTopic(char *buffer, size_t length)
{
    snprintf(buffer, length, "%s/bridge/info", MQTT_ROOT);
}

static void makeBridgeMetricsTopic(char *buffer, size_t length)
{
    snprintf(buffer, length, "%s/bridge/metrics", MQTT_ROOT);
}

static void makeBridgeLogTopic(char *buffer, size_t length)
{
    snprintf(buffer, length, "%s/bridge/log", MQTT_ROOT);
}

static void makeButtonEventTopic(const char *serial, char *buffer, size_t length)
{
    snprintf(buffer, length, "%s/%s/event", MQTT_ROOT, serial);
}

static void makeButtonStatusTopic(const char *serial, char *buffer, size_t length)
{
    snprintf(buffer, length, "%s/%s/status", MQTT_ROOT, serial);
}

static void mqttPublishBridgeStatus(const char *status)
{
    if (!g_mqtt.connected()) return;
    char topic[64];
    makeBridgeStatusTopic(topic, sizeof(topic));
    g_mqtt.publish(topic, status, true);
}

static void mqttPublishBridgeInfo()
{
    if (!g_mqtt.connected()) return;

    char topic[64];
    makeBridgeInfoTopic(topic, sizeof(topic));

    char payload[384];
    snprintf(payload,
             sizeof(payload),
             "{"
             "\"name\":\"%s\","
             "\"version\":\"%s\","
             "\"chip\":\"%s\","
             "\"max_flics\":%u,"
             "\"max_simultaneous_flics\":%u,"
             "\"connected_flics\":%u,"
             "\"ota_hostname\":\"%s\""
             "}",
             BRIDGE_NAME,
             BRIDGE_VERSION,
             ESP.getChipModel(),
             MAX_FLIC_BUTTONS,
             MAX_SIMULTANEOUS_FLICS,
             countActiveBleConnections(),
             OTA_HOSTNAME);

    g_mqtt.publish(topic, payload, true);
}

static void mqttPublishBridgeMetrics()
{
    if (!g_mqtt.connected()) return;

    uint32_t droppedLogs;
    uint32_t droppedEvents;
    uint8_t queuedLogs;
    uint8_t queuedEvents;

    portENTER_CRITICAL(&g_outboundMux);
    droppedLogs = g_droppedLogMessages;
    droppedEvents = g_droppedButtonEvents;
    queuedLogs = g_logQueueCount;
    queuedEvents = g_buttonEventQueueCount;
    portEXIT_CRITICAL(&g_outboundMux);

    bool databaseDirty;
    portENTER_CRITICAL(&g_databaseMux);
    databaseDirty = g_databaseDirty;
    portEXIT_CRITICAL(&g_databaseMux);

    char topic[64];
    makeBridgeMetricsTopic(topic, sizeof(topic));

    char payload[512];
    snprintf(payload,
             sizeof(payload),
             "{"
             "\"uptime_s\":%lu,"
             "\"heap_free\":%u,"
             "\"heap_min\":%u,"
             "\"wifi_rssi\":%d,"
             "\"mqtt_connected\":true,"
             "\"ota_ready\":%s,"
             "\"ota_active\":%s,"
             "\"known_flics\":%u,"
             "\"connected_flics\":%u,"
             "\"queued_logs\":%u,"
             "\"queued_events\":%u,"
             "\"dropped_logs\":%lu,"
             "\"dropped_events\":%lu,"
             "\"database_dirty\":%s,"
             "\"sketch_size\":%u,"
             "\"free_sketch_space\":%u"
             "}",
             (unsigned long)(millis() / 1000UL),
             ESP.getFreeHeap(),
             ESP.getMinFreeHeap(),
             g_wifiConnected ? WiFi.RSSI() : 0,
             g_otaStarted ? "true" : "false",
             g_otaInProgress ? "true" : "false",
             countKnownFlics(),
             countActiveBleConnections(),
             queuedLogs,
             queuedEvents,
             (unsigned long)droppedLogs,
             (unsigned long)droppedEvents,
             databaseDirty ? "true" : "false",
             ESP.getSketchSize(),
             ESP.getFreeSketchSpace());

    g_mqtt.publish(topic, payload, true);
}

static void mqttPublishButtonStatusNow(uint8_t slot)
{
    if (!g_mqtt.connected() || slot >= MAX_FLIC_BUTTONS) return;

    FlicButtonRecord record{};
    portENTER_CRITICAL(&g_databaseMux);
    record = g_buttonDb[slot];
    portEXIT_CRITICAL(&g_databaseMux);

    if (!record.used) return;

    bool connected = false;
    FlicConnectionState state = FLIC_CONN_UNUSED;
    uint32_t attempts = 0;

    const int ci = findConnectionBySlot(slot);
    if (ci >= 0) {
        connected = g_connections[ci].connected;
        state = g_connections[ci].state;
        attempts = g_connections[ci].connectionAttempts;
    }

    char nameRaw[24];
    const uint8_t nameLen = record.data.name.len > 23 ? 23 : record.data.name.len;
    memcpy(nameRaw, record.data.name.value, nameLen);
    nameRaw[nameLen] = '\0';

    char escapedName[64];
    jsonEscape(nameRaw, escapedName, sizeof(escapedName));

    char batteryFields[80];
    if (record.data.battery_voltage_millivolt > 0) {
        snprintf(batteryFields,
                 sizeof(batteryFields),
                 "\"battery_mv\":%u,\"battery_pct\":%u,",
                 record.data.battery_voltage_millivolt,
                 flicBatteryPercent(record.data.battery_voltage_millivolt));
    } else {
        // Keep battery_mv numeric for compatibility with the v1 status payload.
        snprintf(batteryFields,
                 sizeof(batteryFields),
                 "\"battery_mv\":0,\"battery_pct\":null,");
    }

    char topic[64];
    makeButtonStatusTopic(record.data.serial_number, topic, sizeof(topic));

    char payload[512];
    snprintf(payload,
             sizeof(payload),
             "{"
             "\"slot\":%u,"
             "\"serial\":\"%s\","
             "\"name\":\"%s\","
             "\"firmware\":%lu,"
             "%s"
             "\"paired\":true,"
             "\"connected\":%s,"
             "\"state\":\"%s\","
             "\"connection_attempts\":%lu"
             "}",
             slot + 1,
             record.data.serial_number,
             escapedName,
             (unsigned long)record.data.firmware_version,
             batteryFields,
             connected ? "true" : "false",
             connectionStateName(state),
             (unsigned long)attempts);

    g_mqtt.publish(topic, payload, true);
}

static void mqttPublishButtonTombstoneNow(const char *serial)
{
    if (!g_mqtt.connected() || !serial || serial[0] == '\0') return;

    char topic[64];
    makeButtonStatusTopic(serial, topic, sizeof(topic));
    g_mqtt.publish(topic, "", true);
}

static void mqttPublishButtonEventNow(const PendingButtonEvent &event)
{
    if (!g_mqtt.connected() || event.slot >= MAX_FLIC_BUTTONS) return;

    FlicButtonRecord record{};
    portENTER_CRITICAL(&g_databaseMux);
    record = g_buttonDb[event.slot];
    portEXIT_CRITICAL(&g_databaseMux);

    if (!record.used) return;

    char topic[64];
    makeButtonEventTopic(record.data.serial_number, topic, sizeof(topic));

    char payload[384];
    if (event.wasQueued) {
        snprintf(payload,
                 sizeof(payload),
                 "{"
                 "\"slot\":%u,"
                 "\"serial\":\"%s\","
                 "\"event\":\"%s\","
                 "\"class\":%u,"
                 "\"count\":%lu,"
                 "\"queued\":true,"
                 "\"age\":%.3f"
                 "}",
                 event.slot + 1,
                 record.data.serial_number,
                 buttonEventTypeName(event.eventType),
                 event.eventClass,
                 (unsigned long)event.eventCount,
                 event.age);
    } else {
        snprintf(payload,
                 sizeof(payload),
                 "{"
                 "\"slot\":%u,"
                 "\"serial\":\"%s\","
                 "\"event\":\"%s\","
                 "\"class\":%u,"
                 "\"count\":%lu,"
                 "\"queued\":false"
                 "}",
                 event.slot + 1,
                 record.data.serial_number,
                 buttonEventTypeName(event.eventType),
                 event.eventClass,
                 (unsigned long)event.eventCount);
    }

    if (!g_mqtt.publish(topic, payload, false)) {
        Serial.printf("[WARN] MQTT button event publish failed for slot %u\n", event.slot + 1);
    }
}

static void mqttPublishLogNow(const PendingLog &item)
{
#if FLIC_REMOTE_LOGGING
    if (!g_mqtt.connected()) return;

    char topic[64];
    makeBridgeLogTopic(topic, sizeof(topic));

    char escaped[320];
    jsonEscape(item.message, escaped, sizeof(escaped));

    char payload[448];
    snprintf(payload,
             sizeof(payload),
             "{\"level\":\"%s\",\"uptime_ms\":%lu,\"message\":\"%s\"}",
             logLevelName(item.level),
             (unsigned long)item.uptimeMs,
             escaped);

    g_mqtt.publish(topic, payload, false);
#else
    (void)item;
#endif
}

static void serviceMqttOutbound()
{
    if (!g_mqtt.connected()) return;

    // Clear stale retained state before publishing current state.
    uint16_t tombstones;
    char tombstoneSerial[MAX_FLIC_BUTTONS][12] = {};

    portENTER_CRITICAL(&g_outboundMux);
    tombstones = g_buttonTombstoneMask;
    g_buttonTombstoneMask = 0;
    for (uint8_t slot = 0; slot < MAX_FLIC_BUTTONS; slot++) {
        const uint16_t bit = (uint16_t)(1U << slot);
        if (tombstones & bit) {
            memcpy(tombstoneSerial[slot],
                   g_buttonTombstoneSerial[slot],
                   sizeof(tombstoneSerial[slot]));
            memset(g_buttonTombstoneSerial[slot],
                   0,
                   sizeof(g_buttonTombstoneSerial[slot]));
        }
    }
    portEXIT_CRITICAL(&g_outboundMux);

    for (uint8_t slot = 0; slot < MAX_FLIC_BUTTONS; slot++) {
        if (tombstones & (uint16_t)(1U << slot)) {
            mqttPublishButtonTombstoneNow(tombstoneSerial[slot]);
        }
    }

    // At most four retained status records per loop keeps button-event latency low.
    for (uint8_t budget = 0; budget < 4; budget++) {
        int slot = -1;

        portENTER_CRITICAL(&g_outboundMux);
        for (uint8_t i = 0; i < MAX_FLIC_BUTTONS; i++) {
            const uint16_t bit = (uint16_t)(1U << i);
            if (g_buttonStatusDirtyMask & bit) {
                g_buttonStatusDirtyMask &= (uint16_t)~bit;
                slot = i;
                break;
            }
        }
        portEXIT_CRITICAL(&g_outboundMux);

        if (slot < 0) break;
        mqttPublishButtonStatusNow((uint8_t)slot);
    }

    PendingButtonEvent buttonEvent;
    for (uint8_t budget = 0; budget < 6 && popButtonEvent(buttonEvent); budget++) {
        mqttPublishButtonEventNow(buttonEvent);
    }

    PendingLog logItem;
    for (uint8_t budget = 0; budget < 4 && popLog(logItem); budget++) {
        mqttPublishLogNow(logItem);
    }

    if ((uint32_t)(millis() - g_lastMetricsPublishMs) >= BRIDGE_METRICS_INTERVAL_MS) {
        g_lastMetricsPublishMs = millis();
        mqttPublishBridgeMetrics();
    }
}

// ============================================================================
// MQTT / WIFI / OTA
// ============================================================================

static bool connectMQTT()
{
    if (!g_wifiConnected) return false;

    if (g_mqtt.connected()) {
        g_mqttConnected = true;
        return true;
    }

    char statusTopic[64];
    makeBridgeStatusTopic(statusTopic, sizeof(statusTopic));

    bool connected;
    if (strlen(MQTT_USER) > 0) {
        connected = g_mqtt.connect(MQTT_CLIENT_ID,
                                   MQTT_USER,
                                   MQTT_PASSWORD,
                                   statusTopic,
                                   0,
                                   true,
                                   "offline");
    } else {
        connected = g_mqtt.connect(MQTT_CLIENT_ID,
                                   statusTopic,
                                   0,
                                   true,
                                   "offline");
    }

    if (!connected) {
        g_mqttConnected = false;
        Serial.printf("[WARN] MQTT connection failed, state=%d\n", g_mqtt.state());
        return false;
    }

    g_mqttConnected = true;
    bridgeLog(BRIDGE_LOG_INFO, "MQTT connected to %s:%u", MQTT_HOST, MQTT_PORT);

    mqttPublishBridgeStatus("online");
    mqttPublishBridgeInfo();
    scheduleAllButtonStatus();
    g_lastMetricsPublishMs = 0;
    mqttPublishBridgeMetrics();

    return true;
}

static void serviceMQTT()
{
    if (!g_wifiConnected) {
        g_mqttConnected = false;
        return;
    }

    if (!g_mqtt.connected()) {
        g_mqttConnected = false;
        const uint32_t now = millis();

        if ((uint32_t)(now - g_lastMqttAttemptMs) >= MQTT_RECONNECT_DELAY_MS) {
            g_lastMqttAttemptMs = now;
            connectMQTT();
        }
        return;
    }

    g_mqttConnected = true;
    g_mqtt.loop();
    serviceMqttOutbound();
}

static void startWiFi()
{
    WiFi.mode(WIFI_STA);
    WiFi.setSleep(false);
    WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
    g_lastWiFiAttemptMs = millis();
    bridgeLog(BRIDGE_LOG_INFO, "Connecting to WiFi");
}

static void serviceWiFi()
{
    const wl_status_t status = WiFi.status();

    if (status == WL_CONNECTED) {
        if (!g_wifiConnected) {
            g_wifiConnected = true;
            bridgeLog(BRIDGE_LOG_INFO,
                      "WiFi connected, IP=%s RSSI=%d dBm",
                      WiFi.localIP().toString().c_str(),
                      WiFi.RSSI());
        }
        return;
    }

    if (g_wifiConnected) {
        bridgeLog(BRIDGE_LOG_WARN, "WiFi disconnected");
        g_wifiConnected = false;
        g_mqttConnected = false;
        return;
    }

    const uint32_t now = millis();
    if ((uint32_t)(now - g_lastWiFiAttemptMs) >= WIFI_RECONNECT_DELAY_MS) {
        g_lastWiFiAttemptMs = now;
        WiFi.disconnect();
        WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
    }
}

static void setupOTA()
{
    if (g_otaStarted || !g_wifiConnected) return;

    ArduinoOTA.setHostname(OTA_HOSTNAME);
    if (strlen(OTA_PASSWORD) > 0) ArduinoOTA.setPassword(OTA_PASSWORD);

    ArduinoOTA.onStart([]() {
        g_otaInProgress = true;
        NimBLEScan *scan = NimBLEDevice::getScan();
        if (scan && scan->isScanning()) scan->stop();
        bridgeLog(BRIDGE_LOG_INFO, "OTA update started");
    });

    ArduinoOTA.onEnd([]() {
        bridgeLog(BRIDGE_LOG_INFO, "OTA update completed; rebooting");
    });

    ArduinoOTA.onError([](ota_error_t error) {
        g_otaInProgress = false;
        bridgeLog(BRIDGE_LOG_ERROR, "OTA error %u", (unsigned)error);
    });

#if FLIC_VERBOSE_SERIAL
    ArduinoOTA.onProgress([](unsigned int progress, unsigned int total) {
        const unsigned int pct = total ? (progress * 100U / total) : 0U;
        Serial.printf("OTA progress: %u%%\r", pct);
    });
#endif

    ArduinoOTA.begin();
    g_otaStarted = true;
    bridgeLog(BRIDGE_LOG_INFO, "ArduinoOTA ready as %s.local", OTA_HOSTNAME);
}

static void serviceOTA()
{
    if (!g_otaStarted) {
        setupOTA();
        return;
    }

    if (g_wifiConnected) ArduinoOTA.handle();
}

// ============================================================================
// FLIC DATABASE EVENTS
// ============================================================================

static void processDatabaseUpdate(FlicConnection &connection, const Flic2Event &event)
{
    const uint8_t slot = connection.slot;
    if (slot >= MAX_FLIC_BUTTONS) return;

    switch (event.db_update.type) {
        case FLIC2_DB_UPDATE_TYPE_NONE:
            break;

        case FLIC2_DB_UPDATE_TYPE_ADD:
        case FLIC2_DB_UPDATE_TYPE_UPDATE:
            portENTER_CRITICAL(&g_databaseMux);
            g_buttonDb[slot].data = event.db_update.fields;
            g_buttonDb[slot].used = true;
            portEXIT_CRITICAL(&g_databaseMux);

            markDatabaseDirty();
            connection.paired = true;
            connection.temporarySlot = false;
            connection.discardOnDisconnect = false;
            scheduleButtonStatus(slot);
#if FLIC_VERBOSE_SERIAL
            printDatabaseRecord(slot);
#endif
            break;

        case FLIC2_DB_UPDATE_TYPE_DELETE:
            databaseDeleteSlot(slot);
            connection.paired = false;
            connection.discardOnDisconnect = true;
            break;
    }
}

// ============================================================================
// FLIC EVENT PROCESSING
// ============================================================================

static void processFlicEvents(FlicConnection &connection)
{
    if (!connection.used) return;

    Flic2Event event;

    while (flic2_get_next_event(&connection.flic,
                                utcTime(),
                                steadyTime(),
                                &event,
                                connection.connected && connection.writeChar != nullptr)) {
        processDatabaseUpdate(connection, event);

#if FLIC_VERBOSE_SERIAL
        Serial.printf("Flic slot %u event: %s\n", connection.slot + 1, eventTypeName(event.type));
#endif

        switch (event.type) {
            case FLIC2_EVENT_TYPE_OUTGOING_PACKET:
                if (!connection.connected || !connection.writeChar) {
                    bridgeLog(BRIDGE_LOG_WARN,
                              "Slot %u produced an outgoing packet without an active GATT writer",
                              connection.slot + 1);
                    break;
                }

#if FLIC_VERBOSE_SERIAL
                Serial.printf("Slot %u TX %u bytes: ",
                              connection.slot + 1,
                              event.event.outgoing_packet.len);
                printHex(event.event.outgoing_packet.data,
                         event.event.outgoing_packet.len);
                Serial.println();
#endif

                if (!connection.writeChar->writeValue(event.event.outgoing_packet.data,
                                                       event.event.outgoing_packet.len,
                                                       false)) {
                    bridgeLog(BRIDGE_LOG_ERROR,
                              "Flic GATT write failed for slot %u",
                              connection.slot + 1);
                }
                break;

            case FLIC2_EVENT_TYPE_PAIRED:
                connection.paired = true;
                connection.temporarySlot = false;
                connection.discardOnDisconnect = false;
                connection.connectionAttempts = 0;
                scheduleButtonStatus(connection.slot);
                bridgeLog(BRIDGE_LOG_INFO,
                          "Flic slot %u paired successfully (%s)",
                          connection.slot + 1,
                          event.event.paired.serial_number);
                break;

            case FLIC2_EVENT_TYPE_REAUTHENTICATED:
                connection.paired = true;
                connection.connectionAttempts = 0;
                scheduleButtonStatus(connection.slot);
                bridgeLog(BRIDGE_LOG_INFO,
                          "Flic slot %u session re-authenticated",
                          connection.slot + 1);
                break;

            case FLIC2_EVENT_TYPE_BUTTON_EVENT:
                queueButtonEvent(connection.slot, event);
                VLOGF("Slot %u button event %s count=%lu queued=%s\n",
                      connection.slot + 1,
                      buttonEventTypeName(event.event.button_event.event_type),
                      (unsigned long)event.event.button_event.event_count,
                      event.event.button_event.was_queued ? "yes" : "no");
                break;

            case FLIC2_EVENT_TYPE_ALL_QUEUED_BUTTON_EVENTS_PROCESSED:
                VLOGF("Slot %u queued button events drained\n", connection.slot + 1);
                break;

            case FLIC2_EVENT_TYPE_BATTERY_VOLTAGE_UPDATED:
                scheduleButtonStatus(connection.slot);
                bridgeLog(BRIDGE_LOG_INFO,
                          "Flic slot %u battery %u mV (~%u%%)",
                          connection.slot + 1,
                          event.event.battery_voltage_updated.millivolt,
                          flicBatteryPercent(event.event.battery_voltage_updated.millivolt));
                break;

            case FLIC2_EVENT_TYPE_NAME_UPDATED:
                scheduleButtonStatus(connection.slot);
                VLOGF("Slot %u Flic name updated\n", connection.slot + 1);
                break;

            case FLIC2_EVENT_TYPE_FIRMWARE_VERSION_UPDATED:
                scheduleButtonStatus(connection.slot);
                VLOGF("Slot %u firmware version updated to %lu\n",
                      connection.slot + 1,
                      (unsigned long)event.event.firmware_version_updated.firmware_version);
                break;

            case FLIC2_EVENT_TYPE_CHECK_FIRMWARE_REQUEST:
                // Firmware download support is outside this bridge. Tell the Flic library
                // the check failed so its state machine can continue normally.
                flic2_on_downloaded_firmware(&connection.flic,
                                             utcTime(),
                                             steadyTime(),
                                             FLIC2_FIRMWARE_DOWNLOAD_RESULT_FAILED,
                                             nullptr,
                                             0);
                break;

            case FLIC2_EVENT_TYPE_SET_TIMER:
                connection.timerDeadline = event.event.set_timer.absolute_time;
                connection.timerActive = true;
                break;

            case FLIC2_EVENT_TYPE_ABORT_TIMER:
                connection.timerActive = false;
                break;

            case FLIC2_EVENT_TYPE_SESSION_FAILED:
                bridgeLog(BRIDGE_LOG_ERROR,
                          "Flic slot %u session failed (error=%d subcode=%d)",
                          connection.slot + 1,
                          event.event.session_failed.error_code,
                          event.event.session_failed.subcode);
                if (connection.client && connection.client->isConnected()) {
                    connection.client->disconnect();
                } else {
                    enterBackoff(connection);
                }
                return;

            case FLIC2_EVENT_TYPE_PAIRING_FAILED:
                bridgeLog(BRIDGE_LOG_WARN,
                          "Flic slot %u pairing failed (error=%d subcode=%d)",
                          connection.slot + 1,
                          event.event.pairing_failed.error_code,
                          event.event.pairing_failed.subcode);
                if (connection.temporarySlot) connection.discardOnDisconnect = true;
                if (connection.client && connection.client->isConnected()) {
                    connection.client->disconnect();
                } else if (connection.discardOnDisconnect) {
                    resetConnectionRecord(connection);
                } else {
                    enterBackoff(connection);
                }
                return;

            case FLIC2_EVENT_TYPE_UNPAIRED:
                // Flic emits DB_UPDATE_TYPE_DELETE together with UNPAIRED, and
                // processDatabaseUpdate() has already removed the persisted record.
                bridgeLog(BRIDGE_LOG_WARN,
                          "Flic slot %u reports it has been unpaired",
                          connection.slot + 1);
                connection.paired = false;
                connection.discardOnDisconnect = true;
                if (connection.client && connection.client->isConnected()) {
                    connection.client->disconnect();
                } else {
                    resetConnectionRecord(connection);
                }
                return;

            default:
                break;
        }

        // The event handlers above can deliberately discard a temporary record.
        if (!connection.used) break;
    }
}

// ============================================================================
// FLIC TIMERS
// ============================================================================

static void serviceFlicTimers()
{
    const double now = steadyTime();

    for (uint8_t i = 0; i < MAX_FLIC_BUTTONS; i++) {
        FlicConnection &connection = g_connections[i];

        if (!connection.used || !connection.sessionStarted || !connection.timerActive) continue;

        if (now >= connection.timerDeadline) {
            connection.timerActive = false;
            flic2_on_timer(&connection.flic, now);
            processFlicEvents(connection);
        }
    }
}

// ============================================================================
// NIMBLE CALLBACKS
// ============================================================================

static void flicNotificationCallback(NimBLERemoteCharacteristic *characteristic,
                                     uint8_t *data,
                                     size_t length,
                                     bool isNotify)
{
    (void)isNotify;

    const int connectionIndex = findConnectionByCharacteristic(characteristic);
    if (connectionIndex < 0) {
        bridgeLog(BRIDGE_LOG_ERROR, "Notification from an unowned Flic client");
        return;
    }

    FlicConnection &connection = g_connections[connectionIndex];
    if (!connection.gattReady || !connection.sessionStarted) return;

#if FLIC_VERBOSE_SERIAL
    Serial.printf("Slot %u RX %u bytes: ", connection.slot + 1, (unsigned)length);
    printHex(data, length);
    Serial.println();
#endif

    flic2_on_incoming_packet(&connection.flic,
                             utcTime(),
                             steadyTime(),
                             data,
                             length);
    processFlicEvents(connection);
}

class FlicClientCallbacks : public NimBLEClientCallbacks
{
public:
    void onConnect(NimBLEClient *client) override
    {
        const int ci = findConnectionByClient(client);
        if (ci < 0) {
            bridgeLog(BRIDGE_LOG_ERROR, "BLE onConnect received for an unowned client");
            return;
        }

        FlicConnection &connection = g_connections[ci];
        connection.connected = true;
        connection.gattReady = false;
        connection.sessionStarted = false;
        connection.state = FLIC_CONN_CONNECTED;
        connection.backoffUntilMs = 0;

        bridgeLog(BRIDGE_LOG_INFO,
                  "BLE connected: Flic slot %u (%s)",
                  connection.slot + 1,
                  client->getPeerAddress().toString().c_str());
        scheduleButtonStatus(connection.slot);
    }

    void onDisconnect(NimBLEClient *client, int reason) override
    {
        const int ci = findConnectionByClient(client);
        if (ci < 0) {
            Serial.printf("[WARN] BLE disconnect for unowned client, reason=%d\n", reason);
            return;
        }

        FlicConnection &connection = g_connections[ci];
        const uint8_t slot = connection.slot;

        bridgeLog(BRIDGE_LOG_WARN,
                  "BLE disconnected: Flic slot %u reason=%d",
                  slot + 1,
                  reason);

        connection.connected = false;
        connection.gattReady = false;
        connection.service = nullptr;
        connection.writeChar = nullptr;
        connection.notifyChar = nullptr;
        connection.timerActive = false;
        connection.connectPending = false;

        if (connection.sessionStarted) {
            flic2_on_disconnected(&connection.flic);
            // flic2_on_disconnected() can itself generate events. Drain them even
            // though GATT is gone; the old implementation accidentally skipped this.
            processFlicEvents(connection);
        }

        if (!connection.used) return; // Event handling discarded a temporary slot.

        connection.sessionStarted = false;
        releaseClient(connection);

        if (connection.discardOnDisconnect) {
            resetConnectionRecord(connection);
            return;
        }

        connection.lastDisconnectMs = millis();
        connection.state = FLIC_CONN_BACKOFF;
        connection.backoffUntilMs =
            millis() + calculateBackoff(connection.connectionAttempts);
        scheduleButtonStatus(slot);
    }
};

static FlicClientCallbacks g_clientCallbacks;

// ============================================================================
// CLIENT POOL
// ============================================================================

static void setClientCacheSlot(NimBLEClient *client, int8_t slot)
{
    const int poolIndex = findPoolEntryByClient(client);
    if (poolIndex >= 0) g_clientPool[poolIndex].lastSlot = slot;
}

static NimBLEClient *acquireClient(FlicConnection &connection, bool &samePeerCache)
{
    samePeerCache = false;

    if (connection.client) {
        const int poolIndex = findPoolEntryByClient(connection.client);
        if (poolIndex >= 0) {
            samePeerCache = g_clientPool[poolIndex].lastSlot == (int8_t)connection.slot;
        }
        return connection.client;
    }

    const int ci = connectionIndexOf(connection);
    if (ci < 0) return nullptr;

    int selected = -1;

    // First choice: reuse a disconnected client that still caches this peer's GATT table.
    for (uint8_t i = 0; i < MAX_SIMULTANEOUS_FLICS; i++) {
        if (g_clientPool[i].ownerConnection < 0 &&
            g_clientPool[i].client &&
            !g_clientPool[i].client->isConnected() &&
            g_clientPool[i].lastSlot == (int8_t)connection.slot) {
            selected = i;
            samePeerCache = true;
            break;
        }
    }

    // Second choice: create another client until the fixed pool is full.
    if (selected < 0) {
        for (uint8_t i = 0; i < MAX_SIMULTANEOUS_FLICS; i++) {
            if (!g_clientPool[i].client) {
                selected = i;
                break;
            }
        }
    }

    // Final choice: reuse any idle client. Its old attribute cache will be deleted.
    if (selected < 0) {
        for (uint8_t i = 0; i < MAX_SIMULTANEOUS_FLICS; i++) {
            if (g_clientPool[i].ownerConnection < 0 &&
                g_clientPool[i].client &&
                !g_clientPool[i].client->isConnected()) {
                selected = i;
                break;
            }
        }
    }

    if (selected < 0) return nullptr;

    FlicClientPoolEntry &entry = g_clientPool[selected];

    if (!entry.client) {
        entry.client = NimBLEDevice::createClient();
        if (!entry.client) return nullptr;

        entry.client->setClientCallbacks(&g_clientCallbacks, false);
        entry.client->setConnectionParams(80, 90, 17, 800);
        entry.client->setConnectTimeout(5000);
        entry.lastSlot = -1;

        VLOGF("Created NimBLE client pool entry %u\n", selected);
    }

    samePeerCache = entry.lastSlot == (int8_t)connection.slot;
    entry.ownerConnection = (int8_t)ci;
    connection.client = entry.client;
    return entry.client;
}

// ============================================================================
// SCAN CALLBACKS
// ============================================================================

class FlicScanCallbacks : public NimBLEScanCallbacks
{
public:
    void onResult(const NimBLEAdvertisedDevice *device) override
    {
        if (!device || g_otaInProgress) return;

        bool isFlic = device->isAdvertisingService(NimBLEUUID(FLIC_SERVICE_UUID));

        if (!isFlic && device->haveName()) {
            const std::string name = device->getName();
            isFlic = name.length() >= 2 && name[0] == 'F' && name[1] == '2';
        }

        if (!isFlic) return;

        uint8_t address[6];
        if (!addressToLittleEndian(device->getAddress(), address)) {
            bridgeLog(BRIDGE_LOG_ERROR, "Could not convert Flic BLE address");
            return;
        }

        int slot = findSlotByAddress(address);
        bool temporarySlot = false;

        if (slot < 0) {
            slot = findFreeSlot();
            if (slot < 0) {
                bridgeLog(BRIDGE_LOG_WARN, "All %u Flic database slots are occupied/reserved",
                          MAX_FLIC_BUTTONS);
                return;
            }
            temporarySlot = true;
        }

        const int existingConnection = findConnectionBySlot((uint8_t)slot);
        if (existingConnection >= 0) {
            FlicConnection &connection = g_connections[existingConnection];

            if (connection.connected ||
                connection.connectPending ||
                connection.state == FLIC_CONN_CONNECTING) {
                return;
            }

            connection.address = device->getAddress();
            connection.addressKnown = true;
            connection.state = FLIC_CONN_WAITING;
            connection.connectPending = true;
            connection.lastDisconnectMs = millis() - RECONNECT_DELAY_MS;
            return;
        }

        const int connectionIndex = findFreeConnection();
        if (connectionIndex < 0) {
            bridgeLog(BRIDGE_LOG_WARN, "No free Flic connection record");
            return;
        }

        FlicConnection &connection = g_connections[connectionIndex];
        connection = FlicConnection{};

        connection.used = true;
        connection.connectPending = true;
        connection.paired = !temporarySlot;
        connection.addressKnown = true;
        connection.temporarySlot = temporarySlot;
        connection.discardOnDisconnect = false;
        connection.slot = (uint8_t)slot;
        connection.state = FLIC_CONN_WAITING;
        connection.address = device->getAddress();
        connection.lastDisconnectMs = millis() - RECONNECT_DELAY_MS;

        // The address is needed by flic2_init even before pairing. It is only
        // persisted once the protocol supplies a valid ADD/UPDATE DB event.
        portENTER_CRITICAL(&g_databaseMux);
        memcpy(g_buttonDb[slot].address, address, sizeof(address));
        portEXIT_CRITICAL(&g_databaseMux);

        if (temporarySlot) {
            bridgeLog(BRIDGE_LOG_INFO,
                      "New Flic discovered at %s -> temporary slot %u",
                      device->getAddress().toString().c_str(),
                      slot + 1);
        } else {
            VLOGF("Known Flic slot %u discovered at %s\n",
                  slot + 1,
                  device->getAddress().toString().c_str());
        }
    }
};

static FlicScanCallbacks g_scanCallbacks;

// ============================================================================
// CONNECTION FAILURE / CONNECT
// ============================================================================

static void failConnection(FlicConnection &connection, const char *reason)
{
    bridgeLog(BRIDGE_LOG_WARN,
              "Connection setup failed for Flic slot %u: %s",
              connection.slot + 1,
              reason ? reason : "unknown");

    connection.gattReady = false;
    connection.sessionStarted = false;
    connection.service = nullptr;
    connection.writeChar = nullptr;
    connection.notifyChar = nullptr;
    connection.timerActive = false;

    // A GATT setup failure can mean this client's cached attribute table is stale.
    // Force a fresh discovery the next time this pool entry is used.
    if (connection.client) setClientCacheSlot(connection.client, -1);

    if (connection.client && connection.client->isConnected()) {
        connection.client->disconnect();
    } else {
        enterBackoff(connection);
    }
}

static bool connectToFlic(FlicConnection &connection)
{
    if (connection.connected || g_otaInProgress) return connection.connected;
    if (countActiveBleConnections() >= MAX_SIMULTANEOUS_FLICS) return false;

    bool samePeerCache = false;
    NimBLEClient *client = acquireClient(connection, samePeerCache);

    if (!client) {
        connection.connectionAttempts++;
        enterBackoff(connection);
        return false;
    }

    connection.state = FLIC_CONN_CONNECTING;
    connection.connectionAttempts++;

    VLOGF("Connecting Flic slot %u address=%s attempt=%lu cache=%s\n",
          connection.slot + 1,
          connection.address.toString().c_str(),
          (unsigned long)connection.connectionAttempts,
          samePeerCache ? "reuse" : "refresh");

    // If this pool client previously belonged to another peer, its old GATT
    // attributes must be deleted. For the same peer, retain the cached table.
    if (!samePeerCache) setClientCacheSlot(client, -1);

    if (!client->connect(connection.address,
                         !samePeerCache,
                         false,
                         true)) {
        bridgeLog(BRIDGE_LOG_WARN,
                  "BLE connection failed for Flic slot %u",
                  connection.slot + 1);
        enterBackoff(connection);
        return false;
    }

    connection.service = client->getService(NimBLEUUID(FLIC_SERVICE_UUID));
    if (!connection.service) {
        failConnection(connection, "Flic service not found");
        return false;
    }

    connection.writeChar =
        connection.service->getCharacteristic(NimBLEUUID(FLIC_WRITE_UUID));
    if (!connection.writeChar) {
        failConnection(connection, "write characteristic not found");
        return false;
    }

    connection.notifyChar =
        connection.service->getCharacteristic(NimBLEUUID(FLIC_NOTIFY_UUID));
    if (!connection.notifyChar) {
        failConnection(connection, "notify characteristic not found");
        return false;
    }

    if (!connection.notifyChar->canNotify()) {
        failConnection(connection, "notify characteristic cannot notify");
        return false;
    }

    if (!connection.notifyChar->subscribe(true, flicNotificationCallback)) {
        failConnection(connection, "notification subscription failed");
        return false;
    }

    if (!setFlicAddress(connection.slot, connection.address)) {
        failConnection(connection, "BLE address copy failed");
        return false;
    }

    setClientCacheSlot(client, (int8_t)connection.slot);
    initialiseConnectionFlic(connection);

    uint16_t mtu = client->getMTU();
    if (mtu < 23) mtu = 23;
    if (mtu > FLIC_ATT_MTU_MAX) mtu = FLIC_ATT_MTU_MAX;

    connection.gattReady = true;
    connection.sessionStarted = true;
    connection.connectPending = false;
    connection.state = FLIC_CONN_SESSION_ACTIVE;

    flic2_start(&connection.flic, steadyTime(), mtu);
    processFlicEvents(connection);
    scheduleButtonStatus(connection.slot);

    VLOGF("Flic protocol session started for slot %u, MTU=%u\n",
          connection.slot + 1,
          mtu);
    return true;
}

// ============================================================================
// CONNECTION QUEUE
// ============================================================================

static bool hasPendingConnection()
{
    for (uint8_t i = 0; i < MAX_FLIC_BUTTONS; i++) {
        if (g_connections[i].used && g_connections[i].connectPending) return true;
    }
    return false;
}

static void serviceConnectionBackoff()
{
    const uint32_t now = millis();

    for (uint8_t i = 0; i < MAX_FLIC_BUTTONS; i++) {
        FlicConnection &connection = g_connections[i];
        if (!connection.used || connection.state != FLIC_CONN_BACKOFF) continue;

        if ((int32_t)(now - connection.backoffUntilMs) < 0) continue;

        connection.state = FLIC_CONN_WAITING;
        connection.connectPending = true;
        VLOGF("Flic slot %u leaving backoff\n", connection.slot + 1);
    }
}

static void serviceConnectionQueue()
{
    if (g_otaInProgress) return;

    const uint32_t now = millis();

    if ((int32_t)(now - g_bleConnectAfterMs) < 0) {
        return;
    }

    serviceConnectionBackoff();

    for (uint8_t i = 0; i < MAX_FLIC_BUTTONS; i++) {
        FlicConnection &connection = g_connections[i];

        if (!connection.used ||
            !connection.connectPending ||
            connection.connected ||
            !connection.addressKnown) {
            continue;
        }

        if (countActiveBleConnections() >= MAX_SIMULTANEOUS_FLICS) return;
        if ((uint32_t)(now - connection.lastDisconnectMs) < RECONNECT_DELAY_MS) continue;

        NimBLEScan *scan = NimBLEDevice::getScan();
        if (scan && scan->isScanning()) {
            scan->stop();
            return;
        }

        connectToFlic(connection);
        return; // One potentially blocking BLE connection attempt per loop.
    }
}

static void serviceDisconnectedConnections()
{
    if (g_otaInProgress) return;

    const uint32_t now = millis();

    for (uint8_t i = 0; i < MAX_FLIC_BUTTONS; i++) {
        FlicConnection &connection = g_connections[i];

        if (!connection.used ||
            connection.connected ||
            connection.connectPending ||
            connection.state == FLIC_CONN_CONNECTING ||
            connection.state == FLIC_CONN_BACKOFF) {
            continue;
        }

        bool stored;
        portENTER_CRITICAL(&g_databaseMux);
        stored = g_buttonDb[connection.slot].used;
        portEXIT_CRITICAL(&g_databaseMux);

        if (!stored && !connection.temporarySlot) {
            resetConnectionRecord(connection);
            continue;
        }

        if ((uint32_t)(now - connection.lastDisconnectMs) < RECONNECT_DELAY_MS) continue;

        connection.state = FLIC_CONN_WAITING;
        connection.connectPending = true;
        return;
    }
}

// ============================================================================
// SCANNING
// ============================================================================

static void startScan()
{
    if (g_otaInProgress) return;

    NimBLEScan *scan = NimBLEDevice::getScan();
    if (!scan || scan->isScanning()) return;

    scan->setScanCallbacks(&g_scanCallbacks, false);
    scan->setActiveScan(true);
    scan->setInterval(45);
    scan->setWindow(15);
    scan->setMaxResults(0);
    scan->start(SCAN_TIME_MS, false, true);

    VLOGLN("Scanning for Flic 2 advertisements");
}

static void serviceScanning()
{
    if (g_otaInProgress) return;

    NimBLEScan *scan = NimBLEDevice::getScan();
    if (!scan || scan->isScanning()) return;
    if (hasPendingConnection()) return;
    if (countActiveBleConnections() >= MAX_SIMULTANEOUS_FLICS) return;

    const uint32_t now = millis();
    if ((uint32_t)(now - g_lastScanMs) < SCAN_RETRY_DELAY_MS) return;

    g_lastScanMs = now;
    startScan();
}

// ============================================================================
// STARTUP / STATUS
// ============================================================================

static void initialiseClientPool()
{
    for (uint8_t i = 0; i < MAX_SIMULTANEOUS_FLICS; i++) {
        g_clientPool[i].client = nullptr;
        g_clientPool[i].ownerConnection = -1;
        g_clientPool[i].lastSlot = -1;
    }
}

static void restoreStoredConnections()
{
    for (uint8_t slot = 0; slot < MAX_FLIC_BUTTONS; slot++) {
        FlicButtonRecord record{};
        portENTER_CRITICAL(&g_databaseMux);
        record = g_buttonDb[slot];
        portEXIT_CRITICAL(&g_databaseMux);

        if (!record.used) continue;

        const int ci = findFreeConnection();
        if (ci < 0) {
            bridgeLog(BRIDGE_LOG_ERROR, "No RAM connection record for stored Flic slot %u", slot + 1);
            break;
        }

        FlicConnection &connection = g_connections[ci];
        connection = FlicConnection{};

        connection.used = true;
        connection.slot = slot;
        connection.paired = true;
        connection.addressKnown = true;
        connection.temporarySlot = false;
        connection.state = FLIC_CONN_WAITING;
        connection.connectPending = false;
        connection.lastDisconnectMs = millis() - RECONNECT_DELAY_MS;
        storedAddressToNimBLEAddress(record.address, connection.address);

        VLOGF("Restored Flic slot %u serial=%s\n", slot + 1, record.data.serial_number);
    }
}

static void printStartupBanner()
{
    Serial.println();
    Serial.println("==============================================");
    Serial.printf(" %s v%s\n", BRIDGE_NAME, BRIDGE_VERSION);
    Serial.println(" XIAO ESP32-C6 / Flic 2 / MQTT / ArduinoOTA");
    Serial.println("==============================================");
    Serial.printf("Chip: %s\n", ESP.getChipModel());
    Serial.printf("CPU: %u MHz\n", ESP.getCpuFreqMHz());
    Serial.printf("Free heap: %u\n", ESP.getFreeHeap());
    Serial.printf("Sketch size: %u bytes\n", ESP.getSketchSize());
    Serial.printf("Free sketch space: %u bytes\n", ESP.getFreeSketchSpace());
    Serial.printf("Persistent Flic slots: %u\n", MAX_FLIC_BUTTONS);
    Serial.printf("Simultaneous BLE clients: %u\n", MAX_SIMULTANEOUS_FLICS);
    Serial.printf("Verbose serial: %s\n", FLIC_VERBOSE_SERIAL ? "enabled" : "disabled");
    Serial.println("==============================================");
}

// ============================================================================
// PUBLIC ARDUINO ENTRY POINTS
// ============================================================================

void bridgeSetup()
{
    Serial.begin(115200);
    delay(750);
    printStartupBanner();

    initialiseClientPool();

    if (FACTORY_RESET_FLICS_ON_BOOT) {
        factoryResetFlicDatabase();
    } else {
        databaseLoad();
    }

    restoreStoredConnections();

    startWiFi();

    g_mqtt.setServer(MQTT_HOST, MQTT_PORT);
    g_mqtt.setBufferSize(512);

    bridgeLog(BRIDGE_LOG_INFO, "Initialising NimBLE");
    NimBLEDevice::init(BRIDGE_NAME);
    NimBLEDevice::setPower(9);
    NimBLEDevice::setMTU(FLIC_ATT_MTU_MAX);
    g_bleConnectAfterMs = millis() + BLE_STARTUP_GRACE_MS;

    NimBLEScan *scan = NimBLEDevice::getScan();
    if (!scan) {
        bridgeLog(BRIDGE_LOG_ERROR, "Could not obtain NimBLE scanner");
        return;
    }

    scan->setScanCallbacks(&g_scanCallbacks, false);
    scan->setActiveScan(true);
    scan->setInterval(45);
    scan->setWindow(15);
    scan->setMaxResults(0);

    bridgeLog(BRIDGE_LOG_INFO,
              "Bridge ready: %u stored Flic(s), client pool=%u",
              countKnownFlics(),
              MAX_SIMULTANEOUS_FLICS);

    // Start immediately. Existing pairings reconnect only when their advertisements
    // are seen; unknown Flics can reserve temporary slots without touching NVS.
    g_lastScanMs = millis() - SCAN_RETRY_DELAY_MS;
    startScan();
}

void bridgeLoop()
{
    serviceWiFi();
    serviceOTA();
    serviceMQTT();
    serviceDatabasePersistence();

    serviceFlicTimers();

    // Flic events may also be created by timers/disconnect handling. Drain any
    // remaining events here while active sessions are healthy.
    for (uint8_t i = 0; i < MAX_FLIC_BUTTONS; i++) {
        if (g_connections[i].used && g_connections[i].sessionStarted) {
            processFlicEvents(g_connections[i]);
        }
    }

    serviceDisconnectedConnections();
    serviceConnectionQueue();
    serviceScanning();

    delay(5);
}
