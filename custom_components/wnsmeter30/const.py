"""
    component constants
"""
DOMAIN = "wnsmeter30"

CONF_ZAEHLPUNKTE = "zaehlpunkte"
CONF_SELECTED_ZAEHLPUNKTE = "selected_zaehlpunkte"
CONF_ZAEHLPUNKT_ALIASES = "zaehlpunkt_aliases"
CONF_USE_ALIAS_FOR_IDS = "use_alias_for_ids"
CONF_SCAN_INTERVAL = "scan_interval"
DEFAULT_SCAN_INTERVAL_MINUTES = 360
CONF_HISTORICAL_DAYS = "historical_days"
DEFAULT_HISTORICAL_DAYS = 1095
MAX_HISTORICAL_DAYS = 3650
HISTORICAL_API_CHUNK_DAYS = 365
CONF_ENABLE_RAW_API_RESPONSE_WRITE = "enable_raw_api_response_write"
CONF_ENABLE_DAILY_CONS = "enable_daily_cons"
DEFAULT_ENABLE_DAILY_CONS = True
CONF_ENABLE_DAILY_METER_READ = "enable_daily_meter_read"
DEFAULT_ENABLE_DAILY_METER_READ = True
DEFAULT_USE_ALIAS_FOR_IDS = False

ATTRS_ZAEHLPUNKT_CALL = [
    ("zaehlpunktnummer", "zaehlpunktnummer"),
    ("customLabel", "label"),
    ("equipmentNumber", "equipmentNumber"),
    ("dailyConsumption", "dailyConsumption"),
    ("geraetNumber", "deviceId"),
    ("customerId", "geschaeftspartner"),
    ("verbrauchsstelle.strasse", "street"),
    ("verbrauchsstelle.hausnummer", "streetNumber"),
    ("verbrauchsstelle.postleitzahl", "zip"),
    ("verbrauchsstelle.ort", "city"),
    ("verbrauchsstelle.laengengrad", "longitude"),
    ("verbrauchsstelle.breitengrad", "latitude"),
    ("anlage.typ", "type"),
]

ATTRS_ZAEHLPUNKTE_CALL = [
    ("geschaeftspartner", "customerId"),
    ("zaehlpunktnummer", "zaehlpunktnummer"),
    ("customLabel", "label"),
    ("equipmentNumber", "equipmentNumber"),
    ("geraetNumber", "deviceId"),
    ("verbrauchsstelle.strasse", "street"),
    ("verbrauchsstelle.anlageHausnummer", "streetNumber"),
    ("verbrauchsstelle.postleitzahl", "zip"),
    ("verbrauchsstelle.ort", "city"),
    ("verbrauchsstelle.laengengrad", "longitude"),
    ("verbrauchsstelle.breitengrad", "latitude"),
    ("anlage.typ", "type"),
    ("isDefault", "default"),
    ("isActive", "active"),
    ("isSmartMeterMarketReady", "smartMeterReady"),
    ("idexStatus.granularity.status", "granularity")
]

ATTRS_CONSUMPTIONS_CALL = [
    ("consumptionYesterday.value", "consumptionYesterdayValue"),
    ("consumptionYesterday.validated", "consumptionYesterdayValidated"),
    ("consumptionYesterday.date", "consumptionYesterdayTimestamp"),
    ("consumptionDayBeforeYesterday.value", "consumptionDayBeforeYesterdayValue"),
    ("consumptionDayBeforeYesterday.validated", "consumptionDayBeforeYesterdayValidated"),
    ("consumptionDayBeforeYesterday.date", "consumptionDayBeforeYesterdayTimestamp"),
]

ATTRS_BASEINFORMATION_CALL = [
    ("hasSmartMeter", "hasSmartMeter"),
    ("isDataDeleted", "isDataDeleted"),
    ("dataDeletionTimestampUTC", "dataDeletionAt"),
    ("zaehlpunkt.zaehlpunktName", "name"),
    ("zaehlpunkt.zaehlpunktnummer", "zaehlpunkt"),
    ("zaehlpunkt.zaehlpunktAnlagentyp", "type"),
    ("zaehlpunkt.adresse", "address"),
    ("zaehlpunkt.postleitzahl", "zip"),
]

ATTRS_METERREADINGS_CALL = [
    ("meterReadings.0.value", "lastValue"),
    ("meterReadings.0.date", "lastReading"),
    ("meterReadings.0.validated", "lastValidated"),
    ("meterReadings.0.type", "lastType")
]

ATTRS_VERBRAUCH_CALL = [
    ("quarter-hour-opt-in", "optIn"),
    ("statistics.average", "consumptionAverage"),
    ("statistics.minimum", "consumptionMinimum"),
    ("statistics.maximum", "consumptionMaximum"),
    ("values", "values"),
]

ATTRS_HISTORIC_DATA = [
    ('obisCode', 'obisCode'),
    ('einheit', 'unitOfMeasurement'),
    ('messwerte', 'values'),
]

ATTRS_BEWEGUNGSDATEN = [
    ('descriptor.geschaeftspartnernummer', 'customerId'),
    ('descriptor.zaehlpunktnummer', 'zaehlpunkt'),
    ('descriptor.rolle', 'role'),
    ('descriptor.aggregat', 'aggregator'),
    ('descriptor.granularitaet', 'granularity'),
    ('descriptor.einheit', 'unitOfMeasurement'),
    ('values', 'values'),
]

ATTRS_HISTORIC_MEASUREMENT = [
]
