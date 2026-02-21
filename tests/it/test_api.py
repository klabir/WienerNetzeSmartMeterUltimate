"""API tests"""
import pytest
import time
import logging
import re
from requests_mock import Mocker
import datetime as dt
from dateutil.relativedelta import relativedelta

from it import (
    expect_login,
    expect_verbrauch,
    smartmeter,
    expect_zaehlpunkte,
    verbrauch_raw_response,
    zaehlpunkt,
    zaehlpunkt_feeding,
    enabled,
    disabled,
    mock_login_page,
    mock_authenticate,
    PASSWORD,
    USERNAME,
    mock_token,
    mock_refresh_token,
    mock_get_api_key,
    expect_history, expect_bewegungsdaten, zaehlpunkt_response,
)
from wnsm.api.errors import SmartmeterConnectionError, SmartmeterLoginError, SmartmeterQueryError
import wnsm.api.constants as const

COUNT = 10

logger = logging.getLogger(__name__)

@pytest.mark.usefixtures("requests_mock")
def test_successful_login(requests_mock: Mocker):
    expect_login(requests_mock)

    smartmeter().login()
    assert True


@pytest.mark.usefixtures("requests_mock")
def test_unsuccessful_login_failing_on_login_page_load(requests_mock):
    mock_login_page(requests_mock, 404)
    with pytest.raises(SmartmeterConnectionError) as exc_info:
        smartmeter().login()
    assert 'Could not load login page. Error: ' in str(exc_info.value)


@pytest.mark.usefixtures("requests_mock")
def test_unsuccessful_login_failing_on_connection_timeout_while_login_page_load(requests_mock):
    mock_login_page(requests_mock, None)
    with pytest.raises(SmartmeterConnectionError) as exc_info:
        smartmeter().login()
    assert 'Could not load login page' in str(exc_info.value)


@pytest.mark.usefixtures("requests_mock")
def test_unsuccessful_login_failing_on_connection_timeout(requests_mock):
    mock_login_page(requests_mock)
    mock_authenticate(requests_mock, USERNAME, PASSWORD, status=None)
    with pytest.raises(SmartmeterConnectionError) as exc_info:
        smartmeter().login()
    assert 'Could not login with credentials' in str(exc_info.value)


@pytest.mark.usefixtures("requests_mock")
def test_unsuccessful_login_failing_on_credentials_login(requests_mock):
    mock_login_page(requests_mock)
    mock_authenticate(requests_mock, USERNAME, "WrongPassword", status=403)
    with pytest.raises(SmartmeterLoginError) as exc_info:
        smartmeter(username=USERNAME, password="WrongPassword").login()
    assert 'Login failed. Check username/password.' in str(exc_info.value)


@pytest.mark.usefixtures("requests_mock")
def test_unsuccessful_login_failing_on_non_bearer_token(requests_mock):
    mock_login_page(requests_mock)
    mock_authenticate(requests_mock, USERNAME, PASSWORD)
    mock_token(requests_mock, token_type="IAmNotABearerToken")
    with pytest.raises(SmartmeterLoginError) as exc_info:
        smartmeter(username=USERNAME, password=PASSWORD).login()
    assert 'Bearer token required' in str(exc_info.value)


@pytest.mark.usefixtures("requests_mock")
def test_unsuccessful_login_failing_on_redirect_when_location_header_does_not_bear_code(requests_mock):
    mock_login_page(requests_mock)
    mock_authenticate(requests_mock, USERNAME, PASSWORD, status=404)
    with pytest.raises(SmartmeterLoginError) as exc_info:
        smartmeter().login()
    assert "Login failed. Could not extract 'code' from 'Location'" in str(exc_info.value)


@pytest.mark.usefixtures("requests_mock")
def test_unsuccessful_login_failing_on_connection_timeout_while_retrieving_access_token(requests_mock):
    mock_login_page(requests_mock)
    mock_authenticate(requests_mock, USERNAME, PASSWORD)
    mock_token(requests_mock, status=None)
    with pytest.raises(SmartmeterConnectionError) as exc_info:
        smartmeter().login()
    assert 'Could not obtain access token' == str(exc_info.value)


@pytest.mark.usefixtures("requests_mock")
def test_unsuccessful_login_failing_on_empty_response_while_retrieving_access_token(requests_mock):
    mock_login_page(requests_mock)
    mock_authenticate(requests_mock, USERNAME, PASSWORD)
    mock_token(requests_mock, status=404)
    with pytest.raises(SmartmeterConnectionError) as exc_info:
        smartmeter().login()
    assert 'Could not obtain access token: ' in str(exc_info.value)


@pytest.mark.usefixtures("requests_mock")
def test_unsuccessful_login_failing_on_connection_timeout_while_get_config_on_get_api_key(requests_mock):
    mock_login_page(requests_mock)
    mock_authenticate(requests_mock, USERNAME, PASSWORD)
    mock_token(requests_mock)
    mock_get_api_key(requests_mock, get_config_status=None)
    with pytest.raises(SmartmeterConnectionError) as exc_info:
        smartmeter().login()
    assert 'Could not obtain API key' == str(exc_info.value)

@pytest.mark.usefixtures("requests_mock")
def test_unsuccessful_login_failing_on_b2c_api_key_missing(requests_mock):
    mock_login_page(requests_mock)
    mock_authenticate(requests_mock, USERNAME, PASSWORD)
    mock_token(requests_mock)
    mock_get_api_key(requests_mock, include_b2c_key = False)
    with pytest.raises(SmartmeterConnectionError) as exc_info:
        smartmeter().login()
    assert 'b2cApiKey not found in response!' == str(exc_info.value)

@pytest.mark.usefixtures("requests_mock")
def test_unsuccessful_login_failing_on_b2b_api_key_missing(requests_mock):
    mock_login_page(requests_mock)
    mock_authenticate(requests_mock, USERNAME, PASSWORD)
    mock_token(requests_mock)
    mock_get_api_key(requests_mock, include_b2b_key = False)
    with pytest.raises(SmartmeterConnectionError) as exc_info:
        smartmeter().login()
    assert 'b2bApiKey not found in response!' == str(exc_info.value)

@pytest.mark.usefixtures("requests_mock")
def test_warning_b2c_api_key_change(requests_mock,caplog):
    mock_login_page(requests_mock)
    mock_authenticate(requests_mock, USERNAME, PASSWORD)
    mock_token(requests_mock)
    mock_get_api_key(requests_mock, same_b2c_url = False)
    smartmeter().login()
    assert const.API_URL == "https://api.wstw.at/gateway/WN_SMART_METER_PORTAL_API_B2C/2.0"
    assert 'The b2cApiUrl has changed' in caplog.text
    
@pytest.mark.usefixtures("requests_mock")
def test_warning_b2b_api_key_change(requests_mock,caplog):
    mock_login_page(requests_mock)
    mock_authenticate(requests_mock, USERNAME, PASSWORD)
    mock_token(requests_mock)
    mock_get_api_key(requests_mock, same_b2b_url = False)
    smartmeter().login()
    assert const.API_URL_B2B == "https://api.wstw.at/gateway/WN_SMART_METER_PORTAL_API_B2B/2.0"
    assert 'The b2bApiUrl has changed' in caplog.text

@pytest.mark.usefixtures("requests_mock")
def test_access_key_expired(requests_mock):
    mock_login_page(requests_mock)
    mock_authenticate(requests_mock, USERNAME, PASSWORD)
    mock_token(requests_mock, expires=300)
    mock_get_api_key(requests_mock)
    sm = smartmeter().login()
    sm._access_token_expiration = dt.datetime.now() - dt.timedelta(seconds=1)
    sm._refresh_token_expiration = dt.datetime.now() - dt.timedelta(seconds=1)
    with pytest.raises(SmartmeterConnectionError) as exc_info:
        sm._access_valid_or_raise()
    assert 'Refresh Token is not valid anymore' in str(exc_info.value)


@pytest.mark.usefixtures("requests_mock")
def test_refresh_token_success_on_expired_access_token(requests_mock):
    mock_login_page(requests_mock)
    mock_authenticate(requests_mock, USERNAME, PASSWORD)
    mock_token(requests_mock, expires=1)
    mock_get_api_key(requests_mock)
    sm = smartmeter().login()
    time.sleep(2)

    new_access = "NEW_ACCESS_TOKEN_123"
    mock_refresh_token(requests_mock, access_token=new_access)
    mock_get_api_key(requests_mock, bearer_token=new_access)

    sm._access_valid_or_raise()
    assert sm._access_token == new_access


@pytest.mark.usefixtures("requests_mock")
def test_refresh_token_failure_fallback_to_full_login(requests_mock):
    mock_login_page(requests_mock)
    mock_authenticate(requests_mock, USERNAME, PASSWORD)
    mock_token(requests_mock, expires=1)
    mock_get_api_key(requests_mock)
    sm = smartmeter().login()
    time.sleep(2)

    mock_refresh_token(requests_mock, status=400)
    expect_login(requests_mock)

    sm.login()
    assert sm.is_logged_in()


@pytest.mark.usefixtures("requests_mock")
def test_call_api_401_maps_to_login_error(requests_mock):
    expect_login(requests_mock)
    requests_mock.get(re.compile(r".*/zaehlpunkte$"), status_code=401, json={"error": "unauthorized"})
    sm = smartmeter().login()
    with pytest.raises(SmartmeterLoginError) as exc_info:
        sm.zaehlpunkte()
    assert "status 401" in str(exc_info.value)


@pytest.mark.usefixtures("requests_mock")
def test_call_api_500_maps_to_connection_error(requests_mock):
    expect_login(requests_mock)
    requests_mock.get(re.compile(r".*/zaehlpunkte$"), status_code=500, json={"error": "server"})
    sm = smartmeter().login()
    with pytest.raises(SmartmeterConnectionError) as exc_info:
        sm.zaehlpunkte()
    assert "status 500" in str(exc_info.value)


def test_extract_first_form_action_normalizes_relative_url():
    sm = smartmeter()
    action = sm._extract_first_form_action(
        "<html><body><form action='/auth/realms/logwien/login-actions/authenticate?x=1'></form></body></html>",
        "No form",
        "https://log.wien/auth/realms/logwien/protocol/openid-connect/auth?foo=bar",
    )
    assert action == "https://log.wien/auth/realms/logwien/login-actions/authenticate?x=1"


@pytest.mark.usefixtures("requests_mock")
def test_zaehlpunkte(requests_mock: Mocker):
    expect_login(requests_mock)
    expect_zaehlpunkte(requests_mock, [enabled(zaehlpunkt()), disabled(zaehlpunkt())])

    zps = smartmeter().login().zaehlpunkte()
    assert 2 == len(zps[0]['zaehlpunkte'])
    assert zps[0]['zaehlpunkte'][0]['isActive']
    assert not zps[0]['zaehlpunkte'][1]['isActive']


@pytest.mark.usefixtures("requests_mock")
def test_history(requests_mock: Mocker):
    z = zaehlpunkt_response([enabled(zaehlpunkt())])[0]
    zp = z["zaehlpunkte"][0]['zaehlpunktnummer']
    customer_id = z["geschaeftspartner"]
    expect_login(requests_mock)
    expect_history(requests_mock, customer_id, zp)
    expect_zaehlpunkte(requests_mock, [enabled(zaehlpunkt())])
    hist = smartmeter().login().historical_data()
    assert 1 == len(hist['messwerte'])
    assert 'WH' == hist['einheit']
    assert '1-1:1.8.0' == hist['obisCode']
 
@pytest.mark.usefixtures("requests_mock")
def test_history_with_zp(requests_mock: Mocker):
    z = zaehlpunkt_response([enabled(zaehlpunkt())])[0]
    zp = z["zaehlpunkte"][0]['zaehlpunktnummer']
    customer_id = z["geschaeftspartner"]
    expect_login(requests_mock)
    expect_history(requests_mock, customer_id, zp)
    expect_zaehlpunkte(requests_mock, [enabled(zaehlpunkt())])
    hist = smartmeter().login().historical_data(zp)
    assert 1 == len(hist['messwerte'])
    assert 'WH' == hist['einheit']
    assert '1-1:1.8.0' == hist['obisCode']


@pytest.mark.usefixtures("requests_mock")
def test_history_meter_read_query_type(requests_mock: Mocker):
    z = zaehlpunkt_response([enabled(zaehlpunkt())])[0]
    zp = z["zaehlpunkte"][0]["zaehlpunktnummer"]
    customer_id = z["geschaeftspartner"]
    expect_login(requests_mock)
    expect_history(requests_mock, customer_id, zp)
    expect_zaehlpunkte(requests_mock, [enabled(zaehlpunkt())])

    hist = smartmeter().login().historical_data(
        zp,
        dt.date(2026, 2, 20),
        dt.date(2026, 2, 21),
        const.ValueType.METER_READ,
    )

    assert 1 == len(hist["messwerte"])
    assert requests_mock.request_history[-1].qs["wertetyp"][0].upper() == "METER_READ"
    
@pytest.mark.usefixtures("requests_mock")
def test_history_wrong_zp(requests_mock: Mocker, caplog):
    caplog.set_level(logging.DEBUG)
    z = zaehlpunkt_response([enabled(zaehlpunkt())])[0]
    zp = z["zaehlpunkte"][0]['zaehlpunktnummer']
    customer_id = z["geschaeftspartner"]
    expect_login(requests_mock)
    expect_history(requests_mock, customer_id, zp, wrong_zp = True)
    expect_zaehlpunkte(requests_mock, [enabled(zaehlpunkt())])
    with pytest.raises(SmartmeterQueryError) as exc_info:
        smartmeter().login().historical_data()
    assert 'Returned data: ' in caplog.text
    assert 'Returned data does not match given zaehlpunkt!' == str(exc_info.value)
    
@pytest.mark.usefixtures("requests_mock")
def test_history_invalid_obis_code(requests_mock: Mocker):
    z = zaehlpunkt_response([enabled(zaehlpunkt())])[0]
    zp = z["zaehlpunkte"][0]['zaehlpunktnummer']
    customer_id = z["geschaeftspartner"]
    expect_login(requests_mock)
    expect_history(requests_mock, customer_id, zp, all_invalid_obis = True)
    expect_zaehlpunkte(requests_mock, [enabled(zaehlpunkt())])
    with pytest.raises(SmartmeterQueryError) as exc_info:
        smartmeter().login().historical_data()
    assert "No valid OBIS code found. OBIS codes in data: ['9-9:9.9.9']" == str(exc_info.value)

@pytest.mark.usefixtures("requests_mock")
def test_history_multiple_zaehlwerke_one_valid(requests_mock: Mocker):
    z = zaehlpunkt_response([enabled(zaehlpunkt())])[0]
    zp = z["zaehlpunkte"][0]['zaehlpunktnummer']
    customer_id = z["geschaeftspartner"]
    expect_login(requests_mock)
    expect_history(requests_mock, customer_id, zp, zaehlwerk_amount = 3)
    expect_zaehlpunkte(requests_mock, [enabled(zaehlpunkt())])
    hist = smartmeter().login().historical_data()
    assert 1 == len(hist['messwerte'])
    assert 'WH' == hist['einheit']
    assert '1-1:1.8.0' == hist['obisCode']
    
@pytest.mark.usefixtures("requests_mock")
def test_history_multiple_zaehlwerke_all_valid(requests_mock: Mocker, caplog):
    z = zaehlpunkt_response([enabled(zaehlpunkt())])[0]
    zp = z["zaehlpunkte"][0]['zaehlpunktnummer']
    customer_id = z["geschaeftspartner"]
    expect_login(requests_mock)
    expect_history(requests_mock, customer_id, zp, zaehlwerk_amount = 3, all_valid_obis = True)
    expect_zaehlpunkte(requests_mock, [enabled(zaehlpunkt())])
    hist = smartmeter().login().historical_data()
    assert 1 == len(hist['messwerte'])
    assert 'WH' == hist['einheit']
    assert '1-1:1.8.0' == hist['obisCode']
    assert "Multiple valid OBIS codes found: ['1-1:1.8.0', '1-1:1.9.0', '1-1:2.8.0']. Using the first one." in caplog.text

@pytest.mark.usefixtures("requests_mock")
def test_history_multiple_zaehlwerke_all_invalid(requests_mock: Mocker, caplog):
    caplog.set_level(logging.DEBUG)
    z = zaehlpunkt_response([enabled(zaehlpunkt())])[0]
    zp = z["zaehlpunkte"][0]['zaehlpunktnummer']
    customer_id = z["geschaeftspartner"]
    expect_login(requests_mock)
    expect_history(requests_mock, customer_id, zp, zaehlwerk_amount = 3, all_invalid_obis = True)
    expect_zaehlpunkte(requests_mock, [enabled(zaehlpunkt())])
    with pytest.raises(SmartmeterQueryError) as exc_info:
        smartmeter().login().historical_data()
    assert 'Returned zaehlwerke: ' in caplog.text
    assert "No valid OBIS code found. OBIS codes in data: ['9-9:9.9.9', '9-9:9.9.9', '9-9:9.9.9']" == str(exc_info.value)
    
@pytest.mark.usefixtures("requests_mock")
def test_history_empty_messwerte(requests_mock: Mocker, caplog):
    caplog.set_level(logging.DEBUG)
    z = zaehlpunkt_response([enabled(zaehlpunkt())])[0]
    zp = z["zaehlpunkte"][0]['zaehlpunktnummer']
    customer_id = z["geschaeftspartner"]
    expect_login(requests_mock)
    expect_history(requests_mock, customer_id, zp, empty_messwerte = True)
    expect_zaehlpunkte(requests_mock, [enabled(zaehlpunkt())])
    hist=smartmeter().login().historical_data()
    assert 0 == len(hist['messwerte'])
    assert 'WH' == hist['einheit']
    assert '1-1:1.8.0' == hist['obisCode']
    assert "Valid OBIS code '1-1:1.8.0' has empty or missing messwerte." in caplog.text

@pytest.mark.usefixtures("requests_mock")
def test_history_no_zaehlwerke(requests_mock: Mocker, caplog):
    caplog.set_level(logging.DEBUG)
    z = zaehlpunkt_response([enabled(zaehlpunkt())])[0]
    zp = z["zaehlpunkte"][0]['zaehlpunktnummer']
    customer_id = z["geschaeftspartner"]
    expect_login(requests_mock)
    expect_history(requests_mock, customer_id, zp, no_zaehlwerke = True)
    expect_zaehlpunkte(requests_mock, [enabled(zaehlpunkt())])
    with pytest.raises(SmartmeterQueryError) as exc_info:
        smartmeter().login().historical_data()
    assert 'Returned data: ' in caplog.text
    assert 'Returned data does not contain any zaehlwerke or is empty.' == str(exc_info.value)

@pytest.mark.usefixtures("requests_mock")
def test_history_empty_zaehlwerke(requests_mock: Mocker, caplog):
    caplog.set_level(logging.DEBUG)
    z = zaehlpunkt_response([enabled(zaehlpunkt())])[0]
    zp = z["zaehlpunkte"][0]['zaehlpunktnummer']
    customer_id = z["geschaeftspartner"]
    expect_login(requests_mock)
    expect_history(requests_mock, customer_id, zp, empty_zaehlwerke = True)
    expect_zaehlpunkte(requests_mock, [enabled(zaehlpunkt())])
    with pytest.raises(SmartmeterQueryError) as exc_info:
        smartmeter().login().historical_data()
    assert 'Returned data: ' in caplog.text
    assert 'Returned data does not contain any zaehlwerke or is empty.' == str(exc_info.value)
    
@pytest.mark.usefixtures("requests_mock")
def test_history_no_obis_code(requests_mock: Mocker, caplog):
    caplog.set_level(logging.DEBUG)
    z = zaehlpunkt_response([enabled(zaehlpunkt())])[0]
    zp = z["zaehlpunkte"][0]['zaehlpunktnummer']
    customer_id = z["geschaeftspartner"]
    expect_login(requests_mock)
    expect_history(requests_mock, customer_id, zp, no_obis_code = True)
    expect_zaehlpunkte(requests_mock, [enabled(zaehlpunkt())])
    with pytest.raises(SmartmeterQueryError) as exc_info:
        smartmeter().login().historical_data()
    assert 'Returned zaehlwerke: ' in caplog.text
    assert 'No OBIS codes found in the provided data.' == str(exc_info.value)


@pytest.mark.usefixtures("requests_mock")
def test_historical_day_consumption_prefers_consumption_obis(requests_mock: Mocker):
    z = zaehlpunkt_response([enabled(zaehlpunkt())])[0]
    zp = z["zaehlpunkte"][0]["zaehlpunktnummer"]
    customer_id = z["geschaeftspartner"]
    expect_login(requests_mock)
    expect_history(
        requests_mock,
        customer_id,
        zp,
        zaehlwerk_amount=3,
        all_valid_obis=True,
    )
    expect_zaehlpunkte(requests_mock, [enabled(zaehlpunkt())])

    hist = smartmeter().login().historical_day_consumption()

    assert hist["obisCode"] == "1-1:1.9.0"
    assert requests_mock.request_history[-1].qs["wertetyp"][0].upper() == "DAY"


@pytest.mark.usefixtures("requests_mock")
def test_historical_day_consumption_prefers_feeding_obis(requests_mock: Mocker):
    z = zaehlpunkt_response([enabled(zaehlpunkt_feeding())])[0]
    zp = z["zaehlpunkte"][0]["zaehlpunktnummer"]
    customer_id = z["geschaeftspartner"]
    expect_login(requests_mock)
    expect_history(
        requests_mock,
        customer_id,
        zp,
        zaehlwerk_amount=4,
        all_valid_obis=True,
    )
    expect_zaehlpunkte(requests_mock, [enabled(zaehlpunkt_feeding())])

    hist = smartmeter().login().historical_day_consumption()

    assert hist["obisCode"] == "1-1:2.9.0"
    assert requests_mock.request_history[-1].qs["wertetyp"][0].upper() == "DAY"


def test_find_valid_obis_data_raises_when_preferred_obis_missing():
    with pytest.raises(SmartmeterQueryError, match="No preferred OBIS code found"):
        smartmeter().find_valid_obis_data(
            [
                {
                    "obisCode": "1-1:2.8.0",
                    "einheit": "WH",
                    "messwerte": [{"messwert": 1000}],
                }
            ],
            preferred_obis_codes=["1-1:1.9.0", "1-1:1.8.0"],
        )

@pytest.mark.usefixtures("requests_mock")
def test_bewegungsdaten_quarterly_hour_consuming(requests_mock: Mocker):
    z = zaehlpunkt_response([enabled(zaehlpunkt())])[0]
    dateFrom = dt.datetime(2023, 4, 21, 00, 00, 00, 0)
    dateTo = dt.datetime(2023, 5, 1, 23, 59, 59, 999999)
    zpn = z["zaehlpunkte"][0]['zaehlpunktnummer']
    expect_login(requests_mock)
    expect_bewegungsdaten(requests_mock, z["geschaeftspartner"], zpn, dateFrom, dateTo, const.ValueType.QUARTER_HOUR, values_count=COUNT)
    expect_zaehlpunkte(requests_mock, [enabled(zaehlpunkt())])

    hist = smartmeter().login().bewegungsdaten(None, dateFrom, dateTo)

    assert 10 == len(hist['values'])

@pytest.mark.usefixtures("requests_mock")
def test_bewegungsdaten_daily_consuming(requests_mock: Mocker):
    z = zaehlpunkt_response([enabled(zaehlpunkt())])[0]
    dateFrom = dt.datetime(2023, 4, 21, 00, 00, 00, 0)
    dateTo = dt.datetime(2023, 5, 1, 23, 59, 59, 999999)
    zpn = z["zaehlpunkte"][0]['zaehlpunktnummer']
    expect_login(requests_mock)
    expect_bewegungsdaten(requests_mock, z["geschaeftspartner"], zpn, dateFrom, dateTo, const.ValueType.DAY, values_count=COUNT)
    expect_zaehlpunkte(requests_mock, [enabled(zaehlpunkt())])

    hist = smartmeter().login().bewegungsdaten(None, dateFrom, dateTo, const.ValueType.DAY)

    assert 10 == len(hist['values'])
  
@pytest.mark.usefixtures("requests_mock")
def test_bewegungsdaten_quarterly_hour_feeding(requests_mock: Mocker):
    z = zaehlpunkt_response([enabled(zaehlpunkt_feeding())])[0]
    dateFrom = dt.datetime(2023, 4, 21, 00, 00, 00, 0)
    dateTo = dt.datetime(2023, 5, 1, 23, 59, 59, 999999)
    zpn = z["zaehlpunkte"][0]['zaehlpunktnummer']
    expect_login(requests_mock)
    expect_bewegungsdaten(requests_mock, z["geschaeftspartner"], zpn, dateFrom, dateTo, const.ValueType.QUARTER_HOUR, const.AnlagenType.FEEDING, values_count=COUNT)
    expect_zaehlpunkte(requests_mock, [enabled(zaehlpunkt_feeding())])

    hist = smartmeter().login().bewegungsdaten(None, dateFrom, dateTo)

    assert 10 == len(hist['values'])
    
@pytest.mark.usefixtures("requests_mock")
def test_bewegungsdaten_daily_feeding(requests_mock: Mocker):
    z = zaehlpunkt_response([enabled(zaehlpunkt_feeding())])[0]
    dateFrom = dt.datetime(2023, 4, 21, 00, 00, 00, 0)
    dateTo = dt.datetime(2023, 5, 1, 23, 59, 59, 999999)
    zpn = z["zaehlpunkte"][0]['zaehlpunktnummer']
    expect_login(requests_mock)
    expect_bewegungsdaten(requests_mock, z["geschaeftspartner"], zpn, dateFrom, dateTo, const.ValueType.DAY, const.AnlagenType.FEEDING, values_count=COUNT)
    expect_zaehlpunkte(requests_mock, [enabled(zaehlpunkt_feeding())])

    hist = smartmeter().login().bewegungsdaten(None, dateFrom, dateTo, const.ValueType.DAY)

    assert 10 == len(hist['values'])
    
@pytest.mark.usefixtures("requests_mock")
def test_bewegungsdaten_no_dates_given(requests_mock: Mocker):
    z = zaehlpunkt_response([enabled(zaehlpunkt())])[0]
    dateTo = dt.date.today()
    dateFrom = dateTo - relativedelta(years=3)
    zpn = z["zaehlpunkte"][0]['zaehlpunktnummer']
    expect_login(requests_mock)
    expect_bewegungsdaten(requests_mock, z["geschaeftspartner"], zpn, dateFrom, dateTo, values_count=COUNT)
    expect_zaehlpunkte(requests_mock, [enabled(zaehlpunkt())])

    hist = smartmeter().login().bewegungsdaten()

    assert 10 == len(hist['values'])
    
@pytest.mark.usefixtures("requests_mock")
def test_bewegungsdaten_wrong_zp(requests_mock: Mocker):
    z = zaehlpunkt_response([enabled(zaehlpunkt())])[0]
    dateFrom = dt.datetime(2023, 4, 21, 00, 00, 00, 0)
    dateTo = dt.datetime(2023, 5, 1, 23, 59, 59, 999999)
    zpn = z["zaehlpunkte"][0]['zaehlpunktnummer']
    expect_login(requests_mock)
    expect_bewegungsdaten(requests_mock, z["geschaeftspartner"], zpn, dateFrom, dateTo, wrong_zp = True, values_count=COUNT)
    expect_zaehlpunkte(requests_mock, [enabled(zaehlpunkt())])
    with pytest.raises(SmartmeterQueryError) as exc_info:
        smartmeter().login().bewegungsdaten(None, dateFrom, dateTo)
    assert 'Returned data does not match given zaehlpunkt!' == str(exc_info.value)


@pytest.mark.usefixtures("requests_mock")
def test_verbrauch_raw(requests_mock: Mocker):

    dateFrom = dt.datetime(2023, 4, 21, 22, 00, 00)
    zp = "AT000000001234567890"
    customer_id = "123456789"
    valid_verbrauch_raw_response = verbrauch_raw_response()
    expect_login(requests_mock)
    expect_history(requests_mock, customer_id, enabled(zaehlpunkt())['zaehlpunktnummer'])
    expect_zaehlpunkte(requests_mock, [enabled(zaehlpunkt())])
    expect_verbrauch(requests_mock, customer_id, zp, dateFrom, valid_verbrauch_raw_response)

    verbrauch = smartmeter().login().verbrauch(customer_id, zp, dateFrom)

    assert 7 == len(verbrauch['values'])
