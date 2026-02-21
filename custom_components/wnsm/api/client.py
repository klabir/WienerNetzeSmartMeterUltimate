"""Contains the Smartmeter API Client."""
import json
import logging
import shutil
from datetime import datetime, timedelta, date
from urllib import parse
from typing import List, Dict, Any

import requests
from dateutil.relativedelta import relativedelta
from lxml import html

import base64
import hashlib
import os
import copy
import re
import random
import time
import uuid

from . import constants as const
from .errors import (
    SmartmeterError,
    SmartmeterConnectionError,
    SmartmeterLoginError,
    SmartmeterQueryError,
)

logger = logging.getLogger(__name__)


class Smartmeter:
    """Smartmeter client."""

    def __init__(
        self,
        username,
        password,
        input_code_verifier=None,
        enable_raw_api_response_write: bool = False,
        log_scope: str = "default",
    ):
        """Access the Smartmeter API.

        Args:
            username (str): Username used for API Login.
            password (str): Password used for API Login.
        """
        self.username = username
        self.password = password
        self.session = requests.Session()
        self._access_token = None
        self._refresh_token = None
        self._api_gateway_token = None
        self._access_token_expiration = None
        self._refresh_token_expiration = None
        self._api_gateway_b2b_token = None
        
        self._code_verifier = None
        if input_code_verifier is not None:
            if self.is_valid_code_verifier(input_code_verifier):
                self._code_verifier = input_code_verifier
        
        self._code_challenge = None
        self._local_login_args = None
        self._enable_raw_api_response_write = bool(enable_raw_api_response_write)
        self._raw_api_scope = self._sanitize_filename(log_scope) or "default"
        self._raw_api_response_root = None
        self._raw_api_response_dir = None
        self._raw_api_response_root_candidates = [
            "/config/tmp/wnsm_api_calls",
            "/homeassistant/tmp/wnsm_api_calls",
            "/tmp/wnsm_api_calls",
        ]
        self._raw_api_log_prepared = False
        self._raw_api_log_prepare_error = None
        self._raw_api_last_write_error = None
        self._recent_api_calls = []
        self._max_recent_api_calls = 20
        if self._enable_raw_api_response_write:
            self._prepare_raw_api_response_dir()

    def reset(self):
        self.session = requests.Session()
        self._access_token = None
        self._refresh_token = None
        self._api_gateway_token = None
        self._access_token_expiration = None
        self._refresh_token_expiration = None
        self._api_gateway_b2b_token = None
        self._code_verifier = None
        self._code_challenge = None
        self._local_login_args = None
        self._raw_api_log_prepared = False
        self._raw_api_log_prepare_error = None
        self._raw_api_last_write_error = None
        self._recent_api_calls = []

    def is_login_expired(self):
        return self._access_token_expiration is not None and datetime.now() >= self._access_token_expiration

    def is_refresh_expired(self):
        return self._refresh_token_expiration is not None and datetime.now() >= self._refresh_token_expiration

    def is_logged_in(self):
        return self._access_token is not None and not self.is_login_expired()

    def generate_code_verifier(self):
        """
        generate a code verifier
        """
        return base64.urlsafe_b64encode(os.urandom(32)).decode('utf-8').rstrip('=')
    
    def generate_code_challenge(self, code_verifier):
        """
        generate a code challenge from the code verifier
        """
        code_challenge = hashlib.sha256(code_verifier.encode('utf-8')).digest()
        return base64.urlsafe_b64encode(code_challenge).decode('utf-8').rstrip('=')

    def is_valid_code_verifier(self, code_verifier):
        if not (43 <= len(code_verifier) <= 128):
            return False

        pattern = r'^[A-Za-z0-9\-._~]+$'
        if not re.match(pattern, code_verifier):
            return False
        
        return True
    
    def load_login_page(self):
        """
        loads login page and extracts encoded login url
        """
        
        #generate a code verifier, which serves as a secure random value
        if not hasattr(self, '_code_verifier') or self._code_verifier is None:
           #only generate if it does not exist 
           self._code_verifier = self.generate_code_verifier()
        
        #generate a code challenge from the code verifier to enhance security
        self._code_challenge = self.generate_code_challenge(self._code_verifier)
        
        #copy const.LOGIN_ARGS
        self._local_login_args = copy.deepcopy(const.LOGIN_ARGS)
        
        #add code_challenge in self._local_login_args
        self._local_login_args["code_challenge"] = self._code_challenge
        
        login_url = const.AUTH_URL + "auth?" + parse.urlencode(self._local_login_args)
        try:
            result = self.session.get(login_url, timeout=60.0)
        except Exception as exception:
            raise SmartmeterConnectionError("Could not load login page") from exception
        if result.status_code != 200:
            raise SmartmeterConnectionError(
                f"Could not load login page. Error: {result.content}"
            )
        return self._extract_first_form_action(
            result.content,
            "No form found on the login page.",
            result.url if result is not None else login_url,
        )

    def credentials_login(self, url):
        """
        login with credentials provided the login url
        """
        try:
            result = self.session.post(
                url,
                data={
                    "username": self.username,
                    "login": " "
                },
                allow_redirects=False,
                timeout=60.0,
            )
            action = self._extract_first_form_action(
                result.content,
                "Could not login with credentials",
                result.url if result is not None else url,
            )

            result = self.session.post(
                action,
                data={
                    "username": self.username,
                    "password": self.password,
                },
                allow_redirects=False,
                timeout=60.0,
            )
        except Exception as exception:
            raise SmartmeterConnectionError(
                "Could not login with credentials"
            ) from exception

        if "Location" not in result.headers:
            raise SmartmeterLoginError("Login failed. Check username/password.")
        location = result.headers["Location"]

        parsed_url = parse.urlparse(location)

        fragment_dict = dict(
            [
                x.split("=")
                for x in parsed_url.fragment.split("&")
                if len(x.split("=")) == 2
            ]
        )
        if "code" not in fragment_dict:
            raise SmartmeterLoginError(
                "Login failed. Could not extract 'code' from 'Location'"
            )

        code = fragment_dict["code"]
        return code

    def load_tokens(self, code):
        """
        Provided the totp code loads access and refresh token
        """
        try:
            result = self.session.post(
                const.AUTH_URL + "token",
                data=const.build_access_token_args(code=code , code_verifier=self._code_verifier),
                timeout=60.0,
            )
        except Exception as exception:
            raise SmartmeterConnectionError(
                "Could not obtain access token"
            ) from exception

        if result.status_code != 200:
            raise SmartmeterConnectionError(
                f"Could not obtain access token: {result.content}"
            )
        tokens = result.json()
        token_type = tokens.get("token_type")
        if token_type != "Bearer":
            raise SmartmeterLoginError(
                f"Bearer token required, but got {token_type!r}"
            )
        return tokens

    def refresh_tokens(self):
        """Refresh access token with refresh token when possible."""
        if self._refresh_token is None or self.is_refresh_expired():
            raise SmartmeterConnectionError("Refresh Token is not valid anymore, please re-log!")
        try:
            result = self.session.post(
                const.AUTH_URL + "token",
                data={
                    "grant_type": "refresh_token",
                    "client_id": "wn-smartmeter",
                    "redirect_uri": const.REDIRECT_URI,
                    "refresh_token": self._refresh_token,
                },
                timeout=60.0,
            )
        except Exception as exception:
            raise SmartmeterConnectionError("Could not refresh access token") from exception

        if result.status_code != 200:
            raise SmartmeterConnectionError(
                f"Could not refresh access token: {result.content}"
            )
        tokens = result.json()
        token_type = tokens.get("token_type")
        if token_type != "Bearer":
            raise SmartmeterLoginError(
                f"Bearer token required, but got {token_type!r}"
            )

        now = datetime.now()
        self._access_token = tokens["access_token"]
        if "refresh_token" in tokens and tokens["refresh_token"] is not None:
            self._refresh_token = tokens["refresh_token"]
        self._access_token_expiration = now + timedelta(seconds=tokens["expires_in"])
        refresh_expires_in = tokens.get("refresh_expires_in")
        if refresh_expires_in is not None:
            self._refresh_token_expiration = now + timedelta(seconds=refresh_expires_in)
        return tokens

    def login(self):
        """
        login with credentials specified in ctor
        """
        if self._access_token is not None and self.is_login_expired():
            try:
                self.refresh_tokens()
            except SmartmeterError:
                self.reset()
        if not self.is_logged_in():
            url = self.load_login_page()
            code = self.credentials_login(url)
            tokens = self.load_tokens(code)
            self._access_token = tokens["access_token"]
            self._refresh_token = tokens["refresh_token"]
            now = datetime.now()
            self._access_token_expiration = now + timedelta(seconds=tokens["expires_in"])
            self._refresh_token_expiration = now + timedelta(
                seconds=tokens["refresh_expires_in"]
            )

            logger.debug("Access Token valid until %s" % self._access_token_expiration)

            self._api_gateway_token, self._api_gateway_b2b_token = self._get_api_key(
                self._access_token
            )
        return self

    def _access_valid_or_raise(self):
        """Checks if the access token is still valid or raises an exception"""
        if self._access_token is None or self._access_token_expiration is None:
            raise SmartmeterConnectionError(
                "Access Token is not valid anymore, please re-log!"
            )
        if datetime.now() >= self._access_token_expiration:
            self.refresh_tokens()

    def _raise_for_response(self, endpoint: str, status_code: int, error_data: Any):
        if status_code < 400:
            return
        message = f"API request failed for endpoint '{endpoint}' with status {status_code}: {error_data}"
        if status_code in (401, 403):
            raise SmartmeterLoginError(message)
        raise SmartmeterConnectionError(message)

    @staticmethod
    def _extract_first_form_action(content, no_form_error, base_url: str | None = None):
        tree = html.fromstring(content)
        forms = tree.xpath("(//form/@action)")
        if not forms:
            raise SmartmeterConnectionError(no_form_error)
        action = forms[0]
        if base_url is not None:
            return parse.urljoin(base_url, action)
        return action

    @staticmethod
    def _sanitize_filename(value: str) -> str:
        return re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("_")

    @staticmethod
    def _redact_headers(headers: dict) -> dict:
        redacted = dict(headers)
        if "Authorization" in redacted:
            redacted["Authorization"] = "Bearer ***"
        if "X-Gateway-APIKey" in redacted:
            redacted["X-Gateway-APIKey"] = "***"
        return redacted

    def _write_raw_api_response(self, payload: dict, endpoint: str, method: str) -> str | None:
        if not self._enable_raw_api_response_write:
            return None
        self._raw_api_last_write_error = None
        try:
            self._prepare_raw_api_response_dir()
            if not self._raw_api_log_prepared or self._raw_api_response_dir is None:
                self._raw_api_last_write_error = self._raw_api_log_prepare_error or "Raw API log directory is not available."
                return None
            zaehlpunkt = self._extract_zaehlpunkt_for_log(
                endpoint,
                payload.get("query"),
                payload.get("request_body"),
            )
            sub_dir = os.path.join(
                self._raw_api_response_dir,
                self._sanitize_filename(zaehlpunkt),
            )
            os.makedirs(sub_dir, exist_ok=True)
            safe_endpoint = self._sanitize_filename(endpoint)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            filename = f"{timestamp}_{method.lower()}_{safe_endpoint}.json"
            path = os.path.join(sub_dir, filename)
            with open(path, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, indent=2, ensure_ascii=False)
            return path
        except Exception as exception:  # pylint: disable=broad-except
            self._raw_api_last_write_error = str(exception)
            logger.warning(
                "Could not write raw API response file for endpoint '%s': %s",
                endpoint,
                exception,
            )
            return None

    def _prepare_raw_api_response_dir(self) -> None:
        if self._raw_api_log_prepared:
            return
        self._raw_api_log_prepare_error = None
        writable_roots: list[str] = []
        for root in self._raw_api_response_root_candidates:
            try:
                os.makedirs(root, exist_ok=True)
                probe_file = os.path.join(root, f".probe_{uuid.uuid4().hex}")
                with open(probe_file, "w", encoding="utf-8") as handle:
                    handle.write("ok")
                os.remove(probe_file)
                writable_roots.append(root)
            except Exception as exception:  # pylint: disable=broad-except
                self._raw_api_log_prepare_error = f"{root}: {exception}"

        if not writable_roots:
            self._raw_api_log_prepared = False
            self._raw_api_response_root = None
            self._raw_api_response_dir = None
            logger.error(
                "Raw API response logging is enabled but no writable directory is available. Last error: %s",
                self._raw_api_log_prepare_error,
            )
            return

        for root in writable_roots:
            try:
                for name in os.listdir(root):
                    path = os.path.join(root, name)
                    if os.path.isdir(path):
                        shutil.rmtree(path)
                    else:
                        os.remove(path)
                leftovers = os.listdir(root)
                if leftovers:
                    raise OSError(
                        f"Cleanup of '{root}' incomplete, remaining entries: {leftovers}"
                    )
            except Exception as exception:  # pylint: disable=broad-except
                self._raw_api_log_prepare_error = f"{root}: {exception}"
                logger.warning("Raw API cleanup failed for '%s': %s", root, exception)

        selected_root = writable_roots[0]
        try:
            self._raw_api_response_root = selected_root
            self._raw_api_response_dir = os.path.join(selected_root, self._raw_api_scope)
            os.makedirs(self._raw_api_response_dir, exist_ok=True)
            self._raw_api_log_prepared = True
            self._raw_api_log_prepare_error = None
            return
        except Exception as exception:  # pylint: disable=broad-except
            self._raw_api_log_prepare_error = f"{selected_root}: {exception}"

        self._raw_api_log_prepared = False
        self._raw_api_response_root = None
        self._raw_api_response_dir = None
        logger.error(
            "Raw API response logging is enabled but no writable directory is available. Last error: %s",
            self._raw_api_log_prepare_error,
        )

    @staticmethod
    def _extract_zaehlpunkt_for_log(
        endpoint: str,
        query: dict | None,
        request_body: dict | None,
    ) -> str:
        if isinstance(query, dict) and query.get("zaehlpunkt"):
            return str(query["zaehlpunkt"])
        if isinstance(request_body, dict) and request_body.get("zaehlpunkt"):
            return str(request_body["zaehlpunkt"])

        patterns = [
            r"messdaten/[^/]+/([^/]+)/",
            r"zaehlpunkte/[^/]+/([^/]+)/",
        ]
        for pattern in patterns:
            match = re.search(pattern, endpoint)
            if match:
                return str(match.group(1))
        return "general"

    def _record_api_call(
        self,
        method: str,
        endpoint: str,
        url: str,
        query: dict | None,
        request_body: dict | None,
        request_headers: dict,
        response_status: int | None,
        response_body: Any,
    ) -> None:
        payload = {
            "timestamp": datetime.now().isoformat(),
            "method": method,
            "endpoint": endpoint,
            "url": url,
            "query": query,
            "request_headers": self._redact_headers(request_headers),
            "request_body": request_body,
            "response_status": response_status,
            "response_body": response_body,
        }
        file_path = self._write_raw_api_response(payload, endpoint, method)
        summary = {
            "timestamp": payload["timestamp"],
            "method": method,
            "endpoint": endpoint,
            "url": url,
            "response_status": response_status,
            "file_path": file_path,
        }
        self._recent_api_calls.append(summary)
        if len(self._recent_api_calls) > self._max_recent_api_calls:
            self._recent_api_calls = self._recent_api_calls[-self._max_recent_api_calls :]

    def get_recent_api_calls(self) -> list[dict]:
        return list(self._recent_api_calls)

    def get_raw_api_logging_status(self) -> dict:
        return {
            "enabled": self._enable_raw_api_response_write,
            "prepared": self._raw_api_log_prepared,
            "root": self._raw_api_response_root,
            "directory": self._raw_api_response_dir,
            "prepare_error": self._raw_api_log_prepare_error,
            "last_write_error": self._raw_api_last_write_error,
        }

    def _get_api_key(self, token):
        self._access_valid_or_raise()

        headers = {"Authorization": f"Bearer {token}"}
        try:
            result = self.session.get(
                const.API_CONFIG_URL, headers=headers, timeout=60.0
            ).json()
        except Exception as exception:
            raise SmartmeterConnectionError("Could not obtain API key") from exception

        find_keys = ["b2cApiKey", "b2bApiKey"]
        for key in find_keys:
            if key not in result:
                raise SmartmeterConnectionError(f"{key} not found in response!")

        # The b2bApiUrl and b2cApiUrl can also be gathered from the configuration
        # TODO: reduce code duplication...
        if "b2cApiUrl" in result and result["b2cApiUrl"] != const.API_URL:
            const.API_URL = result["b2cApiUrl"]
            logger.warning("The b2cApiUrl has changed to %s! Update API_URL!", const.API_URL)
        if "b2bApiUrl" in result and result["b2bApiUrl"] != const.API_URL_B2B:
            const.API_URL_B2B = result["b2bApiUrl"]
            logger.warning("The b2bApiUrl has changed to %s! Update API_URL_B2B!", const.API_URL_B2B)

        return (result[key] for key in find_keys)

    @staticmethod
    def _dt_string(datetime_string):
        return datetime_string.strftime(const.API_DATE_FORMAT)[:-3] + "Z"

    def _call_api(
        self,
        endpoint,
        base_url=None,
        method="GET",
        data=None,
        query=None,
        return_response=False,
        timeout=60.0,
        extra_headers=None,
    ):
        self._access_valid_or_raise()

        if base_url is None:
            base_url = const.API_URL
        url = parse.urljoin(base_url, endpoint)

        if query:
            url += ("?" if "?" not in endpoint else "&") + parse.urlencode(query)

        headers = {
            "Authorization": f"Bearer {self._access_token}",
        }

        # For API calls to B2C or B2B, we need to add the Gateway-APIKey:
        # TODO: This may be prone to errors if URLs are compared like this.
        #       The Strings has to be exactly the same, but that may not be the case,
        #       even though the URLs are the same.
        if base_url == const.API_URL:
            headers["X-Gateway-APIKey"] = self._api_gateway_token
        elif base_url == const.API_URL_B2B:
            headers["X-Gateway-APIKey"] = self._api_gateway_b2b_token

        if extra_headers:
            headers.update(extra_headers)

        if data:
            headers["Content-Type"] = "application/json"

        method_u = method.upper()
        can_retry = method_u == "GET"
        max_attempts = 3 if can_retry else 1
        api_key_refreshed = False
        transient_status = {429, 500, 502, 503, 504}
        response = None
        response_json = None
        response_text = None

        for attempt in range(1, max_attempts + 1):
            try:
                response = self.session.request(
                    method, url, headers=headers, json=data, timeout=timeout
                )
            except requests.RequestException as exception:
                self._record_api_call(
                    method=method,
                    endpoint=endpoint,
                    url=url,
                    query=query,
                    request_body=data,
                    request_headers=headers,
                    response_status=None,
                    response_body=f"RequestException: {exception}",
                )
                if can_retry and attempt < max_attempts:
                    time.sleep(random.uniform(0.05, 0.2) * attempt)
                    continue
                raise SmartmeterConnectionError(
                    f"API request failed for endpoint '{endpoint}': {exception}"
                ) from exception

            response_json = None
            response_text = None
            try:
                response_json = response.json()
            except ValueError:
                response_text = response.text

            logger.debug(
                "\nAPI Request: %s\n%s\n\nAPI Response: %s",
                url,
                "" if data is None else "body: " + json.dumps(data, indent=2),
                json.dumps(response_json, indent=2) if response_json is not None else response_text,
            )

            self._record_api_call(
                method=method,
                endpoint=endpoint,
                url=url,
                query=query,
                request_body=data,
                request_headers=headers,
                response_status=response.status_code if response is not None else None,
                response_body=response_json if response_json is not None else response_text,
            )

            if (
                response.status_code in (401, 403)
                and not api_key_refreshed
                and can_retry
                and attempt < max_attempts
            ):
                try:
                    self._api_gateway_token, self._api_gateway_b2b_token = self._get_api_key(
                        self._access_token
                    )
                    api_key_refreshed = True
                    if base_url == const.API_URL:
                        headers["X-Gateway-APIKey"] = self._api_gateway_token
                    elif base_url == const.API_URL_B2B:
                        headers["X-Gateway-APIKey"] = self._api_gateway_b2b_token
                    time.sleep(random.uniform(0.05, 0.2) * attempt)
                    continue
                except SmartmeterError:
                    pass

            if (
                can_retry
                and response.status_code in transient_status
                and attempt < max_attempts
            ):
                time.sleep(random.uniform(0.05, 0.2) * attempt)
                continue

            break

        self._raise_for_response(
            endpoint,
            response.status_code if response is not None else 0,
            response_json if response_json is not None else response_text,
        )

        if return_response:
            return response

        if response_json is not None:
            return response_json
        raise SmartmeterConnectionError(
            f"Could not parse JSON response for endpoint '{endpoint}'"
        )

    def get_zaehlpunkt(self, zaehlpunkt: str = None) -> tuple[str, str, str]:
        contracts = self.zaehlpunkte()
        if zaehlpunkt is None:
            customer_id = contracts[0]["geschaeftspartner"]
            zp = contracts[0]["zaehlpunkte"][0]["zaehlpunktnummer"]
            anlagetype = contracts[0]["zaehlpunkte"][0]["anlage"]["typ"]
        else:
            customer_id = zp = anlagetype = None
            for contract in contracts:
                zp_details = [z for z in contract["zaehlpunkte"] if z["zaehlpunktnummer"] == zaehlpunkt]
                if len(zp_details) > 0:
                    anlagetype = zp_details[0]["anlage"]["typ"]
                    zp = zp_details[0]["zaehlpunktnummer"]
                    customer_id = contract["geschaeftspartner"]
        return customer_id, zp, const.AnlagenType.from_str(anlagetype)

    def zaehlpunkte(self):
        """Returns zaehlpunkte for currently logged in user."""
        return self._call_api("zaehlpunkte")

    def consumptions(self):
        """Returns response from 'consumptions' endpoint."""
        return self._call_api("zaehlpunkt/consumptions")

    def base_information(self):
        """Returns response from 'baseInformation' endpoint."""
        return self._call_api("zaehlpunkt/baseInformation")

    def meter_readings(self):
        """Returns response from 'meterReadings' endpoint."""
        return self._call_api("zaehlpunkt/meterReadings")

    def verbrauch(
        self,
        customer_id: str,
        zaehlpunkt: str,
        date_from: datetime,
        resolution: const.Resolution = const.Resolution.HOUR
    ):
        """Returns energy usage.

        This returns hourly or quarter hour consumptions for a single day,
        i.e., for 24 hours after the given date_from.

        Args:
            customer_id (str): Customer ID returned by zaehlpunkt call ("geschaeftspartner")
            zaehlpunkt (str, optional): id for desired smartmeter.
                If None, check for first meter in user profile.
            date_from (datetime): Start date for energy usage request
            date_to (datetime, optional): End date for energy usage request.
                Defaults to datetime.now()
            resolution (const.Resolution, optional): Specify either 1h or 15min resolution
        Returns:
            dict: JSON response of api call to
                'messdaten/CUSTOMER_ID/ZAEHLPUNKT/verbrauchRaw'
        """
        if zaehlpunkt is None or customer_id is None:
            customer_id, zaehlpunkt, anlagetype = self.get_zaehlpunkt()
        endpoint = f"messdaten/{customer_id}/{zaehlpunkt}/verbrauch"
        query = const.build_verbrauchs_args(
            # This one does not have a dateTo...
            dateFrom=self._dt_string(date_from),
            dayViewResolution=resolution.value
        )
        return self._call_api(endpoint, query=query)

    def verbrauchRaw(
        self,
        customer_id: str,
        zaehlpunkt: str,
        date_from: datetime,
        date_to: datetime = None,
    ):
        """Returns energy usage.
        This can be used to query the daily consumption for a long period of time,
        for example several months or a week.

        Note: The minimal resolution is a single day.
        For hourly consumptions use `verbrauch`.

        Args:
            customer_id (str): Customer ID returned by zaehlpunkt call ("geschaeftspartner")
            zaehlpunkt (str, optional): id for desired smartmeter.
                If None, check for first meter in user profile.
            date_from (datetime): Start date for energy usage request
            date_to (datetime, optional): End date for energy usage request.
                Defaults to datetime.now()
        Returns:
            dict: JSON response of api call to
                'messdaten/CUSTOMER_ID/ZAEHLPUNKT/verbrauchRaw'
        """
        if date_to is None:
            date_to = datetime.now()
        if zaehlpunkt is None or customer_id is None:
            customer_id, zaehlpunkt, anlagetype = self.get_zaehlpunkt()
        endpoint = f"messdaten/{customer_id}/{zaehlpunkt}/verbrauchRaw"
        query = dict(
            # These are the only three fields that are used for that endpoint:
            dateFrom=self._dt_string(date_from),
            dateTo=self._dt_string(date_to),
            granularity="DAY",
        )
        return self._call_api(endpoint, query=query)

    def profil(self):
        """Returns profile of a logged-in user.

        Returns:
            dict: JSON response of api call to 'user/profile'
        """
        return self._call_api("user/profile", const.API_URL_ALT)

    def ereignisse(
        self, date_from: datetime, date_to: datetime = None, zaehlpunkt=None
    ):
        """Returns events between date_from and date_to of a specific smart meter.
        Args:
            date_from (datetime.datetime): Starting date for request
            date_to (datetime.datetime, optional): Ending date for request.
                Defaults to datetime.datetime.now().
            zaehlpunkt (str, optional): id for desired smart meter.
                If is None check for first meter in user profile.
        Returns:
            dict: JSON response of api call to 'user/ereignisse'
        """
        if date_to is None:
            date_to = datetime.now()
        if zaehlpunkt is None:
            customer_id, zaehlpunkt, anlagetype = self.get_zaehlpunkt()
        query = {
            "zaehlpunkt": zaehlpunkt,
            "dateFrom": self._dt_string(date_from),
            "dateUntil": self._dt_string(date_to),
        }
        return self._call_api("user/ereignisse", const.API_URL_ALT, query=query)

    def create_ereignis(self, zaehlpunkt, name, date_from, date_to=None):
        """Creates new event.
        Args:
            zaehlpunkt (str): Id for desired smartmeter.
                If None, check for first meter in user profile
            name (str): Event name
            date_from (datetime.datetime): (Starting) date for request
            date_to (datetime.datetime, optional): Ending date for request.
        Returns:
            dict: JSON response of api call to 'user/ereignis'
        """
        if date_to is None:
            dto = None
            typ = "ZEITPUNKT"
        else:
            dto = self._dt_string(date_to)
            typ = "ZEITSPANNE"

        data = {
            "endAt": dto,
            "name": name,
            "startAt": self._dt_string(date_from),
            "typ": typ,
            "zaehlpunkt": zaehlpunkt,
        }

        return self._call_api("user/ereignis", data=data, method="POST")

    def delete_ereignis(self, ereignis_id):
        """Deletes ereignis."""
        return self._call_api(f"user/ereignis/{ereignis_id}", method="DELETE")

    def find_valid_obis_data(self, zaehlwerke: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Find and validate data with valid OBIS codes from a list of zaehlwerke.
        """
        
        # Check if any OBIS codes exist
        all_obis_codes = [zaehlwerk.get("obisCode") for zaehlwerk in zaehlwerke]
        if not any(all_obis_codes):
            logger.debug("Returned zaehlwerke: %s", zaehlwerke)
            raise SmartmeterQueryError("No OBIS codes found in the provided data.")
        
        # Filter data for valid OBIS codes
        valid_data = [
            zaehlwerk for zaehlwerk in zaehlwerke
            if zaehlwerk.get("obisCode") in const.VALID_OBIS_CODES
        ]
        
        if not valid_data:
            logger.debug("Returned zaehlwerke: %s", zaehlwerke)
            raise SmartmeterQueryError(f"No valid OBIS code found. OBIS codes in data: {all_obis_codes}")
        
        # Check for empty or missing messwerte
        for zaehlwerk in valid_data:
            if not zaehlwerk.get("messwerte"):
                obis = zaehlwerk.get("obisCode")
                logger.debug(f"Valid OBIS code '{obis}' has empty or missing messwerte. Data is probably not available yet.")
                
        # Log a warning if multiple valid OBIS codes are found        
        if len(valid_data) > 1:
            found_valid_obis = [zaehlwerk["obisCode"] for zaehlwerk in valid_data]
            logger.warning(f"Multiple valid OBIS codes found: {found_valid_obis}. Using the first one.")

        return valid_data[0]

    def historical_data(
        self,
        zaehlpunktnummer: str = None,
        date_from: date = None,
        date_until: date = None,
        valuetype: const.ValueType = const.ValueType.METER_READ
    ):
        """
        Query historical data in a batch
        If no arguments are given, a span of three year is queried (same day as today but from current year - 3).
        If date_from is not given but date_until, again a three year span is assumed.
        """
        # Resolve Zaehlpunkt
        if zaehlpunktnummer is None:
            customer_id, zaehlpunkt, anlagetype = self.get_zaehlpunkt()
        else:
            customer_id, zaehlpunkt, anlagetype = self.get_zaehlpunkt(zaehlpunktnummer)

        # Set date range defaults
        if date_until is None:
            date_until = date.today()
            
        if date_from is None:
            date_from = date_until - relativedelta(years=3)

        # Query parameters
        query = {
            "datumVon": date_from.strftime("%Y-%m-%d"),
            "datumBis": date_until.strftime("%Y-%m-%d"),
            "wertetyp": valuetype.value,
        }
        
        extra = {
            # For this API Call, requesting json is important!
            "Accept": "application/json"
        }

        # API Call
        data = self._call_api(
            f"zaehlpunkte/{customer_id}/{zaehlpunkt}/messwerte",
            base_url=const.API_URL_B2B,
            query=query,
            extra_headers=extra,
        )

        # Sanity check: Validate returned zaehlpunkt
        if data.get("zaehlpunkt") != zaehlpunkt:
            logger.debug("Returned data: %s", data)
            raise SmartmeterQueryError("Returned data does not match given zaehlpunkt!")

        # Validate and extract valid OBIS data
        zaehlwerke = data.get("zaehlwerke")
        if not zaehlwerke:
            logger.debug("Returned data: %s", data)
            raise SmartmeterQueryError("Returned data does not contain any zaehlwerke or is empty.")

        valid_obis_data = self.find_valid_obis_data(zaehlwerke)
        return valid_obis_data

    def bewegungsdaten(
        self,
        zaehlpunktnummer: str = None,
        date_from: date = None,
        date_until: date = None,
        valuetype: const.ValueType = const.ValueType.QUARTER_HOUR,
        aggregat: str = None,
    ):
        """
        Query historical data in a batch
        If no arguments are given, a span of three year is queried (same day as today but from current year - 3).
        If date_from is not given but date_until, again a three year span is assumed.
        """
        customer_id, zaehlpunkt, anlagetype = self.get_zaehlpunkt(zaehlpunktnummer)

        if anlagetype == const.AnlagenType.FEEDING:
            if valuetype == const.ValueType.DAY:
                rolle = const.RoleType.DAILY_FEEDING.value
            else:
                rolle = const.RoleType.QUARTER_HOURLY_FEEDING.value
        else:
            if valuetype == const.ValueType.DAY:
                rolle = const.RoleType.DAILY_CONSUMING.value
            else:
                rolle = const.RoleType.QUARTER_HOURLY_CONSUMING.value

        if date_until is None:
            date_until = date.today()

        if date_from is None:
            date_from = date_until - relativedelta(years=3)

        query = {
            "geschaeftspartner": customer_id,
            "zaehlpunktnummer": zaehlpunkt,
            "rolle": rolle,
            "zeitpunktVon": date_from.strftime("%Y-%m-%dT%H:%M:00.000Z"), # we catch up from the exact date of the last import to compensate for time shift
            "zeitpunktBis": date_until.strftime("%Y-%m-%dT23:59:59.999Z"),
            "aggregat": aggregat or "NONE"
        }

        extra = {
            # For this API Call, requesting json is important!
            "Accept": "application/json"
        }

        data = self._call_api(
            f"user/messwerte/bewegungsdaten",
            base_url=const.API_URL_ALT,
            query=query,
            extra_headers=extra,
        )
        if data["descriptor"]["zaehlpunktnummer"] != zaehlpunkt:
            raise SmartmeterQueryError("Returned data does not match given zaehlpunkt!")
        return data
