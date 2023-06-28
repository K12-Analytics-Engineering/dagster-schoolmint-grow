# parts from https://github.com/TEAMSchools/whetstone
from logging import Logger
from typing import Dict

import requests
from dagster import ConfigurableResource, get_dagster_logger
from oauthlib.oauth2 import BackendApplicationClient
from pydantic import PrivateAttr
from requests_oauthlib import OAuth2Session
from tenacity import retry, stop_after_attempt, wait_exponential


class SchoolMintGrowApiClient(ConfigurableResource):
    """Class for interacting with the SchoolMint Grow API"""

    api_key: str
    api_secret: str

    _log: Logger = PrivateAttr()
    _access_token: str = PrivateAttr()

    def setup_for_execution(self, context) -> None:
        self._log = get_dagster_logger()
        # fetch access token
        client = BackendApplicationClient(client_id=self.api_key)
        oauth = OAuth2Session(client=client)
        response = oauth.fetch_token(
            token_url="https://api.whetstoneeducation.com/auth/client/token",
            client_id=self.api_key,
            client_secret=self.api_secret,
        )
        self._access_token = response["access_token"]
        self._log.debug(f"Fetched access token {self._access_token}")

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def _call_api(self, method: str, path: str, params: Dict = {}, body=None):
        """ """
        url = f"https://api.whetstoneeducation.com/external/{path}"
        client_session = requests.Session()
        client_session.headers.update(
            {
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self._access_token}",
            }
        )
        try:
            response = client_session.request(
                method=method, url=url, params=params, json=body
            )
            response.raise_for_status()
            return response
        except requests.exceptions.HTTPError as err:
            if response.status_code >= 500:
                self._log.warn(f"Failed to retrieve data: {err}")
                raise err
            else:
                return response

    def get_data(self, schema: str, params: Dict = {}):
        """ """
        default_params = {"limit": 100, "skip": 0}
        default_params.update(params)

        all_data = []
        while True:
            response = self._call_api(
                method="GET",
                path=schema,
                params=default_params,
            )

            if response.ok:
                response_json = response.json()
                data = response_json.get("data")
                all_data.extend(data)

                if len(all_data) >= response_json.get("count"):
                    break
                else:
                    default_params["skip"] += default_params["limit"]
            else:
                raise requests.exceptions.HTTPError(response.json())

        response_json.update({"data": all_data})
        return response_json
