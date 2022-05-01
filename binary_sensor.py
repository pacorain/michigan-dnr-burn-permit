from typing import Any, Optional
from urllib.parse import quote, urlencode
import logging

import voluptuous as vol
import httpx
import pandas

import homeassistant.helpers.config_validation as cv
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.typing import ConfigType, DiscoveryInfoType
from .const import CONF_COUNTY, CONF_TOWNSHIP, URL_BURN_PERMIT_SEARCH
from homeassistant.helpers.httpx_client import get_async_client
from homeassistant.components.binary_sensor import (
    BinarySensorEntity,
    PLATFORM_SCHEMA as PARENT_PLATFORM_SCHEMA,
)

PLATFORM_SCHEMA = PARENT_PLATFORM_SCHEMA.extend(
    {vol.Required(CONF_COUNTY): cv.string, vol.Required(CONF_TOWNSHIP): cv.string}
)

_LOGGER = logging.getLogger(__name__)


async def async_setup_platform(
    hass: HomeAssistant,
    config: ConfigType,
    async_add_entities: AddEntitiesCallback,
    discovery_info: Optional[DiscoveryInfoType] = None,
) -> None:
    county: str = config[CONF_COUNTY]
    township: str = config[CONF_TOWNSHIP]

    async_add_entities([BurnPermitSensor(hass, county, township)])


class BurnPermitSensor(BinarySensorEntity):
    """DNR Scrap Sensor."""

    def __init__(self, hass: HomeAssistant, county: str, township: str):
        self._county = county.upper()
        self._township = township.upper()
        self._async_client = None
        self._hass = hass
        self.data = None

    async def async_update(self):
        """Get the latest data from the Michigan Department of Natural Resources."""
        if not self._async_client:
            self._async_client = get_async_client(self._hass)

        try:
            response = await self._async_client.request(
                "GET", self._resource, follow_redirects=True
            )
            for table in pandas.read_html(response.text):
                if "Township Name" in table:
                    row = table.set_index("Township Name").loc[self._township]
                    self.data = (
                        row["Burning Permits Issued"],
                        row["Guidelines and Restrictions"],
                    )
                    return

            _LOGGER.error(f"No data obtained for county {self._county}.")
        except httpx.RequestError as ex:
            _LOGGER.error("Error fetching data: %s failed with %s", self._resource, ex)
            self.data = None
        except KeyError as ex:
            _LOGGER.error(f"Township {self._township} not found on {self.full_url}")
            self.data = None

    @property
    def full_url(self):
        """Return the URL to query the data from."""
        return f"{URL_BURN_PERMIT_SEARCH}?{quote(self._county)}"

    @property
    def is_on(self) -> bool:
        """Return if burning permits are issued, unrestricted."""
        if self.data is None:
            return False
        return self.data == "Yes", "All Day"

    @property
    def state_attributes(self) -> dict[str, Any] | None:
        """Fetch details on burning permit state."""
        if self.data is None:
            return None
        return {
            "burning_permitted": self.data[0],
            "guidelines_restrictions": self.data[1],
        }

    @property
    def available(self) -> bool:
        """Return if data is available."""
        return self.data is not None
