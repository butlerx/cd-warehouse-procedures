"""function related to transforming json"""


def get_city(city: dict) -> str:
    """get city name from different json versions"""
    toponym_name = _get_toponym(city)
    return (
        toponym_name
        if toponym_name != "Unknown"
        else city["nameWithHierarchy"]
        if (city is not None) and ("nameWithHierarchy" in city)
        else toponym_name
    )


def get_country(country: dict) -> str:
    """get country name from different json versions"""
    return (
        country["countryName"]
        if (country is not None) and (country) and ("coutryName" in country)
        else "Unknown"
    )


def get_state(state: dict) -> str:
    """get state name from different json versions"""
    return _get_toponym(state)


def get_county(county: dict) -> str:
    """get county name from different json versions"""
    return _get_toponym(county)


def _get_toponym(land: dict) -> str:
    return (
        land["toponymName"]
        if (land is not None) and (land) and ("toponymName" in land)
        else "Unknown"
    )
