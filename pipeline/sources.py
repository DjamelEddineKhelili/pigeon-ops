"""
The SOURCE REGISTRY: every outside system the company pulls data from.

One dict per source. Adding a new source = adding one entry here (no other code changes).
That's a real-world pattern: configuration, not code, describes the sources.
"""

SOURCES = {
    # Vélib' live: how many bikes / free docks each station has RIGHT NOW. Changes every minute.
    "velib_status": {
        "url": "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_status.json",
        "ext": "json",
        "every": "5 minutes",
    },
    # Vélib' reference data: station names, GPS position, capacity. Changes rarely.
    "velib_info": {
        "url": "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_information.json",
        "ext": "json",
        "every": "1 day",
    },
    # Weather in central Paris (Open-Meteo, free, no key). "current" = the latest hourly observation.
    "weather": {
        "url": (
            "https://api.open-meteo.com/v1/forecast"
            "?latitude=48.8566&longitude=2.3522"
            "&current=temperature_2m,precipitation,rain,wind_speed_10m,weather_code"
            "&timezone=UTC"
        ),
        "ext": "json",
        "every": "1 hour",
    },
    # French fuel prices, ~10k stations, one row per station with all fuels (data.gouv.fr, CSV ~19 MB).
    # This URL REDIRECTS to the real file: the HTTP client must follow redirects.
    "fuel": {
        "url": "https://www.data.gouv.fr/api/1/datasets/r/edd67f5b-46d0-4663-9de9-e5db1c880160",
        "ext": "csv",
        "every": "3 hours",
    },
}
