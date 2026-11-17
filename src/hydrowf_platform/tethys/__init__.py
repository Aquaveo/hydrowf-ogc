"""Host the OGC API - Processes endpoints in a Tethys portal (portal_config.yml):

    settings:
      TETHYS_PORTAL_CONFIG:
        ADDITIONAL_SETTINGS_FILES: [hydrowf_platform.tethys.settings]
        ADDITIONAL_URLPATTERNS: [hydrowf_platform.tethys.urls.urlpatterns]

Two modules because Tethys imports them at different times: settings while its own settings load,
urls once Django is configured. pygeoapi's views read django.conf.settings when they are imported,
so importing them from the settings module would import Tethys's settings from inside themselves.
"""
