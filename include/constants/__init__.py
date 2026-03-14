from pathlib import Path

CONFIG_FILE_PATH = Path("config/config.yaml")
PARAMS_FILE_PATH = Path("params.yaml")

EQ_COLUMNS = ['type', 'id', 'properties.mag', 'properties.place', 'properties.time', 
              'properties.updated', 'properties.tz', 'properties.url', 'properties.detail', 
              'properties.felt', 'properties.cdi', 'properties.mmi', 'properties.alert', 
              'properties.status', 'properties.tsunami', 'properties.sig', 'properties.net', 
              'properties.code', 'properties.ids', 'properties.sources', 'properties.types', 
              'properties.nst', 'properties.dmin', 'properties.rms', 'properties.gap', 
              'properties.magType', 'properties.type', 'properties.title', 'geometry.type', 
              'geometry.coordinates']