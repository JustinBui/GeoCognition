from include.constants import CONFIG_FILE_PATH, PARAMS_FILE_PATH
from include.helpers import read_yaml, create_directories


class ConfigurationManager:
    def __init__(self, 
                 config_path=CONFIG_FILE_PATH,
                 params_path=PARAMS_FILE_PATH):
        self.config = read_yaml(config_path)
        self.params = read_yaml(params_path)

        create_directories([self.config.artifacts_root])