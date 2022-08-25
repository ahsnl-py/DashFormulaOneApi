from configparser import ConfigParser
from pathlib import Path

def get_project_root() -> Path:
  """Returns project root folder."""
  return Path(__file__).parents[1]

def set_config(config_file_name:str, config_sec_name:str):
  section = config_sec_name
  config_file_path = 'config/' + config_file_name
  if(len(config_file_path) > 0 and len(section) > 0):
    # Create an instance of ConfigParser class
    config_parser = ConfigParser()
    # Read the configuration file
    config_parser.read(config_file_path)
    config = {}
    if(config_parser.has_section(section)):
      config_params = config_parser.items(section)
      for config_param in config_params:
        config[config_param[0]] = config_param[1]
      return config
