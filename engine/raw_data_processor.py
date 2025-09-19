import os
from pathlib import Path
from typing import Dict
from warnings import warn
import yaml


CONFIG_PATH : Path = Path(os.getcwd()).parent / "conf" / "raw_data_source.yml"

def retrieve_data(path: Path, bucket_id: str) -> Dict[str, str]:
    config: Dict[str, Dict[str, str]] = yaml.safe_load(open(path))
    return config[bucket_id]


class ServerConfig:
    def __init__(self, bucket_id: str):
        config_data: Dict[str, str] = retrieve_data(CONFIG_PATH, bucket_id)
        try:
            self.address = config_data["address"]
            self.login = config_data["login"]
            self.password = config_data["password"]
        except KeyError as ke:
            warning: str = f"Some connexion information is missing : {ke.args[0]}"
            warn(UserWarning(warning))


class RawDataProcessor:

    def __init__(self, server_config: ServerConfig):
        self.serverConfig = server_config
