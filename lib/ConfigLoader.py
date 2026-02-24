import configparser
import os


def get_config(env):
    """
    Reads sbdl.conf and returns configuration
    dictionary for given environment.
    """

    config = configparser.ConfigParser()

    base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(base_path, "conf", "sbdl.conf")

    config.read(config_path)

    env = env.upper()

    if env not in config.sections():
        raise Exception(f"Environment {env} not found in sbdl.conf")

    return dict(config[env])
