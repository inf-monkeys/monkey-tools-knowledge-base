import yaml


def load_config(filename):
    with open(filename, 'r') as file:
        config = yaml.safe_load(file)
    return config


config_data = load_config('config.yaml')
