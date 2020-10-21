import configparser

def get_prop(prop_name):
    config = configparser.ConfigParser()
    config.read('properties.ini')
    if 'db_' in prop_name:
        return config.get('DATABASE', prop_name)
    else :
        return config.get('EXCEL', prop_name)