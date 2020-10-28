import os
from sys import exit
from time import time

import yaml
from colorama import Fore, Style
import pandas as pd


CONFIG = {'PATHS': {'DIR_XML': '/XMLs/', 'DIR_OUT': '/OUT/', 'FILE_MAP': '/map.csv'},
          'VERSIONS': {'CURRENT': 1, 'COMPATIBILITY': 1},
          'THREADS': {'MAX': 3},
          'MAP': {'LAZY_MODE': 1, 'PRIOR_ATTR': 1}}
MAP_KEYS = ['KEY_NAME', 'ROOT_ELEMENT', 'KEY_PATH', 'KEY_PATH_ATTR', 'KEY_REQ']


def elapsed_time_of_function(function):
    def wrapped(*args, **kwargs):
        start_time = time()
        res = function(*args, **kwargs)
        print(f'    [*] elapsed time of /{function.__name__}/ is {time() - start_time} seconds')
        return res

    return wrapped


def get_config_key_value(config_ext: dict, config_int: dict, key: str, key_leaf: str = None) -> str:
    if key_leaf:
        if config_ext.get(key) and (type(config_ext.get(key)) == dict and config_ext.get(key).get(key_leaf)) is not None:
            if key == 'PATHS' and config_ext[key].get(key_leaf).find(':') != -1:
                return config_ext[key][key_leaf]
            elif key == 'PATHS':
                return os.path.dirname(__file__) + config_ext[key][key_leaf]
            else:
                return config_ext[key][key_leaf]
        else:
            print_message('W',
                          f'The config.yaml file doesn\'t contain key {key} or {key}/{key_leaf}. '
                          f'The default value is used')
            return os.path.dirname(__file__) + config_int[key][key_leaf] if key == 'PATHS' else config_int[key][
                key_leaf]
    else:
        if config_ext.get(key):
            return config_ext[key][key_leaf]
        else:
            print_message('W', f'The config.yaml file doesn\'t contain key {key}. The default value is used')
            return config_int[key][key_leaf]


def get_config(path) -> dict:
    path_dict = dict.fromkeys(
        ['CONFIG_ABSENT', 'DIR_XML', 'DIR_OUT', 'FILE_MAP', 'VERSION_CURRENT', 'VERSION_COMPATIBILITY', 'THREADS_MAX',
         'MAP_LAZY_MODE', 'MAP_PRIOR_ATTR'])
    path_dict['CONFIG_ABSENT'] = True
    try:
        with open(path, 'r', encoding='windows-1251') as cfg:
            try:
                config_yaml = yaml.safe_load(cfg.read())
            except yaml.YAMLError as e:
                if hasattr(e, 'problem_mark'):
                    mark = e.problem_mark
                    print_message('E', f'Error parsing Yaml file at line {mark.line}, column {mark.column + 1}')
                else:
                    print_message('E', f'Something went wrong while parsing yaml file')
                exit()
            path_dict['CONFIG_ABSENT'] = False

            path_dict['DIR_XML'] = get_config_key_value(config_yaml, CONFIG, 'PATHS', 'DIR_XML')
            path_dict['DIR_OUT'] = get_config_key_value(config_yaml, CONFIG, 'PATHS', 'DIR_OUT')
            path_dict['FILE_MAP'] = get_config_key_value(config_yaml, CONFIG, 'PATHS', 'FILE_MAP')
            path_dict['VERSION_CURRENT'] = get_config_key_value(config_yaml, CONFIG, 'VERSIONS', 'CURRENT')
            path_dict['VERSION_COMPATIBILITY'] = get_config_key_value(config_yaml, CONFIG, 'VERSIONS', 'COMPATIBILITY')
            path_dict['MAP_LAZY_MODE'] = get_config_key_value(config_yaml, CONFIG, 'MAP', 'LAZY_MODE')
            path_dict['MAP_PRIOR_ATTR'] = get_config_key_value(config_yaml, CONFIG, 'MAP', 'PRIOR_ATTR')
            path_dict['THREADS_MAX'] = get_config_key_value(config_yaml, CONFIG, 'THREADS', 'MAX')
    except FileNotFoundError or FileExistsError:
        print_message('W', f'The config.yaml wasn\'t found at {os.path.dirname(__file__)}. The default preset is used')
        path_dict['DIR_XML'] = os.path.dirname(__file__) + CONFIG['PATHS']['DIR_XML']
        path_dict['DIR_OUT'] = os.path.dirname(__file__) + CONFIG['PATHS']['DIR_OUT']
        path_dict['FILE_MAP'] = os.path.dirname(__file__) + CONFIG['PATHS']['FILE_MAP']
    finally:
        return path_dict


def get_xml_map(path, sep):
    map_raw = pd.read_csv(filepath_or_buffer=path, sep=sep, header='infer')
    # TODO: предусмотреть проверку на наличие пустых KEY_NAME, KEY_PATH, некорректная запись XPath
    # TODO: (нет лидирущего слеша, есть слеш после листового тега без аттрибута, неправильный слеш)
    # TODO: Для таких ERROR: Incorrect mapping и абортимся
    key_path = map_raw['KEY_PATH'].str.split('@', expand=True)
    key_path.rename(columns={0: 'KEY_PATH', 1: 'KEY_PATH_ATTR'}, inplace=True)
    key_path.replace('[/]$', '', regex=True, inplace=True)
    map_raw.drop(['KEY_PATH'], axis=1, inplace=True)
    map_raw = pd.concat([map_raw, key_path], axis=1)
    map_raw = map_raw[MAP_KEYS]
    # удаляем закомментированные ключи (два пробела в начале имени поля)
    map_raw.where(map_raw['KEY_NAME'] > u'\u0020\u0020\uFFFF', inplace=True)
    map_raw.dropna(how='all', inplace=True)  # .reset_index()
    map_root = map_raw[map_raw['ROOT_ELEMENT'].isna()]
    map_inter = map_raw[map_raw['ROOT_ELEMENT'].notna()].reset_index()
    map_inter.drop(['index'], axis=1, inplace=True)
    if map_root.empty and map_inter.empty:
        print_message('E', f'No key was found in the map file along the path {path}')
        exit()  # terminate
    return map_root, map_inter


def print_message(type, msg):
    msg_types = {'E': Fore.RED + '[ERROR]' + Style.RESET_ALL,
                 'W': Fore.YELLOW + '[WARNING]' + Style.RESET_ALL,
                 'I': Fore.CYAN + '[INFO]' + Style.RESET_ALL}
    print(f'{msg_types.get(type, "")} {msg}')


def push_config(path, config=CONFIG) -> dict:
    try:
        with open(path, 'w', encoding='windows-1251') as cfg:
            try:
                yaml.dump(config, cfg)
            except yaml.YAMLError as e:
                if hasattr(e, 'problem_mark'):
                    mark = e.problem_mark
                    print_message('E', f'Error parsing Yaml file at line {mark.line}, column {mark.column + 1}')
                else:
                    print_message('E', f'Something went wrong while parsing yaml file')
                exit()
    except FileNotFoundError or FileExistsError:
        print_message('W', f'Can\'t write the config.yaml to {os.path.dirname(__file__)}')

# TODO: Проверяем версию в конфиге; при необходимости поднимаем до актуальной
# with open(config, 'w') as cfg:
#     yaml.dump(CONFIG, cfg)


# if config_absent and :
#     with open(config, 'w') as cfg:
#         yaml.dump(CONFIG, cfg)
