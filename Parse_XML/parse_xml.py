import os

from colorama import init

from ext_parse import normalize_map, parse_xml_main
from ext_accessory import get_config, get_xml_map, push_config

config_file = f'{os.path.dirname(__file__)}/config.yaml'


init()
if __name__ == '__main__':
    config = get_config(config_file)
    xml_map_root, xml_map_iter = get_xml_map(path=config['FILE_MAP'], sep=';')
    normalize_map(xml_path=config['DIR_XML'], map_root=xml_map_root, map_iter=xml_map_iter)
    parse_xml_main(xml_path=config['DIR_XML'], out_path=config['DIR_OUT'], map_root=xml_map_root, map_iter=xml_map_iter,
                   lazy_mode=config['MAP_LAZY_MODE'], thread_qty=config['THREADS_MAX'], prior_attr=config['MAP_PRIOR_ATTR'])

# push_config(config_file)
