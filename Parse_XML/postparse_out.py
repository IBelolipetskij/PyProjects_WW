from glob import glob
from os import path, remove

from ext_accessory import print_message, CONFIG


def parse_out_to_csv():
    data_lst = []
    try:
        remove(f"{path.dirname(__file__) + CONFIG['PATHS']['DIR_OUT']}OVERALL.csv")
    except FileNotFoundError:
        print_message('W', 'File OVERALL.csv was not found')
    files = glob(fr"{path.dirname(__file__) + CONFIG['PATHS']['DIR_OUT']}*.*")

    for xf in files[:1]:
        with open(xf) as f:
            data = [x for x in f]
            data_lst.append(f'FILE,{data[0]}')

    for xf in files:
        with open(xf) as f:
            print_message('I', f'File: {xf}')
            data = [x for x in f]
            data_lst.append(f'{xf},{data[1]}')

    with open(f"{path.dirname(__file__) + CONFIG['PATHS']['DIR_OUT']}OVERALL.csv", 'w', encoding='windows-1251') as f:
        for i in data_lst:
            f.write(i)


if __name__ == '__main__':
    parse_out_to_csv()
