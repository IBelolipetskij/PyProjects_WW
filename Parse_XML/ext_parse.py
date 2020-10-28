import re
from glob import glob
from multiprocessing import Process

import pandas as pd
from lxml import etree as et, objectify as o
from numpy import array_split as a_split

from ext_accessory import print_message, elapsed_time_of_function


# NA = 'n/a'
NA = ''


def normalize_map_root(root, map_root):
    key_links = {i: j for i, j in zip(map_root.columns.to_list(), range(len(map_root.columns.to_list())))}
    val_dict = {}

    for i in map_root.itertuples(index=False):
        if root.tag == str.replace(i[key_links['KEY_PATH']], '/', ''):
            val_dict.update({i[key_links['KEY_PATH']]: ''})
        elif root.tag == i[key_links['KEY_PATH']][1:len(root.tag) + 1]:
            val_dict.update({i[key_links["KEY_PATH"]]: str.replace(i[key_links["KEY_PATH"]], "/" + root.tag, "", 1)})
        else:
            val_dict.update({i[key_links['KEY_PATH']]: i[key_links['KEY_PATH']]})

    map_root.replace({'KEY_PATH': val_dict}, inplace=True)


def normalize_map_iter(root, map_iter):
    key_links = {i: j for i, j in zip(map_iter.columns.to_list(), range(len(map_iter.columns.to_list())))}
    val_dict = {}

    # удаляем из KEY_PATH упоминание ROOT_ELEMENT
    for root_i in map_iter['ROOT_ELEMENT'].unique():
        map_iter_i = map_iter.where(map_iter['ROOT_ELEMENT'] == root_i).dropna(how='all')
        for i in map_iter_i.itertuples(index=False):
            if root_i == i[key_links['KEY_PATH']]:
                val_dict.update({i[key_links['KEY_PATH']]: ''})
            elif root_i == i[key_links['KEY_PATH']][:len(root_i)]:
                val_dict.update({i[key_links["KEY_PATH"]]: str.replace(i[key_links["KEY_PATH"]], root_i, "", 1)})
            else:
                val_dict.update({i[key_links['KEY_PATH']]: i[key_links['KEY_PATH']]})

    map_iter.replace({'KEY_PATH': val_dict}, inplace=True)

    # удаляем из ROOT_ELEMENT упоминание реального root
    val_dict = {}
    for i in map_iter.itertuples(index=False):
        if root.tag == str.replace(i[key_links['ROOT_ELEMENT']], '/', ''):
            val_dict.update({i[key_links['ROOT_ELEMENT']]: ''})
        elif root.tag == i[key_links['ROOT_ELEMENT']][1:len(root.tag) + 1]:
            val_dict.update(
                {i[key_links["ROOT_ELEMENT"]]: str.replace(i[key_links["ROOT_ELEMENT"]], "/" + root.tag, "", 1)})
        else:
            val_dict.update({i[key_links['ROOT_ELEMENT']]: i[key_links['ROOT_ELEMENT']]})
    map_iter.replace({'ROOT_ELEMENT': val_dict}, inplace=True)

    # удаляем из KEY_PATH упоминание очищенного ROOT_ELEMENT
    for root_i in map_iter['ROOT_ELEMENT'].unique():
        map_iter_i = map_iter.where(map_iter['ROOT_ELEMENT'] == root_i).dropna(how='all')

        for i in map_iter_i.itertuples(index=False):
            if root_i == i[key_links['KEY_PATH']]:
                val_dict.update({i[key_links['KEY_PATH']]: ''})
            elif root_i == i[key_links['KEY_PATH']][:len(root_i)]:
                val_dict.update({i[key_links["KEY_PATH"]]: str.replace(i[key_links["KEY_PATH"]], root_i, "", 1)})
            else:
                val_dict.update({i[key_links['KEY_PATH']]: i[key_links['KEY_PATH']]})

    map_iter.replace({'KEY_PATH': val_dict}, inplace=True)


def parse_xml_root(root, map_root, lazy_mode, prior_attr):
    val_lst = []
    key_links = {i: j for i, j in zip(map_root.columns.to_list(), range(len(map_root.columns.to_list())))}
    for i in map_root.itertuples(index=False):
        if lazy_mode != 1 and i[key_links['KEY_REQ']] != 0:
            if i[key_links['KEY_PATH_ATTR']]:
                val_lst.append(
                    root.get(i[key_links['KEY_PATH_ATTR']]) if i[key_links['KEY_PATH']] == '' else root.find(
                        f'./{i[key_links["KEY_PATH"]]}').get(i[key_links['KEY_PATH_ATTR']]))
            else:
                val_lst.append(root.find(f'./{i[key_links["KEY_PATH"]]}').text)
        elif lazy_mode != 1 and i[key_links['KEY_REQ']] == 0:
            if i[key_links['KEY_PATH_ATTR']]:
                try:
                    val_lst.append(root.get(i[key_links['KEY_PATH_ATTR']]) if i[key_links[
                        'KEY_PATH']] == '' else root.find(f'./{i[key_links["KEY_PATH"]]}').get(
                        i[key_links['KEY_PATH_ATTR']]))
                except AttributeError:
                    val_lst.append(NA)
            else:
                try:
                    val_lst.append(NA if i[key_links['KEY_PATH']] == '' else root.find(
                        f'./{i[key_links["KEY_PATH"]]}').text)
                except AttributeError:
                    val_lst.append(NA)
        else:
            exception_exists = False
            try:
                if prior_attr == 1:
                    val_lst.append(root.get(i[key_links['KEY_PATH_ATTR']]) if i[key_links[
                        'KEY_PATH']] == '' else root.find(f'./{i[key_links["KEY_PATH"]]}').get(
                        i[key_links['KEY_PATH_ATTR']]))
                else:
                    if i[key_links['KEY_PATH']] == '' and not i[key_links['KEY_PATH_ATTR']]:
                        val = NA
                    elif i[key_links['KEY_PATH']] == '' and i[key_links['KEY_PATH_ATTR']]:
                        val = root.find(f'./{i[key_links["KEY_PATH_ATTR"]]}').text
                    elif i[key_links['KEY_PATH_ATTR']]:
                        val = root.find(f'./{i[key_links["KEY_PATH"]]}/{i[key_links["KEY_PATH_ATTR"]]}').text
                    else:
                        val = root.find(f'./{i[key_links["KEY_PATH"]]}').text
                    val_lst.append(val)
            except AttributeError:
                exception_exists = True
            except TypeError:
                exception_exists = True
            except ValueError:
                exception_exists = True
            if exception_exists:
                try:
                    if prior_attr == 1:
                        if i[key_links['KEY_PATH']] == '' and not i[key_links['KEY_PATH_ATTR']]:
                            val = NA
                        elif i[key_links['KEY_PATH']] == '' and i[key_links['KEY_PATH_ATTR']]:
                            val = root.find(f'./{i[key_links["KEY_PATH_ATTR"]]}').text
                        elif i[key_links['KEY_PATH_ATTR']]:
                            val = root.find(
                                f'./{i[key_links["KEY_PATH"]]}/{i[key_links["KEY_PATH_ATTR"]]}').text
                        else:
                            val = root.find(f'./{i[key_links["KEY_PATH"]]}').text
                        val_lst.append(val)
                    else:
                        val_lst.append(root.get(i[key_links['KEY_PATH_ATTR']]) if i[key_links[
                            'KEY_PATH']] == '' else root.find(f'./{i[key_links["KEY_PATH"]]}').get(
                            i[key_links['KEY_PATH_ATTR']]))
                except AttributeError:
                    val_lst.append(NA)
                except TypeError:
                    val_lst.append(NA)
                except ValueError:
                    val_lst.append(NA)
    return val_lst


def parse_xml_iter(root, map_iter, current_file, lazy_mode, prior_attr):
    overall_rec_lst, root_rec_lst, key_lst = [], [], []
    key_links = {i: j for i, j in zip(map_iter.columns.to_list(), range(len(map_iter.columns.to_list())))}

    for root_i in map_iter['ROOT_ELEMENT'].unique():
        root_rec_lst = []
        is_root = root.tag == str(root_i).replace('/', '')
        sub_root = str.replace(root_i, "/" + root.tag, "", 1) if root.tag == root_i[1:len(root.tag) + 1] else root_i

        map_iter_i = map_iter.where(map_iter['ROOT_ELEMENT'] == root_i).dropna(how='all')
        key_lst.append(map_iter_i['KEY_NAME'].to_list())
        if not is_root:
            for rec in root.findall(f'./{sub_root}'):
                val_lst = get_values_from_rec(map_iter_i, root_i, key_links, rec, is_root, current_file, lazy_mode,
                                              prior_attr)
                root_rec_lst.append(val_lst)
        else:
            print_message('W', 'Root key can\'t be iterable')
            val_lst = get_values_from_rec(map_iter_i, root_i, key_links, root, is_root, current_file, lazy_mode,
                                          prior_attr)
            root_rec_lst.append(val_lst)
        overall_rec_lst.append(root_rec_lst)

    iter_result = pd.Series(pd.DataFrame(data=overall_rec_lst[i], columns=key_lst[i]) for i in range(len(key_lst)))
    return pd.concat([iter_result[i] for i in range(len(iter_result))], axis=1)


def get_values_from_rec(map_iter, target_root, key_links, rec, is_root, target_file, lazy_mode, prior_attr):
    val_lst = []
    for i in map_iter.itertuples(index=False):
        if is_root:
            print_message('W', '> Key ' + str(
                i[key_links['KEY_NAME']]) + ' specified as iterable but the ROOT_KEY ' + str(
                i[key_links['ROOT_ELEMENT']]) + f' is the same as in the file "{target_file}"')

        if lazy_mode != 1 and i[key_links['KEY_REQ']] != 0:
            if i[key_links['KEY_PATH_ATTR']]:
                val_lst.append(
                    rec.get(i[key_links['KEY_PATH_ATTR']]) if i[key_links['KEY_PATH']] == '' else rec.find(
                        f'./{i[key_links["KEY_PATH"]]}').get(i[key_links['KEY_PATH_ATTR']]))
            else:
                val_lst.append(rec.find(f'./{i[key_links["KEY_PATH"]]}').text)
        elif lazy_mode != 1 and i[key_links['KEY_REQ']] == 0:
            if i[key_links['KEY_PATH_ATTR']]:
                try:
                    val_lst.append(rec.get(i[key_links['KEY_PATH_ATTR']]) if i[key_links[
                        'KEY_PATH']] == '' else rec.find(f'./{i[key_links["KEY_PATH"]]}').get(
                        i[key_links['KEY_PATH_ATTR']]))
                except AttributeError:
                    val_lst.append(NA)
            else:
                try:
                    val_lst.append(NA if i[key_links['KEY_PATH']] == '' else rec.find(
                        f'./{i[key_links["KEY_PATH"]]}').text)
                except AttributeError:
                    val_lst.append(NA)
        else:  # lazy_mode == 1
            exception_exists = False
            try:
                if prior_attr == 1:
                    val_lst.append(rec.get(i[key_links['KEY_PATH_ATTR']]) if i[key_links[
                        'KEY_PATH']] == '' else rec.find(f'./{i[key_links["KEY_PATH"]]}').get(
                        i[key_links['KEY_PATH_ATTR']]))
                else:
                    if i[key_links['KEY_PATH']] == '' and not i[key_links['KEY_PATH_ATTR']]:
                        val = NA
                    elif i[key_links['KEY_PATH']] == '' and i[key_links['KEY_PATH_ATTR']]:
                        val = rec.find(f'./{i[key_links["KEY_PATH_ATTR"]]}').text
                    elif i[key_links['KEY_PATH_ATTR']]:
                        val = rec.find(f'./{i[key_links["KEY_PATH"]]}/{i[key_links["KEY_PATH_ATTR"]]}').text
                    else:
                        val = rec.find(f'./{i[key_links["KEY_PATH"]]}').text
                    val_lst.append(val)
            except AttributeError:
                exception_exists = True
            except TypeError:
                exception_exists = True
            except ValueError:
                exception_exists = True
            if exception_exists:
                try:
                    if prior_attr == 1:
                        if i[key_links['KEY_PATH']] == '' and not i[key_links['KEY_PATH_ATTR']]:
                            val = NA
                        elif i[key_links['KEY_PATH']] == '' and i[key_links['KEY_PATH_ATTR']]:
                            val = rec.find(f'./{i[key_links["KEY_PATH_ATTR"]]}').text
                        elif i[key_links['KEY_PATH_ATTR']]:
                            val = rec.find(
                                f'./{i[key_links["KEY_PATH"]]}/{i[key_links["KEY_PATH_ATTR"]]}').text
                        else:
                            val = rec.find(f'./{i[key_links["KEY_PATH"]]}').text
                        val_lst.append(val)
                    else:
                        val_lst.append(rec.get(i[key_links['KEY_PATH_ATTR']]) if i[key_links[
                            'KEY_PATH']] == '' else rec.find(f'./{i[key_links["KEY_PATH"]]}').get(
                            i[key_links['KEY_PATH_ATTR']]))
                except AttributeError:
                    val_lst.append(NA)
                except TypeError:
                    val_lst.append(NA)
                except ValueError:
                    val_lst.append(NA)
    return val_lst


def normalize_map(xml_path: str, map_root, map_iter, root_key=None):
    xml_files = glob(fr"{xml_path}*.XML")
    if xml_files is not None:
        xml_files.extend(glob(fr"{xml_path}\\*\\*.XML"))
    else:
        xml_files = glob(fr"{xml_path}\\*\\*.XML")
    with open(xml_files[0], encoding='utf-8') as f:
        xml = f.read()
        xml = bytes(bytearray(xml, encoding='utf-8'))
        parser = et.XMLParser(ns_clean=True, recover=True, encoding='utf-8')

        if not root_key:
            root = o.fromstring(xml, parser=parser)
        else:
            root = root_key

        if not map_root.empty:
            normalize_map_root(root, map_root)
        if not map_iter.empty:
            normalize_map_iter(root, map_iter)


def parse_xml_worker(index: int, xml_path: str, out_path: str, map_root, map_iter, lazy_mode, prior_attr,
                     root_key=None):
    if len(xml_path) == 0:
        pass
    else:
        for xf in xml_path:
            with open(xf, encoding='utf-8') as f:
                print_message('I', f'worker {index}: xml file: {xf}')
                xml = f.read()
                xml = bytes(bytearray(xml, encoding='utf-8'))
                parser = et.XMLParser(ns_clean=True, recover=True, encoding='utf-8')

                if not root_key:
                    root = o.fromstring(xml, parser=parser)
                else:
                    root = root_key

                if not map_root.empty:
                    data_root = parse_xml_root(root, map_root, lazy_mode, prior_attr)
                if not map_iter.empty:
                    data_iter = parse_xml_iter(root, map_iter, xf, lazy_mode, prior_attr)
                    if not map_root.empty:
                        data_root_pd = pd.DataFrame(data=[data_root for i in range(data_iter.shape[0])],
                                                    columns=map_root['KEY_NAME'])
                if not map_root.empty and not map_iter.empty:
                    overall_pd = pd.concat([data_root_pd, data_iter], axis=1)
                elif not map_iter.empty:
                    overall_pd = data_iter
                elif not map_root.empty:
                    overall_pd = data_root
                overall_pd.to_csv(out_path + '/' + re.sub(r'.[xX][mM][lL]', '.csv', xf.split('\\')[-1]), sep=',',
                                  index=False, encoding='windows-1251')


@elapsed_time_of_function
def parse_xml_main(xml_path: str, out_path: str, map_root, map_iter, lazy_mode, prior_attr, thread_qty, root_key=None):
    threads_lst = []
    xml_files = glob(fr"{xml_path}*.XML")
    if xml_files is not None:
        xml_files.extend(glob(fr"{xml_path}\\*\\*.XML"))
    else:
        xml_files = glob(fr"{xml_path}\\*\\*.XML")

    if len(xml_files) > 1 and thread_qty > 1:
        thread_qty = thread_qty if thread_qty <= len(xml_files) else len(xml_files)
        xml_lst_sliced = a_split(xml_files, thread_qty)

        for i in range(thread_qty):
            proc = Process(target=parse_xml_worker,
                           args=(i, xml_lst_sliced[i], out_path, map_root, map_iter, lazy_mode, prior_attr, root_key))
            threads_lst.append(proc)
            proc.start()

        for proc in threads_lst:
            proc.join()

    else:
        parse_xml_worker(0, xml_files, out_path, map_root, map_iter, lazy_mode, prior_attr, root_key)
