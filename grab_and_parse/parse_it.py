import requests as rq
import json
from time import time, sleep
import numpy as np
from threading import Thread
from multiprocessing import Pool
import queue
import pandas as pd
import pprint
from os import remove
from glob import glob


def elapsed_time_of_function(function):
    def wrapped(*args, **kwargs):
        start_time = time()
        res = function(*args, **kwargs)
        print(f'    [*] elapsed time of /{function.__name__}/ is {time() - start_time} seconds\n')
        return res

    return wrapped


def get_JWT_token():
    url = "https://any.bubbles.here/SignificantEvents/MessagesService2/v1/auth"

    payload = "{\r\n\"login\": \"secret\",\r\n\"passwordHash\":\r\n\"BIG_secret\"\r\n}"
    headers = {'Content-Type': 'application/json'}

    response = rq.request("POST", url, headers=headers, data=payload).json()

    return response['jwt']


def get_message_list_session_main(token, date_begin, date_end='2020-10-16T14:00:00'):
    start_iteration = time()
    threads_qty = 4

    message_count = get_message_list_session(None, token, date_begin, date_end)
    incr = 0 if message_count % 20 == 0 else 1
    message_offset_list = [20 * i for i in range(message_count // 20 + incr)]
    message_offset_parts = [i.tolist() for i in np.array_split(message_offset_list, threads_qty)]

    queue_lst = [queue.Queue() for i in range(threads_qty)]
    threads_lst = []
    for i in range(threads_qty):
        threads_lst.append(Thread(target=get_message_list_session,
                                  args=(queue_lst[i], token, date_begin, date_end, 20, message_offset_parts[i])))
    for thr in threads_lst:
        thr.start()
    for thr in threads_lst:
        thr.join()

    while any([thr.is_alive() for thr in threads_lst]):  # пока функция выполняется
        pass

    guids = []
    for qe in queue_lst:
        guids.extend(qe.get())

    print(f'>>> Message count:\n'
          f'before = {message_count}\n'
          f'received = {len(guids)}\n'
          f'')
    print(f'\nDone /get_message_list_session_main/ in {time() - start_iteration}')
    return guids


def get_message_list_session(q, token, date_begin, date_end, limit=20, offset=0):
    if type(offset) == list:
        url_list = [
            f"https://any.bubbles.here/SignificantEvents/MessagesService2/v1/messages?dateBegin={date_begin}&dateEnd={date_end}&limit={limit}&offset={i}"
            for i in offset]
    else:
        url_list = [
            f"https://any.bubbles.here/SignificantEvents/MessagesService2/v1/messages?dateBegin={date_begin}&dateEnd={date_end}&limit={limit}&offset={offset}"]

    with rq.Session() as s:
        s.headers = {'Authorization': f'Bearer {token}'}
        if q:
            q.put_nowait([i['guid'] for url in url_list for i in s.get(url, timeout=2).json()['messages']])
        else:
            return s.get(url_list[0], timeout=2).json()['total']


def get_the_message_main(token, guids_lst):
    start_iteration = time()
    threads_qty = 4

    guids_parts = [i.tolist() for i in np.array_split(guids_lst, threads_qty)]

    threads_lst = []
    for i in range(threads_qty):
        threads_lst.append(Thread(target=get_the_message, args=(i, token, guids_parts[i])))
    for thr in threads_lst:
        thr.start()
    for thr in threads_lst:
        thr.join()

    print(f'\nDone /get_the_message_main/ in {time() - start_iteration}')


def get_the_message(index, token, guids_lst):
    url = f"https://any.bubbles.here/SignificantEvents/MessagesService2/v1/messages/"
    init_write_flag = True
    cnt_of_recs_to_push = 1
    message_info_columns = (
        'MESSAGE_GUID'
        , 'REC_EFFECTIVE_FROM'
        , 'Message_Type'
        , 'Message_TypeDescription'
        , 'Message_PublishDate'
        , 'Publisher_Type'
        , 'Publisher_TypeName'
        , 'Publisher_FullName'
        , 'Publisher_NameLat'
        , 'Publisher_FIO'
        , 'Publisher_INN'
        , 'Publisher_OGRN'
        , 'Publisher_EGRULAddress'
        , 'Publisher_CountryCode'
        , 'Publisher_Country'
        , 'Message_Number'
        , 'Message_DisclosureDate'
        , 'Message_LockReason'
        , 'Message_AnnulmentGUID'
        , 'Message_AnnulmentNumber'
        , 'NotaryFIO'
        , 'NotaryPosition'
        , 'ArbitrManagerName'
        , 'Version'
        , 'Type'
        , 'FullName'
        , 'INN'
        , 'OGRN'
        , 'PreviousMessage')

    message_xml_columns = (
        'MESSAGE_GUID'
        , 'Message_Body')

    message_file_infos_columns = (
        'MESSAGE_GUID'
        , 'File_Name'
        , 'File_Hash'
        , 'File_Size')

    linked_messages_columns = (
        'MESSAGE_GUID'
        , 'LNK_Message_GUID'
        , 'LNK_Message_Number'
        , 'LNK_Message_Type'
        , 'LNK_Message_TypeDescription'
        , 'LNK_Message_PublishDate'
        , 'LNK_MessageAnnullment_Guid'
        , 'LNK_MessageAnnullment_Number'
        , 'LNK_Message_LockReason'
        , 'LNK_PreviosMessageGuid')

    with rq.Session() as s:
        s.headers = {'Authorization': f'Bearer {token}'}
        for i in range(len(guids_lst)):
            response = s.get(url + guids_lst[i], timeout=2).json()
            message_info, message_xml, message_file_infos, linked_messages = parse_message(guids_lst[i],
                                                                                                response)

            df_message_info = pd.DataFrame(data=np.array([message_info]), columns=message_info_columns)
            df_message_xml = pd.DataFrame(data=np.array([message_xml]), columns=message_xml_columns)
            df_message_file_infos = pd.DataFrame(
                data=np.array([message_file_infos] if type(message_file_infos) != list else message_file_infos),
                columns=message_file_infos_columns)
            df_linked_messages = pd.DataFrame(
                data=np.array([linked_messages] if type(linked_messages) != list else linked_messages),
                columns=linked_messages_columns)

            if cnt_of_recs_to_push == 1:
                df_message_info_overall = df_message_info
                df_message_xml_overall = df_message_xml
                df_message_file_infos_overall = df_message_file_infos
                df_linked_messages_overall = df_linked_messages
            else:
                df_message_info_overall = pd.concat(objs=[df_message_info_overall, df_message_info],
                                                    ignore_index=True)
                df_message_xml_overall = pd.concat(objs=[df_message_xml_overall, df_message_xml],
                                                        ignore_index=True)
                df_message_file_infos_overall = pd.concat(
                    objs=[df_message_file_infos_overall, df_message_file_infos],
                    ignore_index=True)
                df_linked_messages_overall = pd.concat(objs=[df_linked_messages_overall, df_linked_messages],
                                                       ignore_index=True)
            if cnt_of_recs_to_push >= QTY_OF_RECS_TO_PUSH or i == len(guids_lst) - 1:
                if init_write_flag:
                    df_message_info_overall.to_csv(
                        f'c:\PyProjectsExt\grab_and_parse\message_info_{str(index)}.csv_part', sep='^',
                        index=False, mode='w')
                    df_message_xml_overall.to_csv(
                        f'c:\PyProjectsExt\grab_and_parse\message_xml_{str(index)}.csv_part', sep='^',
                        index=False, mode='w')
                    df_message_file_infos_overall.to_csv(
                        f'c:\PyProjectsExt\grab_and_parse\message_file_infos_{str(index)}.csv_part', sep='^',
                        index=False, mode='w')
                    df_linked_messages_overall.to_csv(
                        f'c:\PyProjectsExt\grab_and_parse\linked_messages_{str(index)}.csv_part', sep='^',
                        index=False, mode='w')
                    init_write_flag = False
                else:
                    df_message_info_overall.to_csv(
                        f'c:\PyProjectsExt\grab_and_parse\message_info_{str(index)}.csv_part', sep='^',
                        index=False, header=False, mode='a')
                    df_message_xml_overall.to_csv(
                        f'c:\PyProjectsExt\grab_and_parse\message_xml_{str(index)}.csv_part', sep='^',
                        index=False, header=False, mode='a')
                    df_message_file_infos_overall.to_csv(
                        f'c:\PyProjectsExt\grab_and_parse\message_file_infos_{str(index)}.csv_part', sep='^',
                        index=False, header=False, mode='a')
                    df_linked_messages_overall.to_csv(
                        f'c:\PyProjectsExt\grab_and_parse\linked_messages_{str(index)}.csv_part', sep='^',
                        index=False, header=False, mode='a')
                cnt_of_recs_to_push = 1
            else:
                cnt_of_recs_to_push += 1


def parse_message(guid, message):
    default_value = '#NULL'

    def get_iter_default(msg: dict, keys: str, default_val=default_value):
        keys_lst = keys.split('.')

        t = None
        for key in keys_lst:
            if t is None:
                t = msg.get(key, default_val)
            else:
                t = t.get(key, default_val)
            if t == default_val or t == {} or t == []:
                return default_val
        return t

    def publisher_type(pub_type_name):
        if pub_type_name == 'Company':
            return 'PublisherInfoCompany'
        elif pub_type_name == 'IndividualEntrepreneur':
            return 'PublisherInfoIndividualEntrepreneur'
        elif pub_type_name == 'Person':
            return 'PublisherInfoPerson'
        elif pub_type_name == 'NonResidentCompany':
            return 'PublisherInfoNonResidentCompany'
        elif pub_type_name == 'Appraiser':
            return 'PublisherInfoAppraiser'
        else:
            return pub_type_name

    def publisher_fullname(pub_type_name, msg, default_val=default_value) -> str:
        if pub_type_name == 'Company':
            return get_iter_default(msg, 'publisher.data.fullName')
        elif pub_type_name in ['NonResidentCompany', 'ForeignSystem']:
            return get_iter_default(msg, 'publisher.data.name')
        else:
            return default_val

    def publisher_name_lat(pub_type_name, msg, default_val=default_value) -> str:
        if pub_type_name == 'NonResidentCompany':
            return get_iter_default(msg, 'publisher.data.latinName')
        else:
            return default_val

    def publisher_fio(pub_type_name, msg, default_val=default_value) -> str:
        if pub_type_name in ['IndividualEntrepreneur', 'Person', 'Appraiser']:
            return get_iter_default(msg, 'publisher.data.fio')
        else:
            return default_val

    def publisher_inn(pub_type_name, msg, default_val=default_value) -> str:
        if pub_type_name in ['Company', 'IndividualEntrepreneur', 'Person', 'Appraiser']:
            return get_iter_default(msg, 'publisher.data.inn')
        elif pub_type_name == 'NonResidentCompany':
            return get_iter_default(msg, 'publisher.data.innOrAnalogue')
        else:
            return default_val

    def publisher_ogrn(pub_type_name, msg, default_val=default_value) -> str:
        if pub_type_name == 'Company':
            return get_iter_default(msg, 'publisher.data.ogrn')
        elif pub_type_name == 'IndividualEntrepreneur':
            return get_iter_default(msg, 'publisher.data.ogrnip')
        elif pub_type_name == 'NonResidentCompany':
            return get_iter_default(msg, 'publisher.data.regNum')
        else:
            return default_val

    def publisher_egrul_address(pub_type_name, msg, default_val=default_value) -> str:
        if pub_type_name == 'Company':
            return get_iter_default(msg, 'publisher.data.egrulAddress')
        else:
            return default_val

    def publisher_country_code(pub_type_name, msg, default_val=default_value) -> str:
        if pub_type_name == 'NonResidentCompany':
            return get_iter_default(msg, 'publisher.data.countryCodeNum')
        else:
            return default_val

    def publisher_country(pub_type_name, msg, default_val=default_value) -> str:
        if pub_type_name == 'NonResidentCompany':
            return get_iter_default(msg, 'publisher.data.country')
        else:
            return default_val

    publisher_type_name = get_iter_default(message, 'publisher.type')

    sat_message_info = (
        # HUB_Message
        guid,  # 'MESSAGE_GUID'
        # SAT_MessageInfo
        get_iter_default(message, 'datePublish'),  # 'REC_EFFECTIVE_FROM'
        get_iter_default(message, 'type.name'),  # 'Message_Type'
        get_iter_default(message, 'type.description'),  # 'Message_TypeDescription'
        get_iter_default(message, 'datePublish'),  # 'Message_PublishDate'
        publisher_type(publisher_type_name),  # 'Publisher_Type'
        publisher_type_name,  # 'Publisher_TypeName'
        publisher_fullname(publisher_type_name, message),  # 'Publisher_FullName'
        publisher_name_lat(publisher_type_name, message),  # 'Publisher_NameLat'
        publisher_fio(publisher_type_name, message),  # 'Publisher_FIO'
        publisher_inn(publisher_type_name, message),  # 'Publisher_INN'
        publisher_ogrn(publisher_type_name, message),  # 'Publisher_OGRN'
        publisher_egrul_address(publisher_type_name, message),  # 'Publisher_EGRULAddress'
        publisher_country_code(publisher_type_name, message),  # 'Publisher_CountryCode'
        publisher_country(publisher_type_name, message),  # 'Publisher_Country'
        get_iter_default(message, 'number'),  # 'Message_Number'
        get_iter_default(message, 'dateDisclosure'),  # 'Message_DisclosureDate'
        get_iter_default(message, 'lockReason'),  # 'Message_LockReason'
        get_iter_default(message, 'annulmentMessage.guid'),  # 'Message_AnnulmentGUID'
        get_iter_default(message, 'annulmentMessage.number'),  # 'Message_AnnulmentNumber'
        get_iter_default(message, 'notaryInfo.name'),  # 'NotaryFIO'
        get_iter_default(message, 'notaryInfo.title'),  # 'NotaryPosition'
        get_iter_default(message, 'arbitrManagerInfo.name'),  # 'ArbitrManagerName'
        '2.5',  # 'Version'
        # SAT_M_MessageAdditionalInfo
        default_value,  # 'Type'
        default_value,  # 'FullName'
        default_value,  # 'INN'
        default_value,  # 'OGRN'
        get_iter_default(message, 'contentAdditionalInfo.message.guid')  # 'PreviousMessage'
    )

    sat_message_xml = (
        guid,  # 'MESSAGE_GUID'
        get_iter_default(message, 'content'),  # 'Message_Body'
    )
    # SAT_M_MessageFileInfos
    filesInfos = get_iter_default(message, 'filesInfo')
    if filesInfos not in [default_value]:
        sat_m_message_file_infos = []
        for filesInfo in filesInfos:
            sat_m_message_file_infos.append((
                guid,  # 'MESSAGE_GUID'
                get_iter_default(filesInfo, 'name'),  # 'File_Name'
                get_iter_default(filesInfo, 'guid'),  # 'File_Hash'
                get_iter_default(filesInfo, 'size')  # 'File_Size'
            ))
    else:
        sat_m_message_file_infos = (guid, default_value, default_value, default_value)

    # SAT_M_LinkedMessages
    linked_messages = get_iter_default(message, 'linkedMessages')
    if linked_messages not in [default_value]:
        sat_m_linked_messages = []
        for linked_message in linked_messages:
            sat_m_linked_messages.append((
                guid,  # 'MESSAGE_GUID'
                get_iter_default(linked_message, 'guid'),  # LNK_Message_GUID'
                get_iter_default(linked_message, 'number'),  # 'LNK_Message_Number'
                get_iter_default(linked_message, 'type.name'),  # 'LNK_Message_Type'
                get_iter_default(linked_message, 'type.description'),  # 'LNK_Message_TypeDescription'
                get_iter_default(linked_message, 'datePublish'),  # 'LNK_Message_PublishDate'
                get_iter_default(linked_message, 'annulmentMessage.guid'),  # 'LNK_MessageAnnullment_Guid'
                get_iter_default(linked_message, 'annulmentMessage.number'),  # 'LNK_MessageAnnullment_Number'
                get_iter_default(linked_message, 'lockReason'),  # 'LNK_Message_LockReason'
                get_iter_default(linked_message, 'contentMessageGuid')  # 'LNK_PreviosMessageGuid'
            ))
    else:
        sat_m_linked_messages = (
            guid, default_value, default_value, default_value, default_value, default_value, default_value,
            default_value, default_value, default_value)

    return sat_message_info, sat_message_xml, sat_m_message_file_infos, sat_m_linked_messages


def join_csv_parts_into_single():
    def join_csv_parts(file_mask):
        data_lst = []
        files = glob(fr"c:\PyProjectsExt\grab_and_parse\{file_mask}*.csv_part")

        for xf in files[:1]:
            with open(xf, encoding='utf-8') as file:
                data = [x for x in file]
                data_lst.append(data[0])

        for xf in files:
            with open(xf, encoding='utf-8') as file:
                data = [x for x in file]
                for x in data[1:]:
                    data_lst.append(x)

        with open(f"c:\PyProjectsExt\grab_and_parse\{file_mask}.csv", 'w', encoding='utf-8') as file:
            for d in data_lst:
                file.write(d)

    files_csv = glob(fr"c:\PyProjectsExt\grab_and_parse\*.csv")

    try:
        for f in files_csv:
            remove(f)
    except FileNotFoundError:
        print("Can't remove files")

    for i in ['linked_messages', 'message_file_infos', 'message_info', 'message_xml']:
        join_csv_parts(i)


QTY_OF_RECS_TO_PUSH = 100

if __name__ == '__main__':
    jwt_token = get_JWT_token()
    print(f'JWT token: {jwt_token}')
    sleep(2)
    files = glob(fr"c:\PyProjectsExt\grab_and_parse\*.csv_part")
    try:
        for file in files:
            remove(file)
    except FileNotFoundError:
        print("Can't remove files")

    guids = get_message_list_session_main(jwt_token, '2020-10-12T01:56:47', '2020-10-15T16:50:54')

    get_the_message_main(jwt_token, guids)
    join_csv_parts_into_single()
