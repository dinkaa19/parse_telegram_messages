from telethon import TelegramClient, sync
from telethon.tl.functions.channels import GetParticipantsRequest
from telethon.tl.types import ChannelParticipantsSearch
from telethon import functions, types
from collections import OrderedDict
from os import path, stat, remove, makedirs
import json
import pandas as pd
import sys
import argparse
import socks


def createParser():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--chat', type=str)
    parser.add_argument('-k', '--keys', type=open)
    return parser


def find_message(chat, k_list, data_frame):
    count = 0
    print('start parsing')
    for key_search in k_list:
        print(f'parsing - {key_search}')
        for message in client.iter_messages(chat, search=key_search):
            # find id_user
            data_frame.loc[count, 'id_user'] = message.from_id
            # find id_message
            data_frame.loc[count, 'id_message'] = message.id
            # find id_group
            data_frame.loc[count, 'id_group'] = message.to_id.channel_id
            # find date
            data_frame.loc[count, 'date'] = message.date
            # find text
            data_frame.loc[count, 'text'] = message.message
            # keys
            data_frame.loc[count, 'keys'] = message.message
            # url_chat
            data_frame.loc[count, 'url_chat'] = chat
            count += 1
    # find info User
    for user in data_frame.id_user:
        index_user = list(data_frame.id_user).index(user)
        # first_name
        data_frame.loc[index_user, 'first_name'] = client(
            functions.users.GetFullUserRequest(id=int(user))).user.first_name
        # last_name
        data_frame.loc[index_user, 'last_name'] = client(
            functions.users.GetFullUserRequest(id=int(user))).user.last_name
        # username
        data_frame.loc[index_user, 'username'] = client(functions.users.GetFullUserRequest(id=int(user))).user.username

def main():
    parser = createParser()
    namespace = parser.parse_args(sys.argv[1:])
    keys = namespace.keys.readlines()
    keys = [x.strip() for x in keys]
    print('read txt')

    CHAT_LINK = namespace.chat
    API_ID = 1307728
    API_HASH = '0fdacb025cf6bfa8585007b565920454'

    print('Start to connect')
    client = TelegramClient(None, API_ID, API_HASH, proxy= (socks.SOCKS5, '45.77.91.16', 43855, True, 'ATRXjm', 'aTB5yt'))
    print('client.start()')
    client.start()


    chat_df = pd.DataFrame(
        columns={'first_name', 'last_name', 'username', 'id_user', 'id_message', 'id_group', 'date', 'text', 'keys',
                 'url_chat'})

    find_message(chat=CHAT_LINK, k_list=keys, data_frame=chat_df)
    chat_df.to_csv('chat_df.csv')


if __name__ == "__main__":
    main()