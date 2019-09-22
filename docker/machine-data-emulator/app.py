import os
import time
import socket
import pandas as pd
from random import randint
from pathlib import Path

GRAPHOUSE_SERVER = '0.0.0.0'
GRAPHOUSE_PORT = 2003
DELAY = float(os.getenv('DELAY', default='10'))  # secs

METRIC_PATH = 'co2.machine.'
DATA_PATH = Path(__file__).parent / 'sample_data_20k.csv'


def get_msg():
    idx = randint(0, num_rows)
    row = df.iloc[idx]
    msg = (METRIC_PATH + row.index).str.cat(
        list(map(lambda x: ' ' + str(x) + ' -1', row.values))
    ).str.cat(sep='\n') + '\n'
    return msg


def send_msg(message):
    print('message sent')
    sock = socket.socket()
    sock.connect((GRAPHOUSE_SERVER, GRAPHOUSE_PORT))
    sock.sendall(message)
    sock.close()


if __name__ == '__main__':
    df = pd.read_csv(DATA_PATH)
    df.drop(['timestamp', 'datetime', 'date'], axis=1, inplace=True)
    num_rows = df.shape[0]
    while True:
        timestamp = int(time.time())
        start = time.time()
        message = get_msg()
        send_msg(message)
        end = time.time()
        print('Sending message took {:2f}'.format(start - end))
        time.sleep(DELAY)
