#!/usr/bin/env python
#coding: utf8
import time
import heapq
import logging
from datetime import datetime, timedelta

import requests
import MySQLdb


MYSQL_HOST = 'localhost'
MYSQL_USERNAME = ''
MYSQL_PASSWORD = ''
MYSQL_TABLE = 'savecb'
LOGFILE = '/var/log/savecbd.log'

MORE_PAGE_URL = 'http://www.cnbeta.com/more.htm?type=all&page=%s'
COMMENTS_URL = 'http://www.cnbeta.com/comment.htm?op=info&page=1&sid=%s'
INSERT_HC_SQL = "INSERT INTO cnbeta_cbhcomment (sid, ranking, name, time, \
                 content, up, down) VALUES (%s,%s,%s,%s,%s,%s,%s)"

posts = []  # A heap of [(sid, time), ...]
max_sid = 0  # The newest post on heap.


def update_post_list():
    global max_sid
    logging.info('Updating list...')
    found_bottom = False
    page = 0
    m = max_sid
    while not found_bottom:
        page += 1  # Start form page 1
        json = requests.get(MORE_PAGE_URL % page).json()
        for p in json['result']:
            sid = int(p['sid'])
            t = datetime.strptime(p['time'], '%Y-%m-%d %H:%M:%S')
            if sid <= max_sid or datetime.now() - t >= timedelta(days=1):
                found_bottom = True
                continue
            if sid > m:
                m = sid
            heapq.heappush(posts, (sid, t))
    max_sid = m


def saveit(sid, c):
    logging.info('Saving %s...' % sid)
    result = requests.get(COMMENTS_URL % sid).json()['result']
    if 'cmntstore' not in result:
        logging.info('No comment found.')
        return
    comments = result['cmntstore']
    hot_comments = [comments[h['tid']] for h in result['hotlist']]

    ranking = 0
    for hc in hot_comments:
        ranking += 1
        c.execute(INSERT_HC_SQL, (sid, ranking, hc['name'], hc['date'],
                                  hc['comment'], hc['score'], hc['reason']))
        # args: (sid, ranking, name, time, content, up, down)


def main():
    logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename=LOGFILE,
                    filemode='a+')
    conn = MySQLdb.connect(MYSQL_HOST, MYSQL_USERNAME, MYSQL_PASSWORD,
                           MYSQL_TABLE, charset='utf8')
    c = conn.cursor()
    while True:
        if not posts:
            update_post_list()
            if not posts:
                time.sleep(600)
                continue
        sid, post_time = heapq.heappop(posts)
        sleep_time = post_time + timedelta(days=1) - datetime.now()
        if sleep_time <= timedelta(minutes=1):
            continue
        if sleep_time > timedelta(minutes=3):
            logging.info('Sleep %s min for %s.' \
                         % (sleep_time.total_seconds() // 60, sid))
            time.sleep(sleep_time.total_seconds() - 60)
            conn.ping(True)  # Ensure MySQL connected.
        saveit(sid, c)
        conn.commit()


if __name__ == '__main__':
    for i in range(32):
        try:
            main()
        except Exception as e:
            logging.exception(e)
            time.sleep(60)

