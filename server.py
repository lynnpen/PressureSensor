#!/usr/bin/env python
# coding: utf-8
import sys
import os
import socket
import threading
import struct
import signal
import pymysql
import time
import logging
import SocketServer

logging.basicConfig(filename='server.log',level=logging.INFO, filemode = 'a+', format = '%(asctime)s: %(message)s')

connection = pymysql.connect(host='localhost',
                             user='kh_sssos',
                             password='kh_sssos',
                             charset='utf8mb4',
                             db='kh_sssos')

HOST = '0.0.0.0'
PORT = 9150
client_addr = []
client_socket = []

class ThreadedTCPRequestHandler(SocketServer.BaseRequestHandler):
    def setup(self):
        self.ip = self.client_address[0].strip()
        self.port = self.client_address[1]
        self.cur_thread = threading.current_thread()
        self.request.settimeout(90)
        logging.info('[%s]: client=%s|msg=client is connect!' % (self.cur_thread.name, self.ip))
        client_addr.append(self.client_address)
        client_socket.append(self.request)

    def handle(self):
        while True:
            try:
                data = self.request.recv(1024)
            except socket.timeout:
                logging.info('[%s]: client=%s|msg=client is timeout, will disconnect' % (self.cur_thread.name, self.ip))
                break
                #self.request.sendall(response)
            if data == 'exit' or not data:
                break
            elif len(data) == 2:
                a = struct.unpack('>H', data)
                if a[0] == 37380:
                    logging.info('[%s]: client=%s|msg=pong!' % (self.cur_thread.name, self.ip))
            elif len(data) == 14:
                tm = int(time.time())
                flag, title, tpt, mpa1, mpa2, vol, ext1, ext2 = struct.unpack('>HHHHHBBH', data)
                if flag == 65260:  # feec
                    tpt_calc = lambda tpt: -(65535-tpt+1)/100.0 if tpt > 32767 else tpt/100.0
                    mpa1 = mpa1/1000.0
                    mpa2 = mpa2/1000.0
                    vol_calc = lambda vol: '---' if vol == 0 else (vol+200)/100.0
                    #print 'title: %d, temp: %f, mpa1: %f, mpa2: %f, vol: %f' % (title, tpt, mpa1, mpa2, vol)
                    logging.info('[%s]: client=%s|title=%d|temp=%f|mpa1=%f|mpa2=%f|vol=%f' % (self.cur_thread.name, \
                            self.ip, title, tpt_calc(tpt), mpa1, mpa2, vol_calc(vol)))
                    with connection.cursor() as cursor:
                        sql = 'select * from today_device where title=%s'
                        cursor.execute(sql, (title, ))
                        result = cursor.fetchone()
                        if result:
                            sql = 'update today_device set lasttime=%s,device_status=1 where title=%s'
                            cursor.execute(sql, (tm, title))
                        else:
                            sql = 'insert into today_device set lasttime=%s,addtime=%s,device_status=1,title=%s'
                            cursor.execute(sql, (tm, tm, title))
                        connection.commit()
                        sql = 'select id from today_device where title=%s'
                        cursor.execute(sql, (title, ))
                        dev_id = cursor.fetchone()
                        sql = 'insert into today_time_data set device_id=%s,addtime=%s,temperature="%s",mpa_one="%s" \
                                ,mpa_two="%s",voltage="%s"'
                        cursor.execute(sql, (dev_id, tm, tpt_calc(tpt), mpa1, mpa2, vol_calc(vol)))
                        connection.commit()
            time.sleep(0.1)

    def finish(self):
        logging.info('[%s]: client=%s|msg=client is disconnected' % (self.cur_thread.name, self.ip))
        client_addr.remove(self.client_address)
        client_socket.remove(self.request)

class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    allow_reuse_address = True

if __name__ == "__main__":
    server = ThreadedTCPServer((HOST, PORT), ThreadedTCPRequestHandler)
    ip, port = server.server_address
    server_thread = threading.Thread(target=server.serve_forever)
    server_thread.daemon = False
    server_thread.start()

    try:
        signal.pause()
    except:
        logging.info('[main]: server going to stop!')
        server.shutdown()
        server.server_close()
