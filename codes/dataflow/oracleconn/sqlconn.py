#!/usr/bin/python
# -*- coding: utf-8 -*-
import socket

def sqlGetIp():
    hostname = socket.gethostname()
    ip = socket.gethostbyname(hostname)
    return ip

def sqlconn():
    import cx_Oracle
    if sqlGetIp() == '10.17.12.8':
        dsn = cx_Oracle.makedsn('10.17.12.57', '1521', 'wind')
        conn = cx_Oracle.connect('tpaquery','Tpam1234',dsn)
    else:
        dsn = cx_Oracle.makedsn('10.17.11.26', '1521', 'WIND')
        conn = cx_Oracle.connect('windread','windread',dsn)
    return conn

def sqlconnJYDB():
    import cx_Oracle
    if sqlGetIp() == '10.17.12.8':
        dsn = cx_Oracle.makedsn('10.17.12.57', '1521', 'jyhqdb')
        conn = cx_Oracle.connect('tpaquery','Tpam1234',dsn)
    else:
        dsn = cx_Oracle.makedsn('10.17.3.196', '1521', 'jyhqdb')
        conn = cx_Oracle.connect('lhtz_query','lhtz_query123',dsn)
    return conn

def sqlconnJYDBHQTEST():
    import cx_Oracle
    dsn = cx_Oracle.makedsn('10.17.12.57', '1521', 'jyhqdb')
    conn = cx_Oracle.connect('tpaquery','Tpam1234',dsn)
    return conn

def sqlconnTPADC():
    import cx_Oracle
    # if sqlGetIp() == '10.17.12.8':
    #     dsn = cx_Oracle.makedsn('10.17.12.131', '1521', 'TPADC')
    # else:
    #     dsn = cx_Oracle.makedsn('10.17.18.51', '1521', 'TPADC')
    dsn = cx_Oracle.makedsn('10.17.18.51', '1521', 'TPADC')
    conn = cx_Oracle.connect('tpa_combi','combi123',dsn)
    return conn

def sqlexec(conn,sqlq):
    connect =conn.cursor()                                           #获取cursor
    exesql=connect.execute(sqlq)                   #使用cursor进行各种操作
    data = exesql.fetchall()
    connect.close()                                                 #关闭cursor
    return data
