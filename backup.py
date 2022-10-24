#!/usr/bin/python

#module impots
import mariadb
import time
import logging
import logging.handlers
import sys
import os
import shutil
import datetime

from os import listdir
from os.path import isfile, join
from logging.handlers import RotatingFileHandler


##TODO
# MEnsajes en BBDD para ver por donde llega.
# Gestion de errores

LOG = "/mnt/backup/berry/log//backup.log"
BASE_PATH_TO_BACKUP ="/mnt/backup/backup"
BASE_PATH_TO_RECOVER = "/mnt/NAS/"
LENGTH_BASE_PATH_TO_RECOVER = len(BASE_PATH_TO_RECOVER)


def recoverFiles(path,log):
    log.info(f'-- recoverFiles {path}')
    aFile = []
    for root, dirs, files in os.walk(path):
        for name in files:
            path = os.path.join(root, name)
            if os.path.isfile(path):
                longdate = os.path.getmtime(path)
                date =  datetime.datetime.fromtimestamp(longdate)
                aFile.append([path,date])

    if(logging.getLogger().level == logging.DEBUG):
        for _file, _time in aFile:
            log.debug(f'<< {_file} >> <<< {_time} .... ')
    return aFile

def configureLog():
    handler = RotatingFileHandler(LOG, maxBytes=8000,backupCount=3)
    formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s:%(module)s:%(message)s')
    handler.setFormatter(formatter)
    root = logging.getLogger()
    root.setLevel(os.environ.get("LOGLEVEL", "INFO"))
    root.addHandler(handler)
    return root

miLog = configureLog()

def createConection(log):
    log.info("-- createConexion")
    return mariadb.connect(
        user="backup",
        password="sT@Uz6r26G**",
        host="192.168.0.197",
        port=3307,
        database="backup")


def recoverDirectories(conn, log):
    log.info(f"-- RecoverDirectories")
    cur = conn.cursor()
    cur.execute("SELECT id, path FROM DirectoryToBackup WHERE path is not null AND path != '' AND id > (SELECT state FROM Status WHERE stateDesc = 'directory')")
    files = cur.fetchall()
    cur.close()
    return files

def insertIntoTemp(conn, files, log):
    log.info(f'--isertIntoTemp')
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE TmpFiles")
    for aFile in files:
        path = aFile[0]
        date = aFile[1]
        cur.execute("INSERT INTO TmpFiles (path, modifiedDate) values (%s, %s)", (path,date))
    conn.commit()
    cur.close()


def recoverBackupPath(path,log):
    log.debug(f"--recoverBackupPath {path}")
    return  BASE_PATH_TO_BACKUP+"/"+path[LENGTH_BASE_PATH_TO_RECOVER:]
    
def deleteFilesRemoved(conn,log):
    log.info("--deleteFilesRemoved")
    cur = conn.cursor()
    cur.execute("SELECT b.path FROM TmpFiles t RIGHT JOIN FilesBackup b ON t.path = b.path WHERE t.path IS NULL")
    paths = cur.fetchall()
    for path in paths:

        log.info(f'To delte {path[0]}')
        try:
           cur.execute(f"DELETE FROM FilesBackup WHERE path = '{path[0]}'")
           pathToDelete =  recoverBackupPath(path[0],log)
           if(os.path.isfile(pathToDelete)):
               log.info(f"Delete file {pathToDelete}")
               os.remove(pathToDelete)
           conn.commit()
        except Exception as e:
           log.error(f"file {path[0]} cant be deleted")
           log.error(e)
           conn.rollback()
    cur.close()


def insertNewFilesToBackup(conn,log):
    log.info("--insertNewFilesToBackup")
    cur = conn.cursor()
    cur.execute("SELECT t.path, t.modifiedDate FROM TmpFiles t LEFT JOIN FilesBackup b ON t.path = b.path WHERE b.path IS NULL")
    paths = cur.fetchall()
    for path in paths:
        log.debug(f"To insert {path[0]} -- {path[1]}")
        cur.execute("INSERT INTO FilesBackup (path, modifiedDate) values (%s, %s)", (path[0], path[1]))
    conn.commit()
    cur.close()


def copyFiles(conn,log):
    log.info("--copyFiles")
    cur = conn.cursor()
    cur.execute("SELECT t.path FROM TmpFiles t, FilesBackup b where t.path = b.path AND date_add(t.modifiedDate, INTERVAL 1 DAY )  >= b.modifiedDate")
    pathos = cur.fetchall()
    cur.close()
    for path in paths:
        try:
            cur2 = conn.cursor()
            cur2.execute = ("UPDATE FilesBackup SET modifiedDate = %s WHERE path=%s",(now(),path[0]))
            destination = recoverBackupPath(path[0],log)
            log.debug(f"* Copy from {path[0]} to {destination}")
            os.makedirs(os.path.dirname(destination), exist_ok=True)
            shutil.copy(path[0], destination)
            conn.commit()
        except Excpetio as e:
            log.error(f"Error making mackup for [{path[0]}]")
            log.error(e)
            conn.rollback()

def updateDirectoryStatus(conn,id,path):
    log.info("--updateStatus")
    cur = conn.cursor()
    cur.execute(f"UPDATE Status SET state = {id} WHERE stateDesc='directory'")
    conn.commit()

def logMessage(message, level):
    match level:
        case "info":
            miLog.info(message)
        case "error":
            miLog.error(message)
        case "debug":
            miLog.debug(message)
        case _:
            log.error(f"Log lever incorrect {level}")


##MAIN
log = configureLog()
log.info("Init")
try:
    conn = createConection(log)
    for id, path in recoverDirectories(conn,log):
        log.debug(f"* Listing {id} - {path}")
        fullPath = BASE_PATH_TO_RECOVER + path
        files = recoverFiles(fullPath, log)
        insertIntoTemp(conn,files,log)
        deleteFilesRemoved(conn,log)
        insertNewFilesToBackup(conn,log)
        copyFiles(conn,log)
        updateDirectoryStatus(conn,id,log)
    updateDirectoryStatus(conn,-1,log)
    conn.close()
except Exception as e:
    log.error(e)
log.info("Finish")
