#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Emergency backup script. Backups and compress databases as bz2
using pg_dump. This script is intended to be used in servers where
any other backup script fails
"""

import os
import shutil
import argparse
import datetime
import logging
import tarfile
import bz2
from sh import pg_dump


parser = argparse.ArgumentParser()
action = parser.add_mutually_exclusive_group(required=True)
action.add_argument("--dbs", help="Database name")
action.add_argument("--move", help="Move backups from temp dir to backup dir",
                    action="store_true")
parser.add_argument("-d", "--backup-dir", help="Path for backup file",
                    default=".")
parser.add_argument("--origin-dir", help="Path of temporary backup dir",
                    default=None)
parser.add_argument("--reason", help="Reason to make this backup",
                    default=False)
parser.add_argument("--logfile", help="File where log will be saved",
                    default=None)
parser.add_argument("--log-level", help="Level of logger. INFO as default",
                    default="info")

args = parser.parse_args()
level = getattr(logging, args.log_level.upper(), None)
logging.basicConfig(level=level,
                    filename=args.logfile,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("Extra_backup")

database = args.dbs
move = args.move
backup_dir = args.backup_dir
reason = args.reason
origin_dir =args.origin_dir


def clean_files(files):
    """ Remove unnecesary and temporary files

    Args:
        files (list): A list of absolute or relatove paths thar will be erased
    """
    for fname in files:
        if os.path.isfile(fname):
            os.remove(fname)
        elif os.path.isdir(fname):
            shutil.rmtree(fname)


def compress_files(name, files, dest_folder=None):
    """ Compress a file, set of files or a folder in tar.bz2 format

    Args:
        name (str): Desired file name w/o extenssion
        files (list): A list with the absolute o relative path to the files
                      that will be added to the compressed file
        dest_folder (str): The folder where will be stored the compressed file
    """
    if not dest_folder:
        dest_folder = '.'
    logger.debug("Generating compressed file: %s in %s folder",
                 name, dest_folder)
    full_name = os.path.join(dest_folder, '%s.tar.bz2' % name)
    bz2_file = bz2.BZ2File(full_name, mode='w', compresslevel=9)
    with tarfile.open(mode='w', fileobj=bz2_file) as tar_bz2_file:
        for fname in files:
            tar_bz2_file.add(fname,
                             os.path.join(name, os.path.basename(fname)))
    bz2_file.close()
    return full_name


def dump_database(dest_folder, database_name):
    """ Dumps database using Oerplib in Base64 format

    Args:
        dest_folder (str): Folder where the function will save the dump
        database_name (str): Database name that will be dumped
        super_user_pass (str): Super user password to be used
                               to connect with odoo instance
        host (str): Host name or IP address to connect
        port (int): Port number which Odoo instance is listening to
    Returns:
        The full dump path and name with .b64 extension
    """
    logger.debug("Dumping database %s into %s folder",
                 database_name, dest_folder)
    dump_name = os.path.join(dest_folder, "database_dump.sql")
    pg_dump(database_name, no_owner=True, file=dump_name)
    return dump_name


def backup_database(database_name, dest_folder,
                    reason=False, tmp_dir="/tmp"):
    """ Receive database name and back it up

    Args:
        database_name (str): The database name
        dest_folder (str): Folder where the backup will be stored
        user (str): Super user login in the instance
        password (str): Super user password
        host (str): Hostname or ip where the Odoo instance is running
        port (int): Port number where the instance is llinstening
        reason (str): Optional parameter that is used in case
                      there is a particular reason for the backup
        tmp_dir (str): Optional parameter to store the temporary working dir,
                      default is /tmp

    Returns:
        Full path to the backup
    """
    files = []
    if reason:
        file_name = '%s_%s_%s_%s'% \
                    (database_name, "sql", reason,
                     datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
    else:
        file_name = '%s_%s_%s'%(database_name, "sql",
                                datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
    logger.info("Dumping database")
    dbase = dump_database(tmp_dir, database_name)
    files.append(os.path.join(tmp_dir, dbase))
    logger.info("Compressing dump %s", dbase)
    full_name = compress_files(file_name, files, dest_folder)
    clean_files(files)
    return os.path.abspath(full_name)


def move_files(from_dir, to_dir):
    """Moves files between directories

    Args:
        from_dir: Origin directory
        to_dir: Destination directory
    """
    for each in os.listdir(from_dir):
        lfile = os.path.join(to_dir, os.path.basename(each))
        shutil.copy(each, lfile)
    for each in os.listdir(from_dir):
        os.remove(each)


if __name__ == '__main__':
    if database:
        logger.info("Starting backup for %s", database)
        try:
            db_name = backup_database(database, backup_dir, reason)
            logger.info("Backup complete. Available in: %s", db_name)
        except Exception as e:
            logger.error("Backup failed: %s", e)
    if move:
        logger.info("Starting files' move")
        move_files(origin_dir, backup_dir)
