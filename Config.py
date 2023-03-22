import configparser
import os
from datetime import datetime

import ftputil
import pymysql
from ftputil.error import FTPOSError


class __DatabaseManager:
    def __init__(self):
        self.conn = None
        self.cursor = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, traceback):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            if exc_type:
                self.conn.rollback()
            else:
                self.conn.commit()
            self.conn.close()
        self.closedb = True

    def __del__(self):
        try:
            if not self.closedb:
                if self.cursor:
                    self.cursor.close()
                if self.conn:
                    self.conn.close()
                self.closedb = True
        except (pymysql.Error, Exception):
            pass


class DownLog(__DatabaseManager):
    def __init__(self):
        super().__init__()
        self.mysqlinfo = MysqlInfo(section='LocalServer')
        self.ftpinfo = FTPInfo()
        self.mysqlinfo.db_name = 'mroparse'
        self.mysqlinfo.tb_name = "downlog"
        self._connect()
        self.closedb = False
        self.errlog = ErrorLog('DownLog')

    def _connect(self):
        try:
            self.conn = pymysql.connect(
                host=self.mysqlinfo.host,
                port=self.mysqlinfo.port,
                user=self.mysqlinfo.user,
                password=self.mysqlinfo.passwd,
                database=self.mysqlinfo.db_name,
                autocommit=True
            )
            self.cursor = self.conn.cursor()
            self._create_table()
        except pymysql.Error as e:
            self.cursor = None
            self.conn = None
            self.errlog.add_error('_connect', f"Error connecting to MySQL: {e}")
            raise f"Error connecting to MySQL: {e}"

    def _create_table(self):
        # 判断数据表是否存在，不存在则创建
        try:
            self.cursor.execute(
                f"SELECT table_name FROM information_schema.tables WHERE table_name='{self.mysqlinfo.tb_name}'")
            if not self.cursor.fetchone():
                self.cursor.execute(f"CREATE TABLE {self.mysqlinfo.tb_name} ("
                                    f"id INT PRIMARY KEY AUTO_INCREMENT, "
                                    f"ftp_name VARCHAR(255) NOT NULL, "
                                    f"filepath VARCHAR(255) NOT NULL, "
                                    f"log_time DATETIME NOT NULL)")

                self.cursor.execute(f"CREATE INDEX filepath_index ON {self.mysqlinfo.tb_name} (filepath)")
                self.cursor.execute(f"CREATE INDEX ftp_name_index ON {self.mysqlinfo.tb_name} (ftp_name)")
                self.conn.commit()

        except pymysql.Error as e:
            self.errlog.add_error('_create_table', f"Error creating table: {e}")
            raise f"Error creating table: {e}"

    def isexists(self, filepath):
        if not self.cursor:
            self._connect()

        try:
            self.cursor.execute(f"SELECT * FROM {self.mysqlinfo.tb_name} WHERE ftp_name = %s AND filepath = %s",
                                (self.ftpinfo.ftp_name, filepath))
            result = self.cursor.fetchone()
            return bool(result) if result else False
        except pymysql.Error as e:
            self.errlog.add_error('isexists', f"check file isexists: {filepath}; error:{e}")
            return False

    def savelog(self, filepath):
        if not self.cursor:
            self._connect()
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            self.cursor.execute(
                f"INSERT INTO {self.mysqlinfo.tb_name} (ftp_name, filepath, log_time) VALUES (%s, %s, %s)",
                (self.ftpinfo.ftp_name, filepath, now))
        except pymysql.IntegrityError:
            return True
        except pymysql.Error as e:
            self.errlog.add_error('savelog', f"Error savelog file{filepath}; error: {e}")
            return False
        return True

    def dellog_by_time(self, time=None):
        if not self.cursor:
            self._connect()
        if time is None:
            time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            self.cursor.execute(f"DELETE FROM {self.mysqlinfo.tb_name} WHERE log_time < %s", (time,))
        except pymysql.Error as e:
            self.errlog.add_error('dellog_by_time', f"Error dellog: {e}")
            return False
        return True


class ErrorLog:
    def __init__(self, class_name=None):
        self.class_name = class_name
        self.mysql_info = MysqlInfo()
        self.mysql_info.db_name = 'mroparse'
        self._connect()
        self.closedb = False

    def _connect(self):
        try:
            self.conn = pymysql.connect(
                host=self.mysql_info.host,
                port=self.mysql_info.port,
                user=self.mysql_info.user,
                password=self.mysql_info.passwd,
                database=self.mysql_info.db_name
            )
            self.cursor = self.conn.cursor()
            self._create_table()
        except pymysql.Error as e:
            self.cursor = None
            self.conn = None
            raise Exception(f"Error connecting to MySQL: {e}")

    def _create_table(self):
        try:
            self.cursor.execute("CREATE TABLE IF NOT EXISTS ErrorLog ("
                                "id INTEGER PRIMARY KEY AUTO_INCREMENT, "
                                "log_time DATETIME NOT NULL, "
                                "from_class VARCHAR(255) NOT NULL, "
                                "from_func VARCHAR(255) NOT NULL,"
                                "error_text TEXT NOT NULL)")
            self.conn.commit()
            try:
                self.cursor.execute("""
                            CREATE INDEX log_time_index ON ErrorLog (log_time);
                        """)
                self.conn.commit()
            except pymysql.Error:
                pass
        except pymysql.Error as e:
            raise Exception(f"Error creating table: {e}")

    def add_error(self, from_func, error_text):
        from_class = self.class_name
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            self.cursor.execute(
                "INSERT INTO ErrorLog (log_time, from_class, from_func, error_text) VALUES (%s, %s, %s, %s)",
                (now, from_class, from_func, str(error_text).replace("'", "‘")))
            self.conn.commit()
            print("add", error_text)
        except pymysql.Error as e:
            raise Exception(f"Error adding error record: {e}")

    def delete_errors_before(self, start_time, end_time):
        try:
            self.cursor.execute("DELETE FROM ErrorLog WHERE log_time >= %s AND log_time <= %s", (start_time, end_time))
            self.conn.commit()
        except pymysql.Error as e:
            raise Exception(f"Error deleting error records before time: {e}")

    def query_errors_by_time(self, start_time, end_time):
        try:
            self.cursor.execute("SELECT * FROM ErrorLog WHERE log_time >= %s AND log_time <= %s",
                                (start_time, end_time))
            results = self.cursor.fetchall()
            return results
        except pymysql.Error as e:
            raise Exception(f"Error querying error records by time: {e}")


class MysqlInfo:
    def __init__(self, cfg_path='./configure/mysql.ini', section="LocalServer"):
        self.__cfg_path = cfg_path
        os.makedirs(os.path.dirname(self.__cfg_path), exist_ok=True)
        self.__config = configparser.ConfigParser()
        self.section = section
        try:
            if not os.path.exists(self.__cfg_path):
                self.__config[self.section] = {
                    'host': '',
                    'port': -1,
                    'user': '',
                    'passwd': ''
                }
                with open(self.__cfg_path, 'w') as f:
                    self.__config.write(f)
            self.__config.read(self.__cfg_path)
            self.host = self.__config.get(self.section, 'host')
            self.port = int(self.__config.get(self.section, 'port'))
            self.user = self.__config.get(self.section, 'user')
            self.passwd = self.__config.get(self.section, 'passwd')
            self.db_name = None
            self.tb_name = None
        except (FileNotFoundError, configparser.Error, ValueError) as e:
            raise Exception(f"Error initializing MysqlInfo: {e}")

    def update(self, host=None, port=None, user=None, passwd=None):
        try:
            self.__config.set(self.section, 'host', host)
        except configparser.NoSectionError:
            self.__config.read(self.__cfg_path)
        try:

            if host is not None:
                self.__config.set(self.section, 'host', host)
                self.host = host
            if port is not None:
                self.__config.set(self.section, 'port', str(port))
                self.port = int(port)
            if user is not None:
                self.__config.set(self.section, 'user', user)
                self.user = user
            if passwd is not None:
                self.__config.set(self.section, 'passwd', passwd)
                self.passwd = passwd

            with open(self.__cfg_path, 'w') as f:
                self.__config.write(f)
        except (FileNotFoundError, configparser.Error, ValueError) as e:
            raise Exception(f"Error initializing MysqlInfo: {e}")

    def read(self):
        try:
            self.__config.read(self.__cfg_path)
            self.host = self.__config.get(self.section, 'host')
            self.port = int(self.__config.get(self.section, 'port'))
            self.user = self.__config.get(self.section, 'user')
            self.passwd = self.__config.get(self.section, 'passwd')
        except (configparser.Error, ValueError) as e:
            raise Exception(f"Error reading MysqlInfo: {e}")

    def check(self):
        try:
            conn = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.passwd,
                database=self.db_name
            )
            conn.close()
        except (pymysql.Error, Exception) as e:
            raise Exception(f"Error checking MysqlInfo: {e}")


class FTPInfo:
    def __init__(self, cfg_path=os.path.join(os.getcwd(), 'configure', 'ftpinfo.ini')):
        self.__cfg_path = cfg_path
        os.makedirs(os.path.dirname(cfg_path), exist_ok=True)
        self.__config = configparser.ConfigParser()
        self.errorlog = ErrorLog('FTPInfo')
        try:
            if not os.path.exists(cfg_path):
                self.__config['FTPInfo'] = {
                    'ftp_name': '',
                    'host': '',
                    'port': -1,
                    'user': '',
                    'passwd': '',
                    'sync_path': '',
                    'down_path': '',
                    'filter': ''
                }
                with open(cfg_path, 'w') as f:
                    self.__config.write(f)
            self.__config.read(cfg_path)
            self.ftp_name = self.__config.get('FTPInfo', 'ftp_name')
            self.host = self.__config.get('FTPInfo', 'host')
            self.port = int(self.__config.get('FTPInfo', 'port'))
            self.user = self.__config.get('FTPInfo', 'user')
            self.passwd = self.__config.get('FTPInfo', 'passwd')
            self.sync_path = self.__config.get('FTPInfo', 'sync_path')
            self.down_path = self.__config.get('FTPInfo', 'down_path')
            self.scan_filter = self.__config.get('FTPInfo', 'scan_filter')
        except (configparser.Error, Exception) as e:
            self.errorlog.add_error('init', e)

    def update(self, ftp_name=None, host=None, port=None, user=None, passwd=None, sync_path=None, down_path=None,
               scan_filter=None):
        if ftp_name is not None:
            self.__config.set('FTPInfo', 'ftp_name', ftp_name)
            self.ftp_name = ftp_name
        if host is not None:
            self.__config.set('FTPInfo', 'host', host)
            self.host = host
        if port is not None:
            self.__config.set('FTPInfo', 'port', str(port))
            self.port = int(port)
        if user is not None:
            self.__config.set('FTPInfo', 'user', user)
            self.user = user
        if passwd is not None:
            self.__config.set('FTPInfo', 'passwd', passwd)
            self.passwd = passwd
        if sync_path is not None:
            self.__config.set('FTPInfo', 'sync_path', sync_path)
            self.sync_path = sync_path
        if down_path is not None:
            self.__config.set('FTPInfo', 'down_path', down_path)
            self.down_path = down_path
        if scan_filter is not None:
            self.__config.set('FTPInfo', 'scan_filter', scan_filter)
            self.scan_filter = scan_filter
        try:
            with open(self.__cfg_path, 'w') as f:
                self.__config.write(f)
        except configparser.Error as e:
            self.errorlog.add_error('update', e)
            return False
        return True

    def read(self):
        try:
            self.__config.read(self.__cfg_path)
            self.ftp_name = self.__config.get('FTPInfo', 'ftp_name')
            self.host = self.__config.get('FTPInfo', 'host')
            self.port = int(self.__config.get('FTPInfo', 'port'))
            self.user = self.__config.get('FTPInfo', 'user')
            self.passwd = self.__config.get('FTPInfo', 'passwd')
            self.sync_path = self.__config.get('FTPInfo', 'sync_path')
            self.down_path = self.__config.get('FTPInfo', 'down_path')
            self.scan_filter = self.__config.get('FTPInfo', 'scan_filter')
        except (configparser.Error, ValueError) as e:
            self.errorlog.add_error('read', e)
            return False
        return True

    def check(self):
        try:
            ftputil.FTPHost(self.host, self.user, self.passwd, self.port, timeout=60)
        except (FTPOSError, Exception) as e:
            self.errorlog.add_error('check', e)
            return False
        return True
