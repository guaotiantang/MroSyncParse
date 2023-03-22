import multiprocessing
import os
import time
import ftputil
from ftputil.error import FTPOSError
from Config import FTPInfo, DownLog, ErrorLog


class FtpScanClass:

    def __init__(self, manager_dict):
        self.manager_dict = manager_dict
        self.ftpinfo = FTPInfo()
        self.ftpinfo.read()
        self.ftp = None
        self.errlog = ErrorLog('FtpScanClass')
        self.connect_to_ftp()
        self.db = DownLog()

    def connect_to_ftp(self):
        try:
            self.ftp = ftputil.FTPHost(self.ftpinfo.host,
                                       self.ftpinfo.user,
                                       self.ftpinfo.passwd,
                                       self.ftpinfo.port,
                                       timeout=60)
        except (FTPOSError, Exception) as e:
            self.errlog.add_error('connect_to_ftp', 'Error for Ftp Connect: {}'.format(str(e)))
            raise 'Error for Ftp Connect: {}'.format(str(e))

    def stop(self):
        self.manager_dict['status'] = False

    def scan_newfiles(self):
        ftp_path = self.ftpinfo.sync_path
        scan_filter = self.ftpinfo.scan_filter.split('|')
        new_files = []
        try:
            for root, dirs, files in self.ftp.walk(ftp_path):
                if not self.manager_dict['status']:
                    break
                for name in files:
                    ftp_file = self.ftp.path.join(root, name)
                    if ftp_file.endswith('.zip') and not self.db.isexists(ftp_file):
                        dir_name = self.ftp.path.dirname(ftp_path)
                        if not any(temp_dir in dir_name for temp_dir in scan_filter):
                            file_size = self.ftp.path.getsize(ftp_file)
                            file_mtime = time.time()
                            file_info = (ftp_file, file_size, file_mtime)
                            new_files.append(file_info)
        except (FTPOSError, Exception) as e:
            self.errlog.add_error('scan_newfiles', 'Error occurred while scanning New FTP directory:{}'.format(str(e)))
            return []

        return sorted(new_files, key=lambda f: f[1])

    def save_all_files_log(self):
        self.ftpinfo.read()
        ftp_path = self.ftpinfo.sync_path
        try:
            for root, dirs, files in self.ftp.walk(ftp_path):
                for name in files:
                    ftp_file = self.ftp.path.join(root, name)
                    if ftp_file.endswith('.zip') and not self.db.isexists(ftp_file):
                        self.db.savelog(ftp_file)
        except (FTPOSError, Exception) as e:
            self.errlog.add_error('save_all_files_log', 'Error occurred while scanning All FTP directory {}'.format(str(e)))
            return False
        return True

    def file_download(self, file_info):
        filepath = file_info[0]
        try:
            self.ftpinfo.read()
            filesize = file_info[1]
            scantime = file_info[2]

            with ftputil.FTPHost(self.ftpinfo.host, self.ftpinfo.user, self.ftpinfo.passwd, self.ftpinfo.port,
                                 timeout=60) as ftp:
                ftp_path = os.path.dirname(filepath)
                ftp.chdir(ftp_path)
                download_path = os.path.join(
                    self.ftpinfo.down_path,
                    self.ftpinfo.ftp_name,
                    *os.path.normpath(filepath).split(os.path.sep))

                os.makedirs(os.path.dirname(download_path), exist_ok=True)
                init_size = filesize
                size_change_time = scantime
                # 检测文件大小是否发生变化，如果在一段时间内文件大小未发生变化则开始下载
                while self.manager_dict['status']:
                    cur_size = ftp.stat(filepath).cur_size = ftp.stat(filepath).st_size
                    if cur_size != init_size:
                        init_size = cur_size
                        size_change_time = time.time()
                    elif time.time() - size_change_time >= 9:
                        # 文件大小未发生变化9秒，判定为对方已经上传完成
                        ftp.download(filepath, download_path)
                        return download_path
                    time.sleep(3)

        except (ftputil.error.FTPIOError, Exception) as e:
            self.errlog.add_error('file_download',
                                  'Error occurred while downloading file {}: {}'.format(filepath, str(e)))
            return None

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.ftp is not None:
            self.ftp.close()


class FtpScanProcess(multiprocessing.Process):
    def __init__(self, manager_dict, interval=60):
        super().__init__()
        self.ftp_scan = None
        self.interval = interval
        self.manager_dict = manager_dict
        manager_dict['status'] = True
        self.errlog = ErrorLog('FtpScanProcess')

    def run(self):
        self.manager_dict['status'] = True
        self.ftp_scan = FtpScanClass(self.manager_dict)
        if self.ftp_scan.ftp is None:
            self.errlog.add_error('run', 'error FTP Connect Fail')
            self.manager_dict['status'] = False

        while self.manager_dict['status']:
            try:
                new_files = self.ftp_scan.scan_newfiles()
                if new_files:
                    print(f"Found {len(new_files)} new files")
                    for file_info in new_files:
                        if not self.manager_dict['status']:
                            break
                        local_file = self.ftp_scan.file_download(file_info)
                        if local_file is not None:
                            with DownLog() as db:
                                db.savelog(file_info[0])
                for i in range(self.interval):
                    if self.manager_dict['status']:
                        time.sleep(1)
                    else:
                        break
            except Exception as e:
                self.errlog.add_error('run', 'restart by Error occurred while scanning FTP directory: {}'.format(str(e)))
                self.run()

    def stop(self):
        self.manager_dict['status'] = False
        if hasattr(self, 'ftp_scan') and self.ftp_scan is not None:
            self.ftp_scan.stop()
