# 这是一个示例 Python 脚本。
import asyncio
import multiprocessing
import os
import sys
import time

from Config import MysqlInfo, FTPInfo, DownLog
from MroSync import FtpScanProcess


async def handle_user_input():
    global ftp_scan_process
    while True:
        cmd = await asyncio.get_event_loop().run_in_executor(None, input, "Enter command (start, stop): ")
        if cmd == "start":
            if ftp_scan_process is None:
                manager_dict = manager.dict()
                manager_dict['status'] = True
                ftp_scan_process = FtpScanProcess(manager_dict)
            if not ftp_scan_process or not ftp_scan_process.is_alive():
                ftp_scan_process.start()
                time.sleep(1)
                print("Process started.")

            else:
                print("Process is started.")
        elif cmd == "stop":
            if ftp_scan_process:
                ftp_scan_process.stop()
                ftp_scan_process.join()
                ftp_scan_process = None
            else:
                print("Process is not started.")
        elif cmd == "exit":
            if ftp_scan_process and ftp_scan_process.is_alive():
                ftp_scan_process.stop()
                ftp_scan_process.join()
            sys.exit()
        elif cmd == 'del':
            DownLog().dellog_by_time('2023-03-27 10:00:00')
        else:
            print("Invalid command. Please enter 'start' or 'stop'.")


if __name__ == '__main__':
    mysql = MysqlInfo(section='LocalServer')
    ftp = FTPInfo()
    mysql.update(host='localhost', port=3306, user='root', passwd='242520')
    ftp.update(ftp_name='test',
               host='192.168.192.200',
               port=21,
               user='nixevol',
               passwd='242520',
               sync_path='/sync/',
               down_path=os.path.join(os.getcwd(), 'sync'),
               scan_filter='tmp|temp')
    print("MySQL Check:", mysql.check())
    print("FTP Check:", ftp.check())
    manager = multiprocessing.Manager()
    ftp_scan_process = None
    asyncio.run(handle_user_input())
