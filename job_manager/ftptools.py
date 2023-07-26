import os
from ftplib import FTP, error_perm
import io

from job_manager.singleton import SingletonInstance


class FtpConnector(SingletonInstance):
    def __init__(self):
        self.ftp_ctx = None
        self.host = ''
        self.username = ''
        self.password = ''
        self.pwd = ''
        self.dir_files = {}

    def login(self, host, username, password):
        try:
            self.host = host
            self.username = username
            self.password = password
            self.ftp_ctx = FTP(host, username, password)
            self._init_after_login()
        except Exception as msg:
            return False, str(msg)
        return True, 'Success connect!'

    def _init_after_login(self):
        self.local_browse_rec = []
        self.remote_browse_rec = []
        self.pwd = self.ftp_ctx.pwd()
        self.remote_origin_path = self.pwd
        self.remote_browse_rec.append(self.pwd)

    def check_and_connect(self):
        try:
            self.ftp_ctx.voidcmd("NOOP")
        except Exception as e:
            self.login(self.host, self.username, self.password)

    def download_to_remote_file_list(self, add_item_cb):
        """
        download file and directory list from FTP Server
        """
        self.dir_files = {}

        def _parseFileInfo(file):
            """
            parse files information "drwxr-xr-x 2 root wheel 1024 Nov 17 1993 lib" result like follower
                                    "drwxr-xr-x", "2", "root", "wheel", "1024 Nov 17 1993", "lib"
            """

            item = [f for f in file.split(' ') if f != '']
            mode, num, owner, group, size, date, filename = (
                item[0], item[1], item[2], item[3], item[4], ' '.join(item[5:8]), ' '.join(item[8:]))

            if file.startswith('d'):
                pathname = os.path.join(self.pwd, filename)
                self.dir_files[pathname] = True

            add_item_cb(file, mode, num, owner, group, size, date, filename)

        self.ftp_ctx.dir('.', _parseFileInfo)

    def get_pwd(self):
        return self.pwd

    def is_dir(self, dirname):
        return self.dir_files.get(dirname, None)

    def is_first_browse_rec(self):
        pathname = self.remote_browse_rec[self.remote_browse_rec.index(self.pwd) - 1]
        return pathname != self.remote_browse_rec[0]

    def is_last_browse_rec(self):
        pathname = self.remote_browse_rec[self.remote_browse_rec.index(self.pwd) + 1]
        return pathname != self.remote_browse_rec[-1]

    def cd_to_back_directory(self):
        pathname = self.remote_browse_rec[self.remote_browse_rec.index(self.pwd) - 1]
        self.pwd = pathname
        self.ftp_ctx.cwd(pathname)

    def cd_to_next_directory(self):
        pathname = self.remote_browse_rec[self.remote_browse_rec.index(self.pwd) + 1]
        self.pwd = pathname
        self.ftp_ctx.cwd(pathname)

    def cd_to_directory(self, dirname):
        pathname = os.path.join(self.pwd, dirname)
        if not self.is_dir(pathname):
            return False
        self.remote_browse_rec.append(pathname)
        self.ftp_ctx.cwd(pathname)
        self.pwd = self.ftp_ctx.pwd()
        return True

    def cwd(self, pathname):
        try:
            self.ftp_ctx.cwd(pathname)
            self.remote_browse_rec.append(pathname)
            self.pwd = self.ftp_ctx.pwd()
        except error_perm as e:
            return False
        return True

    def get_file_list(self, path=None):
        if path is None:
            return self.ftp_ctx.nlst()
        else:
            return self.ftp_ctx.nlst(path)

    def store(self, path, data_bytes, mkdirs=True):
        if mkdirs:
            dir_path = os.path.dirname(path)
            self.makedir(dir_path)

        try:
            bio = io.BytesIO()
            bio.write(data_bytes)
            bio.seek(0)

            self.ftp_ctx.storbinary(f'STOR {path}', bio)
            return path
        except error_perm as e:
            return None

    def load(self, path):
        bio = io.BytesIO()
        self.ftp_ctx.retrbinary(f'RETR {path}', bio.write)
        return bio

    def makedir(self, path, recursive=True):
        if not path:
            return None

        if recursive:
            prefix_path = '/' if path[0] == '/' else ''
            paths = path.split('/')
            merge_path = ''
            ret = None
            for p in paths:
                if not p:
                    continue
                merge_path = os.path.join(merge_path, p)
                ret = self.makedir(prefix_path + merge_path, False)
        else:
            try:
                ret = self.ftp_ctx.mkd(path)
            except error_perm as e:
                return None
        return ret


if __name__ == '__main__':
    with open('/Users/bogonets/Documents/1.jpeg', 'rb') as f:
        data = f.read()

    def add_item_cb(file, mode, num, owner, group, size, date, filename):
        print(file)
    FtpConnector.instance().login('192.168.0.87', 'wooruang', 'bogowooang')
    # FtpConnector.instance().download_to_remote_file_list(add_item_cb)
    print(FtpConnector.instance().cwd('/SAVEZONE/ai_work_job'))
    print(FtpConnector.instance().get_pwd())
    FtpConnector.instance().download_to_remote_file_list(add_item_cb)
    print(FtpConnector.instance().get_file_list())
    print(FtpConnector.instance().get_file_list('/SAVEZONE/ai_work_job/test1'))
    print(FtpConnector.instance().cwd('/SAVEZONE/ai_work_job/test.jpg'))
    print(FtpConnector.instance().makedir('/SAVEZONE/ai_work_job/test2'))
    print(FtpConnector.instance().makedir('/SAVEZONE/ai_work_job/test2/t1/t2/t3/t4'))
    print(FtpConnector.instance().store('/SAVEZONE/ai_work_job/t/t.jpg', data))
    print(FtpConnector.instance().load('/SAVEZONE/ai_work_job/t/t.jpg').getbuffer().nbytes)




