from appdirs import AppDirs
import unittest
import radiam
import os
import tempfile
import shutil
from radiam_api import RadiamAPI

# copied this from radiam_tray, might not all be necessary for testing
dirs = AppDirs("radiam-agent", "Compute Canada")
os.makedirs(dirs.user_data_dir, exist_ok=True)
tokenfile = os.path.join(dirs.user_data_dir, "token")
dir_path = os.getcwd()
configjson = os.path.join(dir_path, "configsetting.json")
projectsjson = os.path.join(dir_path, "projects.json")
resumefile = os.path.join(dirs.user_data_dir, "resume")
logger = radiam.log_setup("info")
auth = {}
arguments = {'--hostname': "http://127.0.0.1:8100",
             '--minsize': 0,
             '--mtime': 0,
             '--password': "admin",
             '--rootdir': None,
             '--username': "admin",
             '--projectname': "testproject",
             '--quitafter': True
             }

class TestRadiam(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestRadiam, self).__init__(*args, **kwargs)
        self.logger = logger
        self.dirs = dirs
        self.arguments = arguments
        self.resumefile = resumefile
        self.tray_options = {}

    def test_load_config(self):
        self.config, self.load_config_status = radiam.load_config(self.dirs.user_data_dir, self.arguments, self.logger, self.tray_options)
        self.assertIsNotNone(self.config)

    def test_index_file(self):
        self.config, self.load_config_status = radiam.load_config(self.dirs.user_data_dir, self.arguments, self.logger, self.tray_options)
        fp = tempfile.TemporaryDirectory()
        project = self.config['projects']['project_list'][0]
        file_list = radiam.get_list_of_files(fp.name, self.config[project])
        self.assertIsNotNone(file_list)
        fp.cleanup()

    def test_crawl(self):
        fp = tempfile.TemporaryDirectory()
        self.arguments['--rootdir'] = fp.name
        self.config, self.load_config_status = radiam.load_config(self.dirs.user_data_dir, self.arguments, self.logger, self.tray_options)
        agent_config = {
            "tokenfile": tokenfile,
            "baseurl": self.config['api']['host'],
            "logger": logger
        }
        API = RadiamAPI(**agent_config)
        temppath = os.path.join(fp.name, "radiamtemp.txt")
        with open(temppath, "w") as textfile:
            textfile.write("testing")
        crawler = radiam.crawl(self.dirs, self.arguments, self.logger, self.config, API, self.tray_options)
        self.assertIsNone(crawler)
        fp.cleanup()

    def test_get_dir_meta(self):
        fp = tempfile.TemporaryDirectory()
        self.arguments['--rootdir'] = fp.name
        self.config, self.load_config_status = radiam.load_config(self.dirs.user_data_dir, self.arguments, self.logger, self.tray_options)
        project_key = self.config['projects']['project_list'][0]
        temppath = os.path.join(fp.name, "radiamtemp.txt")
        with open(temppath, "w") as textfile:
            textfile.write("testing")
        dir_meta = radiam.get_dir_meta(fp.name, self.config, project_key)
        self.assertIsNotNone(dir_meta)
        fp.cleanup()

    def test_get_file_meta(self):
        fp = tempfile.TemporaryDirectory()
        self.arguments['--rootdir'] = fp.name
        self.config, self.load_config_status = radiam.load_config(self.dirs.user_data_dir, self.arguments, self.logger, self.tray_options)
        project_key = self.config['projects']['project_list'][0]
        temppath = os.path.join(fp.name, "radiamtemp.txt")
        with open(temppath, "w") as textfile:
            textfile.write("testing")
        file_meta = radiam.get_file_meta(temppath, self.config, project_key)
        self.assertIsNotNone(file_meta)
        fp.cleanup()

    def test_file_excluded(self):
        fp = tempfile.TemporaryDirectory()
        self.arguments['--rootdir'] = fp.name
        self.config, self.load_config_status = radiam.load_config(self.dirs.user_data_dir, self.arguments, self.logger, self.tray_options)
        project_key = self.config['projects']['project_list'][0]
        temppath = os.path.join(fp.name, "radiamtemp.txt")
        with open(temppath, "w") as textfile:
            textfile.write("testing")
        is_file_excluded = radiam.file_excluded(temppath, self.config[project_key])
        self.assertFalse(is_file_excluded)
        fp.cleanup()


if __name__ == '__main__':
    unittest.main(logger, dirs, arguments, tokenfile, resumefile)