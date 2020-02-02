from appdirs import AppDirs
import sys
import zerorpc
import radiam
import os
import re
from tempfile import mkstemp, gettempdir
from shutil import move
from radiam_api import RadiamAPI
import json


dirs = AppDirs("radiam-agent", "Compute Canada")
os.makedirs(dirs.user_data_dir, exist_ok=True)
tokenfile = os.path.join(dirs.user_data_dir, "token")
dir_path = gettempdir()
configjson = os.path.join(dir_path, "configsetting.json")
projectsjson = os.path.join(dir_path, "projects.json")
resumefile = os.path.join(dirs.user_data_dir, "resume")
logger = radiam.log_setup("info")
auth = {}
arguments = {'--hostname': None,
             '--minsize': 0,
             '--mtime': 0,
             '--password': None,
             '--rootdir': None,
             '--username': None,
             '--projectname': None,
             '--quitafter': None
             }

def replace(file_path, pattern, subst):
    fh, temp_path = mkstemp()
    with os.fdopen(fh, 'w') as new_file:
        with open(file_path) as old_file:
            for line in old_file:
                new_file.write(re.sub(pattern, subst, line))
    os.remove(file_path)
    move(temp_path, file_path)


class RadiamTray(object):
    def __init__(self, logger, dirs, arguments, tokenfile, resumefile):
        self.logger = logger
        self.dirs = dirs
        self.arguments = arguments
        self.resumefile = resumefile
        self.tray_options = {}
        self.config, self.load_config_status = radiam.load_config(dirs.user_data_dir, arguments, logger, self.tray_options)
        with open(configjson, "w") as json_file:
            json.dump(self.config, json_file)
        if self.load_config_status:
            self.config['api']['host'] = self.config['api']['host'].strip('/')
            self.agent_config = {
                "tokenfile": tokenfile,
                "baseurl": self.config['api']['host'],
                "logger": logger
            }
            self.API = RadiamAPI(**self.agent_config)

    def get_host(self):
        with open(configjson) as json_file:
            data = json.load(json_file)
        return data['api']['host']

    def get_rootdir(self):
        with open(configjson) as json_file:
            data = json.load(json_file)
        return data['project1']['rootdir']

    def projects_results(self):
        with open(projectsjson) as json_file:
            data = json.load(json_file)
        return json.dumps([i['name'] for i in data['results']])

    def crawl(self):
        if not self.config['api']['host']:
            self.logger.error("Remote project URL is not configured.")
            return "Error: You need to set a remote project name and URL so the app knows where to connect."
        elif self.load_config_status:
            crawlout = radiam.crawl(self.dirs, self.arguments, self.logger, self.config, self.API, self.tray_options)
            return crawlout
        else:
            return "Error: You need to configure a project before crawling."

    def settings(self):
        configfile = os.path.join(self.dirs.user_data_dir, "radiam.txt")
        if not os.path.exists(configfile):
            radiam.write_new_config(configfile, self.tray_options)
        return configfile

    def set_project_path(self, path):
        configfile = os.path.join(self.dirs.user_data_dir, "radiam.txt")
        replace(configfile, "^#?rootdir.*", ("rootdir = " + path))

    def set_resume_file(self, success):
        if success == 1:
            open_resumefile = open(self.resumefile, 'w+')
            open_resumefile.close()
        else:
            if os.path.exists(self.resumefile):
                os.remove(self.resumefile)

    def check_resume_file(self):
        if os.path.exists(self.resumefile):
            return 1

    def get_token(self, username, password, hostname):
        if hostname is not None:
            self.tray_options = {"hostname": hostname}
            configfile = os.path.join(self.dirs.user_data_dir, "radiam.txt")
            replace(configfile, "^#?host.*", ("host = " + hostname))
            self.config['api']['host'] = hostname
        if not self.config['api']['host']:
            self.logger.error("Remote project URL is not configured.")
            return "Error: You need to set a remote project name and URL so the app knows where to connect."
        self.agent_config = {
            "tokenfile": tokenfile,
            "baseurl": self.config['api']['host'],
            "logger": logger
        }
        self.API = RadiamAPI(**self.agent_config)
        login_status = self.API.login(username, password)
        if login_status:
            with open(projectsjson, "w") as json_file:
                json.dump(self.API.get_projects(), json_file)
            return "Token obtained."
        else:
            return "Unable to obtain token. Verify your credentials and Radiam URL."

    def set_config(self, projectname, rootdir):
        self.tray_options.update({"projectname": projectname, "rootdir": rootdir})
        self.config, self.load_config_status = radiam.load_config(self.dirs.user_data_dir, self.arguments, self.logger, self.tray_options)
        with open(configjson, "w") as json_file:
            json.dump(self.config, json_file)
        self.agent_config = {
            "tokenfile": tokenfile,
            "baseurl": self.config['api']['host'],
            "logger": self.logger
        }
        self.API = RadiamAPI(**self.agent_config)
        return "Config set."


if __name__ == '__main__':
    s = zerorpc.Server(RadiamTray(logger, dirs, arguments, tokenfile, resumefile))
    s.bind('tcp://127.0.0.1:' + str(sys.argv[1]))
    s.run()