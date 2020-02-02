#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Usage:
  radiam.py [--projectname=<pro>] [--mtime=<mt>] [--minsize=<ms>] [--hostname=<host>] [--username=<user>] [--password=<pass>] [--rootdir=<DIR>] [--quitafter] [--logout] [--loglevel=<loglevel>] ...

Options:
  -d --rootdir=<DIR>  Directory to start crawling from
  -m --mtime=<mt>  Minimum days ago for modified time (default: 0) [default: 0]
  -s --minsize=<ms>  Minimum file size in Bytes (default: 0 Bytes) [default: 0]
  -h --hostname Specify hostname if not in config (e.g. https://localhost:8100, https://dev2.radiam.ca:8100)
  -u --username Specify username if connecting without a token
  -p --password Specify password if connecting without a token
  -n --projectname=<pro> Project name
  -q --quitafter  Quit after initial crawl
  -o --logout  Remove old tokens
  -l --loglevel The logging level(debug, error, warning or info)
"""

from docopt import docopt
from scandir import scandir
import os
import sys
import socket
import persistqueue
from persistqueue import Queue
from configobj import ConfigObj
import logging
import time
from datetime import datetime
import platform
from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserver
from watchdog.events import FileSystemEventHandler
from appdirs import AppDirs
import json
import pickle
import signal
import yaml
import uuid
from radiam_api import RadiamAPI
import radiam_extract
from requests import exceptions
import re

dirs = AppDirs("radiam-agent", "Compute Canada")
os.makedirs(dirs.user_data_dir, exist_ok=True)
tokenfile = os.path.join(dirs.user_data_dir, "token")
os.environ['TIKA_LOG_PATH'] = dirs.user_data_dir
from tika import parser as tikaParser
post_data_limit = 1000000

# only available on non-Windows, and optional
try:
    import grp
except:
    pass

if platform.system() == 'Windows':
    import win32security
else:
    import pwd

version = '1.2'
__version__ = version

default_location_type = "location.type.server"


class FileSystemMonitor(FileSystemEventHandler):
    def __init__(self, API, config, project_key, logger, list_last_crawl):
        self.API = API
        self.c_set = set()
        self.d_set = set()
        self.config = config
        self.project_key = project_key
        self.project_config = config[project_key]
        self.logger = logger
        self.set_last_crawl = set(list_last_crawl)

    def on_deleted(self, event):
        res = self.API.search_endpoint_by_path(self.project_config['endpoint'], event.src_path)
        what = "unknown"
        if res:
            for doc in res['results']:
                if doc['type'] == "directory":
                    what = "directory"
                    if not dir_excluded(event.src_path, self.project_config):
                        self.d_set.add(event.src_path)
                elif doc['type'] == "file":
                    what = "file"
                    if not file_excluded(event.src_path, self.project_config) and not yml_file(event.src_path):
                        self.d_set.add(event.src_path)
                else:
                    what = "unknown"
                    self.logger.warning("The type is unknown.")
        while len(self.d_set)!=0:
            path_de = os.path.abspath(self.d_set.pop())
            try_connection_in_worker(self.API, self.project_config, path_de, self.logger)
            self.set_last_crawl.discard(os.path.abspath(path_de))
            self.logger.info("Deleted %s: %s", what, event.src_path)
            meta_status, parent_path = update_path(event.src_path, self.config, self.project_key, self.API,
                                                   self.project_config, self.logger)
            if meta_status:
                self.set_last_crawl.add(os.path.abspath(parent_path))
                self.logger.info("Update the information for directory %s", parent_path)

    def on_created(self, event):
        self.on_create_modify(event, "Created", self.logger)

    def on_modified(self, event):
        self.on_create_modify(event, "Modified", self.logger)

    def on_moved(self, event):
        what = 'directory' if event.is_directory else 'file'
        if what == 'file' and not file_excluded(event.src_path, self.project_config) and not file_excluded(event.dest_path, self.project_config) and not yml_file(event.src_path) and not yml_file(event.dest_path):
            self.d_set.add(event.src_path)
            self.c_set.add(event.dest_path)
        if what == 'directory' and not dir_excluded(event.src_path, self.project_config) and not dir_excluded(event.dest_path, self.project_config) and not yml_file(event.src_path) and not yml_file(event.dest_path):
            self.d_set.add(event.src_path)
            self.c_set.add(event.dest_path)
        while len(self.c_set)!=0:
            path_in = self.c_set.pop()
            if what == "directory":
                metadata = get_dir_meta(path_in, self.config, self.project_key)
            else:
                metadata = get_file_meta(path_in, self.config, self.project_key)
            if metadata is not None:
                try_connection_in_worker(self.API, self.project_config, path_in, self.logger, metadata)
                self.set_last_crawl.add(os.path.abspath(path_in))
        while len(self.d_set)!=0:
            path_de = self.d_set.pop()
            try_connection_in_worker(self.API, self.project_config, path_de, self.logger)
            self.set_last_crawl.discard(os.path.abspath(path_de))
            self.logger.info("Moved %s: from %s to %s", what, event.src_path, event.dest_path)
        meta_status_src, parent_path_src = update_path(event.src_path, self.config, self.project_key, self.API,
                                                       self.project_config, self.logger)
        meta_status_dest, parent_path_dest = update_path(event.dest_path, self.config, self.project_key, self.API,
                                                         self.project_config, self.logger)
        if meta_status_src:
            self.set_last_crawl.add(os.path.abspath(parent_path_src))
            self.logger.info("Update the information for directory %s", parent_path_src)
        if meta_status_dest:
            self.set_last_crawl.add(os.path.abspath(parent_path_dest))
            self.logger.info("Update the information for directory %s", parent_path_dest)

    def on_create_modify(self, event, action, logger):
        what = 'directory' if event.is_directory else 'file'
        if what == 'file' and not file_excluded(event.src_path, self.project_config) and not yml_file(event.src_path):
            self.c_set.add(event.src_path)
        if what == 'directory' and not dir_excluded(event.src_path, self.project_config):
            self.c_set.add(event.src_path)
        while len(self.c_set) != 0:
            path_in = self.c_set.pop()
            if what == "directory":
                metadata = get_dir_meta(path_in, self.config, self.project_key)
            else:
                metadata = get_file_meta(path_in, self.config, self.project_key)
            if metadata is not None:
                try_connection_in_worker(self.API, self.project_config, path_in, self.logger, metadata)
                self.set_last_crawl.add(os.path.abspath(path_in))
                self.logger.info("%s %s: %s", action, what, event.src_path)
            meta_status, parent_path = update_path(event.src_path, self.config, self.project_key, self.API,
                                                   self.project_config, logger)
            if meta_status:
                self.set_last_crawl.add(os.path.abspath(parent_path))
                self.logger.info("Update the information for directory %s", parent_path)


def update_path(path, config, project_key, API, project_config, logger):
    parent_path = os.path.abspath(os.path.join(path, os.pardir))
    metadata = get_dir_meta(parent_path, config, project_key)
    if metadata is not None:
        try_connection_in_worker(API, project_config, parent_path, logger, metadata)
        return True, parent_path
    else:
        return False, parent_path


def try_connection_in_worker(API, project_config, path, logger, metadata=None):
    while True:
        try:
            res = API.search_endpoint_by_path(project_config['endpoint'], path)
            if res:
                if metadata:
                    if res['count'] == 0:
                        API.create_document(project_config['endpoint'], metadata)
                        logger.debug("POSTing to API: {}".format(json.dumps(metadata)))
                    else:
                        API.create_document(project_config['endpoint'], metadata)
                        logger.debug("POSTing to API: " + json.dumps(metadata))
                else:
                    for doc in res['results']:
                        API.delete_document(project_config['endpoint'], doc['id'])
                        logger.debug("DELETEing document {} from API".format(doc['id']))
            return
        except exceptions.ConnectionError:
            time.sleep(10)
            pass


def try_connection_in_worker_bulk(API, project_config, logger, metadata):
    while True:
        try:
            logger.debug("POSTing to API: {}".format(json.dumps(metadata)))
            resp_text, status = API.create_document_bulk(project_config['endpoint'], metadata)
            if resp_text:
                if isinstance(resp_text, list):
                    for s in resp_text:
                        if isinstance(s, dict):
                            if not s.get('result'):
                                # TODO: Requeue this file?
                                logger.error("Error sending file to API: {} {}".format(s.get('docname'), s.get('result')))
                else:
                    logger.error("Radiam API error with index '{}': {}\n".format(project_config['endpoint'], resp_text))
            return resp_text, status
        except exceptions.ConnectionError:
            time.sleep(10)
            pass


def replace_config(configfile, logger, tray_options):
    logger.error("Config file is corrupt or missing too many fields.")
    logger.error("It may be from a much older version of the app.")
    logger.error("Your old config has been backed up and a new one has been generated.")
    if os.path.exists(configfile + ".old"):
        os.remove(configfile + ".old")
    os.rename(configfile, (configfile + ".old"))
    write_new_config(configfile, tray_options)


def write_new_config(configfile, tray_options):
    agent_id = uuid.uuid4()
    with open(configfile, "w") as new_config:
        new_config.write("# Radiam agent configuration file\n")
        new_config.write("# All lines without a starting hash mark are required and must be configured.\n")
        new_config.write("# Remove the starting hash mark from any optional line that you fill in.\n\n")
        new_config.write("[api]\n")
        new_config.write("# Host will be the full URL to the Radiam API eg: https://dev.radiam.ca \n")
        if tray_options:
            new_config.write("host = {}\n\n".format(tray_options['hostname']))
        else:
            new_config.write("host =\n")
        new_config.write("# Port number does not usually need to be changed\n")
        new_config.write("#port = 8100\n\n")
        new_config.write("[agent]\n")
        new_config.write("# This ID is randomly generated and does not need to be changed.\n")
        new_config.write("id = {}\n".format(agent_id))
        new_config.write("# Minimum days ago for modified time (default: 0)\n")
        new_config.write("#mtime = 0\n")
        new_config.write("# Minimum file size in Bytes for indexing (default: 0 Bytes)\n")
        new_config.write("#minsize = 0\n\n")
        new_config.write("[location]\n")
        new_config.write("# A nickname for the computer on which this is running.\n")
        new_config.write("#name = \n\n")
        new_config.write("[projects]\n")
        new_config.write("# Project_list is a comma separated list of labels eg: project1, project2, project3\n")
        new_config.write("# Each label in the list must have its own section below\n")
        new_config.write("project_list = project1\n\n")
        new_config.write("[project1]\n")
        new_config.write("# rootdir is the top level directory for this project data files.\n")
        if tray_options:
            new_config.write("rootdir = {}\n".format(tray_options['rootdir']))
        else:
            new_config.write("rootdir =\n")
        new_config.write("# Project name must match exactly with a project that you have permission to.\n")
        if tray_options:
            new_config.write("name = {}\n".format(tray_options['projectname']))
        else:
            new_config.write("name =\n")
        new_config.write("# Comma separated lists of directories to include or exclude for this project.\n")
        new_config.write("included_dirs =\n")
        new_config.write("excluded_dirs = .*,.snapshot,.Snapshot,.zfs\n")
        new_config.write("# Comma separated lists of files to include or exclude for this project.\n")
        new_config.write("included_files =\n")
        new_config.write("excluded_files = .*,Thumbs.db,.DS_Store,._.DS_Store,.localized,desktop.ini,*.pyc,*.swx,*.swp,*~,~$*,NULLEXT\n")
        new_config.write("# URL to a Tika instance for optional metadata parsing in this project.\n")
        new_config.write("#tika_host =\n")
        new_config.write("#rich_metadata = disabled\n\n")


def config_list_check(config, project_key, input_field):
    if not config[project_key].get(input_field):
        config[project_key][input_field] = list()
    else:
        if isinstance(config[project_key][input_field], str):
            config[project_key][input_field] = [config[project_key][input_field]]


def load_config(user_data_dir, arguments, logger, tray_options):
    """Load the configuration for this agent from the config file"""
    config = None
    configfile = os.path.join(user_data_dir, "radiam.txt")
    config = ConfigObj(configfile)
    if tray_options:
        write_new_config(configfile, tray_options)
        config = ConfigObj(configfile)
        if isinstance(config['projects']['project_list'], str):
            config['projects']['project_list'] = [config['projects']['project_list']]
    else:
        if not "projects" in config:
            try:
                write_new_config(configfile, None)
                config = ConfigObj(configfile)
                if isinstance(config['projects']['project_list'], str):
                    config['projects']['project_list'] = [config['projects']['project_list']]
            except:
                logger.error("Configuration file cannot be created or read. The app will not work; please report this issue.")
                return config, False

        if config['projects']['project_list']:
            if isinstance(config['projects']['project_list'], str):
                config['projects']['project_list'] = [config['projects']['project_list']]
            first_project_key = config['projects']['project_list'][0]
            if arguments['--projectname']:
                config[first_project_key]['name'] = arguments['--projectname']
            else:
                if not config[first_project_key]['name']:
                    return config, False
            if arguments['--rootdir']:
                config[first_project_key]['rootdir'] = arguments['--rootdir']
            else:
                if not config[first_project_key]['rootdir']:
                    logger.error("Project rootdir was not supplied in config or command line")
                    return config, False
            for project_key in config['projects']['project_list']:
                config_list_check(config, project_key, "included_files")
                config_list_check(config, project_key, "excluded_files")
                config_list_check(config, project_key, "included_dirs")
                config_list_check(config, project_key, "excluded_dirs")
        else:
            return config, False

    if "api" in config:
        if config['api'].get('host') is None:
            config['api']['host'] = "http://localhost:8100"
        if config['api'].get('port'):
            config['api']['host'] = config['api']['host'] + ":" + config['api']['port']
        if arguments['--hostname']:
            config['api']['host'] = arguments['--hostname']
    else:
        replace_config(configfile, logger, tray_options)
        return config, False

    if "agent" in config:
        if config['agent'].get('mtime') is None:
            config['agent']['mtime'] = "0"
        if arguments['--mtime']:
            config['agent']['mtime'] = arguments['--mtime']
        if arguments['--minsize']:
            config['agent']['minsize'] = arguments['--minsize']
        if config['agent'].get('minsize') is None:
            config['agent']['minsize'] = "0"
        if config['agent'].get('loglevel') == "debug":
            logger.setLevel(logging.DEBUG)
            logger.info("Log level changed to debug based on agent configuration setting")
    else:
        replace_config(configfile, logger, tray_options)
        return config, False

    if "location" in config:
        if not config['location'].get('name') and not config['location'].get('id'):
            config['location']['name'] = socket.gethostname()
    else:
        replace_config(configfile, logger, tray_options)
        return config, False
    return config, True


def agent_checkin(API, config, logger):
    changed = False
    host = config['api']['host']

    for project_key in config['projects']['project_list']:
        if "id" in config[project_key] and config[project_key]['id']:
            config[project_key]["endpoint"] = host + "/api/projects/" + config[project_key]['id'] + "/"
            res = API.search_endpoint_by_name('projects', config[project_key]['id'], "id")
            if not res or not "results" in res or res["count"] == 0:
                return False, "Project id {} does not appear to exist - was it deleted?".format(config[project_key]['id'])
        else:
            project_res = API.search_endpoint_by_name('projects', config[project_key]['name'])
            if project_res and project_res["count"] > 0:
                config[project_key]["id"] = project_res['results'][0]['id']
                config[project_key]["endpoint"] = host + "/api/projects/" + config[project_key]['id'] + "/"
                changed = True
            else:
                return False, "A project with name {} was not found.".format(config[project_key]['name'])
        logger.debug("Endpoint for project {} is {}\n".format(config[project_key]['name'], config[project_key]['endpoint']))

    if not config['agent']['id']:
        config['agent']['id'] = uuid.uuid4()
        changed = True

    # Create the location if needed
    if config['location'].get('id'):
        #TODO - should we POST to update the latest hostname of this location?
        pass
    else:
        res = API.search_endpoint_by_name('locations', config['location']['name'], "display_name")
        if res and "results" in res and res["count"] > 0:
            config['location']['id'] = res['results'][0]['id']
            changed = True
        else:
            res = API.search_endpoint_by_name('locationtypes', default_location_type, "label")
            if res and "results" in res and res["count"] > 0:
                location_id = res['results'][0]['id']
            else:
                return False, "Could not look up location type ID for {}".format(default_location_type)
            hostname = socket.gethostname()
            newLocation = {
                "display_name": config['location']['name'],
                "host_name": hostname,
                "location_type": location_id
            }
            res = API.create_location(newLocation)
            if res and "id" in res:
                config['location']['id'] = res['id']
                changed = True
            else:
                return False, "Tried to create a new location, but the API call failed"

    # Create the user agent if needed
    res = API.search_endpoint_by_name('useragents', config['agent']['id'], "id")
    if not res or not "results" in res or res["count"] == 0:
        logger.debug("Useragent {} was not found in the remote system; creating it now".format(config['agent']['id']))
        res = API.get_logged_in_user()
        if res and "id" in res:
            current_user_id = res['id']
            project_config_list = []
            for p in config['projects']['project_list']:
                project_config_list.append({
                    "project": config[p]['name'],
                    "config": {"rootdir": config[p]['rootdir']}
                })
            newAgent = {
                "id": config['agent']['id'],
                "user": current_user_id,
                "version": version,
                "location": config['location'].get('id'),
                "project_config_list": project_config_list
            }
            logger.debug("JSON: {}".format(json.dumps(newAgent)))
            res = API.create_useragent(newAgent)
            if not res or not "id" in res:
                return False, "Tried to create a new user agent, but the API call failed"
            else:
                logger.debug("Useragent {} created".format(config['agent']['id']))
        else:
            logger.error("Could not determine current logged in user to create useragent")
            return False, "Could not determine current logged in user to create useragent"
    else:
        logger.debug("Useragent {} appears to exist".format(config['agent']['id']))

    for project_key in config['projects']['project_list']:
        try:
            logger.debug("Endpoint for project {} is {}\n".format(config[project_key]['name'], config[project_key]['endpoint']))
        except:
            pass
    if changed:
        config.write()

    return True, None


def log_setup(logLevel):
    """Set up logging for Radiam agent."""
    radiam_logger = logging.getLogger('radiam')

    logging.addLevelName(logging.INFO, logging.getLevelName(logging.INFO))
    logging.addLevelName(logging.WARNING, logging.getLevelName(logging.WARNING))
    logging.addLevelName(logging.ERROR, logging.getLevelName(logging.ERROR))
    logging.addLevelName(logging.DEBUG, logging.getLevelName(logging.DEBUG))
    logformatter = '%(asctime)s [%(levelname)s] %(message)s'
    formatter = logging.Formatter(logformatter)
    clean_logformatter = '%(message)s'
    clean_formatter = logging.Formatter(clean_logformatter)

    logging.basicConfig(format=logformatter, level=logging.INFO, filename=os.path.join(dirs.user_data_dir, "radiam_log.txt"), filemode='w')

    console = logging.StreamHandler(sys.stdout)
    if logLevel == "debug":
        console.setLevel(logging.DEBUG)
        radiam_logger.setLevel(logging.DEBUG)
    elif logLevel == "error":
        console.setLevel(logging.ERROR)
        radiam_logger.setLevel(logging.ERROR)
    elif logLevel == "warning":
        console.setLevel(logging.WARNING)
        radiam_logger.setLevel(logging.WARNING)
    else:
        console.setLevel(logging.INFO)
        radiam_logger.setLevel(logging.INFO)
    radiam_logger.info("Log level: {}".format(logLevel))

    console.addFilter(lambda record: record.levelno <= logging.INFO)
    console.setFormatter(clean_formatter)
    logging.getLogger('').addHandler(console)

    err_console = logging.StreamHandler()
    err_console.setLevel(logging.WARNING)
    err_console.setFormatter(formatter)
    logging.getLogger('').addHandler(err_console)

    return radiam_logger


def get_extended_metadata(dir, project_config):
    if not project_config.get("tika_host") and not project_config.get("rich_metadata"):
        return None
    if project_config.get("rich_metadata") == "enabled":
        return radiam_extract.route_metadata_parser(dir)
    if os.path.getsize(dir) > 500000:
        return None
    try:
        parsed = tikaParser.from_file(dir, project_config.get("tika_host"))
        if parsed["status"] == 200:
            return parsed['metadata']
        else:
            return None
    except:
        return None


def get_dir_meta(path, config, project_key):
    try:
        if dir_excluded(path, config[project_key]):
            return None
        # get directory meta using lstat
        mode, ino, dev, nlink, uid, gid, size, atime, mtime, ctime = os.lstat(path)

        # convert times to utc for es
        mtime_utc = datetime.utcfromtimestamp(mtime).isoformat()
        atime_utc = datetime.utcfromtimestamp(atime).isoformat()
        ctime_utc = datetime.utcfromtimestamp(ctime).isoformat()

        # get time now in utc
        indextime_utc = datetime.utcnow().isoformat()

        # try to get owner user name
        if platform.system() == 'Windows':
            sd = win32security.GetFileSecurity(path, win32security.OWNER_SECURITY_INFORMATION)
            owner_sid = sd.GetSecurityDescriptorOwner()
            owner, domain, owner_type = win32security.LookupAccountSid(None, owner_sid)
        else:
            owner = pwd.getpwuid(uid).pw_name

        if platform.system() == 'Windows':
            group = "Windows"
        else:
            try:
                group = grp.getgrgid(gid).gr_name.split('\\')
                # remove domain before group
                if len(group) == 2:
                    group = group[1]
                else:
                    group = group[0]
            # if we can't find the group name, use the gid number
            except KeyError:
                group = platform.system()

        parentdir = os.path.abspath(os.path.join(path, os.pardir))

        dirmeta_dict = {
            "name": os.path.basename(path),
            "path": os.path.abspath(path),
            "path_parent": parentdir,
            "items": len([f for f in os.listdir(path)]),
            "file_num_in_dir": len([f for f in os.listdir(path)if os.path.isfile(os.path.join(path, f))]),
            "last_modified": mtime_utc,
            "last_access": atime_utc,
            "last_change": ctime_utc,
            "owner": owner,
            "group": group,
            "indexing_date": indextime_utc,
            "indexed_by": owner,
            "type": "directory",
            "location": config['location']['id'],
            "agent": config['agent']['id']
        }
        yaml_path = os.path.join(path, (os.path.basename(path) + ".yml"))
        if os.path.isfile(yaml_path):
            try:
                with open(yaml_path, 'r') as stream:
                    yaml_data = yaml.safe_load(stream)
                filemeta_dict["extended_metadata"] = yaml_data
            except:
                pass

    except (IOError, OSError) as e:
        return False
    except FileNotFoundError as e:
        return False

    return dirmeta_dict


def get_file_meta(path, config, project_key):
    """Scrapes file meta and ignores files smaller than minsize Bytes,
    newer than mtime and in excluded_files. Returns file meta dict."""

    try:
        if file_excluded(path, config[project_key]) or yml_file(path):
            return None

        mode, ino, dev, nlink, uid, gid, size, atime, mtime, ctime = os.lstat(path)

        # Skip files smaller than minsize cli flag
        if size < int(config['agent'].get('minsize',0)):
            return None

        # Convert time in days (mtime cli arg) to seconds
        time_sec = int(config['agent'].get('mtime',0)) * 86400
        file_mtime_sec = time.time() - mtime
        # Only process files modified at least x days ago
        if file_mtime_sec < time_sec:
            return None

        # convert times to utc for es
        mtime_utc = datetime.utcfromtimestamp(mtime).isoformat()
        atime_utc = datetime.utcfromtimestamp(atime).isoformat()
        ctime_utc = datetime.utcfromtimestamp(ctime).isoformat()

        # try to get owner user name
        if platform.system() == 'Windows':
            sd = win32security.GetFileSecurity(path, win32security.OWNER_SECURITY_INFORMATION)
            owner_sid = sd.GetSecurityDescriptorOwner()
            owner, domain, owner_type = win32security.LookupAccountSid (None, owner_sid)
        else:
            owner = pwd.getpwuid(uid).pw_name

        if platform.system() == 'Windows':
            group = "Windows"
        else:
            try:
                group = grp.getgrgid(gid).gr_name.split('\\')
                # remove domain before group
                if len(group) == 2:
                    group = group[1]
                else:
                    group = group[0]
            # if we can't find the group name, use the gid number
            except KeyError:
                group = platform.system()

        # get time
        indextime_utc = datetime.utcnow().isoformat()

        # get absolute path of parent directory
        parentdir = os.path.abspath(os.path.join(path, os.pardir))

        # create file metadata dictionary
        filemeta_dict = {
            "name": os.path.basename(path),
            "extension": os.path.splitext(os.path.basename(path))[1][1:].strip().lower(),
            "path_parent": parentdir,
            "path": os.path.abspath(path),
            "filesize": size,
            "owner": owner,
            "group": group,
            "last_modified": mtime_utc,
            "last_access": atime_utc,
            "last_change": ctime_utc,
            "indexed_by": owner,
            "indexing_date": indextime_utc,
            "type": "file",
            "location": config['location']['id'],
            "agent": config['agent']['id']
        }
        extended_metadata = get_extended_metadata(path, config[project_key])
        filemeta_dict["extended_metadata"] = extended_metadata
    except (IOError, OSError) as e:
        return False

    except FileNotFoundError as e:
        return False

    return filemeta_dict


def yml_file(filepath):
    filename = os.path.basename(filepath)
    parent_dir = os.path.basename(os.path.dirname(filepath))
    if filename == parent_dir + ".yml":
        return True
    else:
        return False


def file_excluded(filepath, project_config):
    """Return True if path or ext in excluded_files set """
    filename = os.path.basename(filepath)
    extension = os.path.splitext(filename)[1][1:].strip().lower()
    # return if filename in included list (whitelist)
    if filename in project_config.get('included_files'):
        return False
    excluded_list = project_config.get('excluded_files')
    # check for filename in excluded_files set
    if filename in excluded_list:
        return True
    # check for extension in and . (dot) files in excluded_files
    if (not extension and 'NULLEXT' in excluded_list) or '*.' + extension in excluded_list or \
            (filename.startswith('.') and u'.*' in excluded_list) or \
            (filename.endswith('~') and u'*~' in excluded_list) or \
            (filename.startswith('~$') and u'~$*' in excluded_list):
        return True
    return False


def dir_excluded(path, project_config):
    """Return True if path in excluded_dirs set """
    # return if directory in included list (whitelist)
    if os.path.basename(path) in project_config.get('included_dirs') or path in project_config.get('included_dirs'):
        return False
    # skip any dirs which start with . (dot) and in excluded_dirs
    if os.path.basename(path).startswith('.') and u'.*' in project_config.get('excluded_dirs'):
        return True
    # skip any dirs in excluded_dirs
    if os.path.basename(path) in project_config.get('excluded_dirs') or path in project_config.get('excluded_dirs'):
        return True
    # skip any dirs that are found in reg exp checks including wildcard searches
    found_dir = False
    found_path = False
    for d in project_config.get('excluded_dirs'):
        if d == '.*':
            continue
        if d.startswith('*') and d.endswith('*'):
            d = d.replace('*', '')
            if d in os.path.basename(path):
                found_dir = True
                break
            elif d in path:
                found_path = True
                break
        elif d.startswith('*'):
            d = d + '$'
            if re.search(d, os.path.basename(path)):
                found_dir = True
                break
            elif re.search(d, path):
                found_path = True
                break
        elif d.endswith('*'):
            d = '^' + d
            if re.search(d, os.path.basename(path)):
                found_dir = True
                break
            elif re.search(d, path):
                found_path = True
                break
        else:
            if d == os.path.basename(path):
                found_dir = True
                break
            elif d == path:
                found_path = True
                break

    if found_dir or found_path:
        return True

    return False


def full_run(API, q_dir, config, logger):

    def post_data(metadata, files, entry, bulksize, bulkdata):
        resp_text, status = None, False
        if metadata is None or type(metadata) is list and len(metadata) == 0:
            pass
        else:
            files.append(os.path.abspath(os.path.join(path, entry.name)))
            metasize = len(json.dumps(metadata))
            if metasize + bulksize > post_data_limit:
                resp_text, status = try_connection_in_worker_bulk(API, config[project_key], logger, bulkdata)
                bulkdata = []
                bulkdata.append(metadata)
                bulksize = len(json.dumps(bulkdata))
            else:
                bulkdata.append(metadata)
                bulksize = len(json.dumps(bulkdata))
        return bulkdata, bulksize, resp_text, status

    while True:
        try:
            # start at the base directory
            file_list_all = []
            for project_key in config['projects']['project_list']:
                q_dir.put(config[project_key]['rootdir'])
                files = []
                # file_list, resp_text, status = worker(API, q_dir, files, config, project_key, logger)
                bulkdata = []
                bulksize = 0

                while True:
                    try:
                        path = q_dir.get_nowait()
                        try:
                            for entry in scandir(path):
                                if entry.is_dir(follow_symlinks=False):
                                    if not dir_excluded(os.path.join(path, entry.name), config[project_key]):
                                        q_dir.put(os.path.join(path, entry.name))
                                        metadata = get_dir_meta(os.path.join(path, entry.name), config, project_key)
                                        bulkdata, bulksiz, eresp_text, status = post_data(metadata, files, entry,
                                                                                          bulksize, bulkdata)
                                elif entry.is_file(follow_symlinks=False):
                                    metadata = get_file_meta(os.path.join(path, entry.name), config, project_key)
                                    bulkdata, bulksize, resp_text, status = post_data(metadata, files, entry, bulksize,
                                                                                      bulkdata)

                        except (PermissionError, OSError) as e:
                            logger.warning(e)
                            pass
                        q_dir.task_done()
                    except persistqueue.exceptions.Empty:
                        break
                if bulkdata is None or type(bulkdata) is list and len(bulkdata) == 0:
                    logger.info("No files to index on Project %s", config[project_key]['name'])
                    log_full_run_filelist(dirs, files, config[project_key]['name'])
                    logger.info("Agent has added %s files to Project %s", len(files), config[project_key]['name'])
                    return None, 200
                else:
                    resp_text, status = try_connection_in_worker_bulk(API, config[project_key], logger, bulkdata)

                if status:
                    # file_list_all += files
                    logger.info("Finished indexing files to Project %s", config[project_key]['name'])
                    log_full_run_filelist(dirs, files, config[project_key]['name'])
                    logger.info("Agent has added %s files to Project %s", len(files), config[project_key]['name'])
                else:
                    return resp_text, status
            return resp_text, status
        except exceptions.ConnectionError:
            time.sleep(10)
            pass


def diff_list(first, second):
    second = set(second)
    return [item for item in first if item not in second]


def get_list_of_files(dir_name, project_config):
    # Get the list of all files in directory tree at given path
    file_list = set()
    dir_list = set()
    for (dirpath, dirnames, filenames) in os.walk(dir_name):
        if not dir_excluded(dirpath, project_config):
            dir_list.update({os.path.abspath(os.path.join(dirpath, directory)) for directory in dirnames if not dir_excluded(os.path.join(dirpath, directory), project_config)})
            file_list.update({os.path.abspath(os.path.join(dirpath, file)) for file in filenames if not file_excluded(os.path.join(dirpath, file), project_config) and not yml_file(os.path.join(dirpath, file))})
    file_list.update(dir_list)
    return list(file_list)


def log_full_run_filelist(dirs, file_list, project_name):
    with open(os.path.join(dirs.user_data_dir, "last_crawl_%s.data" % project_name), "wb") as last_crawl:
        pickle.dump(file_list, last_crawl)


def check_last_crawl_list(API, dirs, config, logger):
    try:
        for project_key in config['projects']['project_list']:
            curlist = get_list_of_files(config[project_key]['rootdir'], config[project_key])
            with open(os.path.join(dirs.user_data_dir, "last_crawl_%s.data" % config[project_key]['name']), "rb") as last_crawl:
                oldlist = pickle.load(last_crawl)
                if set(oldlist) != set(curlist):
                    deletes = diff_list(oldlist, curlist)
                    for d_path in deletes:
                        # for X in delete, post deletes for files that are missing
                        try:
                            try_connection_in_worker(API, config[project_key], d_path, logger)
                            logger.info("%s is deleted" % d_path)
                        except:
                            pass
                    return False
        return True
    except:
        return False


def load_list_last_crawl(config, project_key):
    with open(os.path.join(dirs.user_data_dir, "last_crawl_%s.data" % config[project_key]['name']), "rb") as last_crawl:
        lastcrawl_list = pickle.load(last_crawl)
    return lastcrawl_list


def backend_monitor(API, config, logger):
    logger.info("Start backend monitor")
    if platform.system() == 'Windows':
        observer = PollingObserver()
    else:
        observer = Observer()
    for project_key in config['projects']['project_list']:
        event_handler = FileSystemMonitor(API, config, project_key, logger, load_list_last_crawl(config, project_key))
        list_last_crawl = load_list_last_crawl(config, project_key)
        observer.schedule(event_handler, config[project_key]['rootdir'], recursive=True)
    observer.start()
    try:
        while True:
            # check the consistency between list_last_crawl and the current list in event_handler every 30s
            time.sleep(30)
            for project_key in config['projects']['project_list']:
                if set(list_last_crawl) != event_handler.set_last_crawl:
                    log_full_run_filelist(dirs, list(event_handler.set_last_crawl), config[project_key]['name'])
                    list_last_crawl = load_list_last_crawl(config, project_key)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
    return


def check_api_status(API, project_config):
    if API.api_get_statusCode(project_config['endpoint'] + "docs/") == 200:
        return True
    else:
        return False


def crawl(dirs, arguments, logger, config, API, tray_options):
    if arguments['--username'] is None or arguments['--password'] is None:
        if API.load_auth_from_file():
            logger.info('Loaded tokens from file')
        else:
            logger.error("Unable to load auth tokens from file, while username and password also not supplied")
            logger.warning("Try the --help argument to see how to authenticate from the command line.")
            return "Error: You need to obtain a login token with your username and password the first time you use the app."
    else:
        if API.login(arguments['--username'], arguments['--password']):
            logger.info('Logged in as %s' % (arguments['--username']))
        else:
            logger.error("Unable to log in with that username and password combination")
            return "Error: Unable to obtain a login token. Please check the credentials."

    checkin_status, err_message = agent_checkin(API, config, logger)

    if not checkin_status:
        logger.error(err_message)
        if not tray_options:
            sys.exit()
        return err_message

    queue_on_disk = os.path.join(dirs.user_data_dir, "radiam_queue")
    q_dir = Queue(queue_on_disk)

    def handle_exit(*args):
        for project_key in config['projects']['project_list']:
            cur_list = get_list_of_files(config[project_key]["rootdir"], config[project_key])
            log_full_run_filelist(dirs, cur_list, config[project_key]['name'])
            logger.info("Save last_crawl_%s.data" % config[project_key]['name'])
        return sys.exit(0)

    def start_process():
        logger.info('Start crawling...')
        resp_text, status = full_run(API, q_dir, config, logger)
        if not arguments['--quitafter']:
            if status:
                backend_monitor(API, config, logger)
            else:
                return resp_text

    signal.signal(signal.SIGTERM, handle_exit)
    signal.signal(signal.SIGINT, handle_exit)

    project_endpoints_ok = True
    for pro_key in config['projects']['project_list']:
        project_endpoints_ok *= check_api_status(API, config[pro_key])

    if project_endpoints_ok:
        if check_last_crawl_list(API, dirs, config, logger):
            if not arguments['--quitafter']:
                backend_monitor(API, config, logger)
        else:
            start_process()
    else:
        start_process()


if __name__ == "__main__":
    if sys.version_info[0] < 3:
        raise Exception("Python 3 is required to run the Radiam agent")
    arguments = docopt(__doc__, version=__version__)
    if arguments['--logout'] and os.path.isfile(tokenfile):
        os.remove(tokenfile)
        print("Removed old auth tokens. Exiting.")
        sys.exit()
    if arguments['--rootdir'] and isinstance(arguments['--rootdir'], (list,)):
        arguments['--rootdir'] = arguments['--rootdir'][0]
    logLevel = "info"
    if arguments['--loglevel']:
        if isinstance(arguments['--loglevel'], (list,)):
            arguments['--loglevel'] = arguments['--loglevel'][0]
        logLevel = arguments['--loglevel']
    logger = log_setup(logLevel)
    tray_options = {}
    config, load_config_status = load_config(dirs.user_data_dir, arguments, logger, tray_options)

    if load_config_status:
        if not config['api']['host']:
            logger.error("Remote project URL is not configured.")
            sys.exit()
        config['api']['host'] = config['api']['host'].strip('/')
    else:
        logger.error("Missing options. Config can be edited at: %s", os.path.join(dirs.user_data_dir, "radiam.txt"))
        sys.exit()

    agent_config = {
        "tokenfile": tokenfile,
        "baseurl": config['api']['host'],
        "logger": logger
    }
    logger.debug("Agent will use Radiam API at: " + config['api']['host'])
    API = RadiamAPI(**agent_config)
    logger.debug("Starting file system crawl")
    crawlout = crawl(dirs, arguments, logger, config, API, tray_options)
    if crawlout is not None:
        print(crawlout)
