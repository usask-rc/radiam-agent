"use strict";

if(require('electron-squirrel-startup')) return;
const {app, dialog, nativeImage, shell, Tray, Menu, BrowserWindow, autoUpdater} = require("electron");
const gotTheLock = app.requestSingleInstanceLock();
if (!gotTheLock) {
  app.quit();
} else {
  app.on('second-instance', (event, commandLine, workingDirectory) => {
    notifier.notify({"title" : "Radiam", "message" : "Radiam is already running."});
  });
}

require('update-electron-app')();
const notifier = require("node-notifier");
const zerorpc = require("zerorpc");
global.client = new zerorpc.Client();
const portfinder = require("portfinder");
const fs = require('fs');
const AutoLaunch = require('auto-launch');

const path = require('path')
const PY_RADIAM_TRAY_FOLDER = 'radiam_tray'
const PY_FOLDER = '..'
const PY_MODULE = 'radiam_tray'

const iconPath = path.join(__dirname, 'radiam.png');
const trayIcon = nativeImage.createFromPath(iconPath);

const runIconPath = path.join(__dirname, 'radiam-on.png');
const runTrayIcon = nativeImage.createFromPath(runIconPath);

const configsetting_path = path.join(app.getPath('temp'),'configsetting.json')

let pythonChild = null
let projectFolder = null
let crawlSuccess = null
let loginWindow = null
let configWindow = null
let mainWindow = null
let aboutWindow = null

var radiamAutoLauncher = new AutoLaunch({
  name: 'Radiam Agent'
});

const sleep = (waitTimeInMs) => new Promise(resolve => setTimeout(resolve, waitTimeInMs));

const guessPackaged = () => {
  const fullPath = path.join(__dirname, PY_RADIAM_TRAY_FOLDER)
  return require('fs').existsSync(fullPath)
}

const getScriptPath = () => {
  if (!guessPackaged()) {
    return path.join(__dirname, PY_FOLDER, PY_MODULE + '.py')
  }
  radiamAutoLauncher.isEnabled().then(function(isEnabled){
    if(!isEnabled){
      radiamAutoLauncher.enable();
    }
  })
  if (process.platform === 'win32') {
    return path.join(__dirname, PY_RADIAM_TRAY_FOLDER, PY_MODULE + '.exe')
  }
  return path.join(__dirname, PY_RADIAM_TRAY_FOLDER, PY_MODULE)
}

const createWindow = () => {
  mainWindow = new BrowserWindow({
    width: 440,
    height: 400,
    backgroundColor: "#D6D8DC",
    webPreferences: {
      nodeIntegration: true
    }
  });

  if (app.dock) { app.dock.show() };

  mainWindow.setMenuBarVisibility(false);

  mainWindow.loadURL(require('url').format({
    pathname: path.join(__dirname, 'indexWelcome.html'),
    protocol: 'file:',
    slashes: true
  }))

  mainWindow.on('close', (event) => {
    if (mainWindow != null){
      mainWindow.hide();
      event.preventDefault();
    }
    if (app.dock && configWindow === null && loginWindow === null && aboutWindow === null) { app.dock.hide() }
    mainWindow = null
  });
}


app.on('ready', () => {
  if (mainWindow === null) {
    if (fs.existsSync(configsetting_path)) {

      let config_json_raw = fs.readFileSync(configsetting_path);
      let config_json = JSON.parse(config_json_raw);

      if (config_json["api"]["host"] == "" &&
        config_json["project1"]["name"] == "" &&
        config_json["project1"]["rootdir"] == "") {
        createWindow();
      }
    } else {
      createWindow();
    }
  }
})

portfinder.basePort = 4242;
let port = portfinder.getPort(function (err, port) {
  client.connect("tcp://127.0.0.1:" + String(port));
  const createRadiam = () => {
    let script = getScriptPath()
    if (guessPackaged()) {
      pythonChild = require('child_process').spawn(script, [port])
    } else {
      pythonChild = require('child_process').spawn('python3', [script, port])
    }

    if (pythonChild != null) {
      console.log('Python started successfully')

      pythonChild.stdout.on('data', function (data) {
        console.log(data.toString());
      });
    }
  }

  app.on('ready', createRadiam);
});

const exitRadiam = () => {
  pythonChild.kill()
  pythonChild = null
  global.client.close();
}

const getCredentials = () => {
  loginWindow = new BrowserWindow({
    width: 400,
    height: 320,
    backgroundColor: "#D6D8DC",
    show: false,
    webPreferences: {
      nodeIntegration: true
    }
  })

  if (app.dock) { app.dock.show() }

  loginWindow.setMenuBarVisibility(false);

  loginWindow.loadURL(require('url').format({
    pathname: path.join(__dirname, 'indexToken.html'),
    protocol: 'file:',
    slashes: true
  }))

  loginWindow.once('ready-to-show', () => {
    loginWindow.show()
  })

  loginWindow.on('close', (event) => {
    if (loginWindow != null){
      loginWindow.hide();
      event.preventDefault();
    }
    if (app.dock && configWindow === null && mainWindow === null && aboutWindow === null) { app.dock.hide() }
    loginWindow = null
  })
}

const getConfigsetting = () => {
  configWindow = new BrowserWindow({
    width: 440,
    height: 400,
    backgroundColor: "#D6D8DC",
    show: false,
    webPreferences: {
      nodeIntegration: true
    }
  })

  if (app.dock) { app.dock.show() }

  configWindow.setMenuBarVisibility(false);

  configWindow.loadURL(require('url').format({
    pathname: path.join(__dirname, 'indexConfig.html'),
    protocol: 'file:',
    slashes: true
  }))

  configWindow.once('ready-to-show', () => {
    configWindow.show()
  })

  configWindow.on('close', (event) => {
    if (configWindow != null){
      configWindow.hide();
      event.preventDefault();
    }
    if (app.dock && loginWindow === null && mainWindow === null && aboutWindow === null) { app.dock.hide() }
    configWindow = null
  })
}

const about = () => {
  aboutWindow = new BrowserWindow({
    width: 400,
    height: 200,
    backgroundColor: "#D6D8DC",
    show: false,
    webPreferences: {
      nodeIntegration: true
    }
  })

  if (app.dock) { app.dock.show() }

  aboutWindow.setMenuBarVisibility(false);

  aboutWindow.loadURL(require('url').format({
    pathname: path.join(__dirname, 'indexAbout.html'),
    protocol: 'file:',
    slashes: true
  }))

  aboutWindow.once('ready-to-show', () => {
    aboutWindow.show()
  })

  aboutWindow.on('close', (event) => {
    if (aboutWindow != null){
      aboutWindow.hide();
      event.preventDefault();
    }
    if (app.dock && loginWindow === null && mainWindow === null && configWindow === null) { app.dock.hide() }
    aboutWindow = null
  })
}

app.on("before-quit", ev => {
  if (loginWindow != null){
    loginWindow.close();
  }
  if (configWindow != null){
    configWindow.close();
  }
  if (mainWindow != null){
    mainWindow.close();
  }
  if (aboutWindow != null){
    aboutWindow.close();
  }
  top = null;
});

app.on('will-quit', ev => {
  exitRadiam();
  if (crawlSuccess != null){
    client.invoke("set_resume_file", 1, function(error, res, more) {} );
  } else {
    client.invoke("set_resume_file", 0, function(error, res, more) {} );
  }
  app.quit();
})

const runningMenu = Menu.buildFromTemplate([
  {label: "Pause Crawling", click: (item, window, event) => {
    exitRadiam();
    sleep(2000).then(() => {
      crawlSuccess = null
      portfinder.getPort(function (err, port) {
        client.connect("tcp://127.0.0.1:" + String(port));

        const createRadiam = () => {
          let script = getScriptPath()
          if (guessPackaged()) {
            pythonChild = require('child_process').spawn(script, [port])
          } else {
            pythonChild = require('child_process').spawn('python3', [script, port])
          }
          
          if (pythonChild != null) {
            console.log('Python started successfully')

            pythonChild.stdout.on('data', function (data) {
              console.log(data.toString());
            });
          }
        }

        createRadiam();
        top.tray.setContextMenu(menu);
        top.tray.setImage(trayIcon);
      });
    });
  }},
  {role: "quit"}
]);

const menu = Menu.buildFromTemplate([
  {label: "Crawl", click: (item, window, event) => {
      top.tray.setImage(runTrayIcon);
      top.tray.setContextMenu(runningMenu);
      client.invoke("crawl", function(error, res, more) {
          if (res){
            notifier.notify({"title" : "Radiam", "message" : res});
            top.tray.setContextMenu(menu);
            top.tray.setImage(trayIcon);
          } else {
            crawlSuccess = 1
          }
      });
  }},
  {label: "Change Project Folder", click: (item, window, event) => {
      projectFolder = dialog.showOpenDialogSync({properties: ["openDirectory"]});
      if (projectFolder){
        client.invoke("set_project_path", JSON.stringify(projectFolder[0]), function(error, res, more) {} );
      }
      crawlSuccess = null
  }},
  {label: "Update Login Credentials", click: (item, window, event) => {
      if (loginWindow === null){
        getCredentials()
      }
      crawlSuccess = null
  }},
  {label: "Quick Setup", click: (item, window, event) => {
      if (configWindow === null){
        getConfigsetting()
      }
      crawlSuccess = null
  }},
  {label: "Advanced Settings", click: (item, window, event) => {
      client.invoke("settings", function(error, res, more) {
          shell.openPath(res);
      });
      crawlSuccess = null
  }},
  {label: "About", click: (item, window, event) => {
      if (aboutWindow === null){
        about()
      }
  }},
  {type: "separator"},
  {role: "quit"},
]);

if (process.argv.slice(-1)[0] === '--run-tests') {
  sleep(2000).then(() => {
    const total_tests = 1
    let tests_passing = 0
    let failed_tests = []

    if (pythonChild != null) {
      tests_passing++;
    } else {
      failed_tests.push('spawn_python');
    }

    console.log(`of ${total_tests} tests, ${tests_passing} passing`);

    if (tests_passing < total_tests) {
      console.error(`failed tests: ${failed_tests}`);  
    }

    app.quit();
  });
};

let top = {};

app.once("ready", ev => {
  if (app.dock) { app.dock.hide() }
  top.tray = new Tray(trayIcon);
  top.tray.setToolTip("Radiam Agent");
  client.invoke("check_resume_file", function(error, res, more) {
    if (res){
      top.tray.setImage(runTrayIcon);
      top.tray.setContextMenu(runningMenu);
      client.invoke("crawl", function(error, res, more) {
          if (res){
            notifier.notify({"title" : "Radiam", "message" : res});
            top.tray.setContextMenu(menu);
            top.tray.setImage(trayIcon);
          } else {
            crawlSuccess = 1
          }
      });
    } else {
      top.tray.setContextMenu(menu);
    }
  })
});
