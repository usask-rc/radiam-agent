const notifier = require('node-notifier');
const tt = require('electron-tooltip');
const {dialog} = require('electron').remote;
const remote = require('electron').remote;
let client = remote.getGlobal('client');
let projectFolder = null;
tt({position: 'right'})

function setconfig() {
  var projectname = document.getElementById("projectname").value;
  var rootdir = document.getElementById("rootdir").value;
  client.invoke("set_config", projectname, rootdir, function(error, res, more) {
    if (res){
      if (res == "Config set.") {
        var window = remote.getCurrentWindow();
        window.close();
      }
      notifier.notify({"title" : "Radiam", "message" : res});
    } else {
      setTimeout(notifier.notify({"title" : "Radiam", "message" : "Unable to set config. Make sure you have access to a project."}), 1500)
    }
  });
}

function setlocation() {
  projectFolder = dialog.showOpenDialog({properties: ["openDirectory"]});
  if (projectFolder){
    document.getElementById("rootdir").value = projectFolder;
  }
}

function get_root_dir() {
  client.invoke("get_rootdir", function(error, res, more) {
    document.getElementById("rootdir").setAttribute('value', res)
  });
}

function get_drop_down_list() {
  client.invoke("projects_results", function(error, res, more) {
    var select = document.getElementById("projectname");
    var options = JSON.parse(res);
    for(var i = 0; i < options.length; i++) {
        var opt = options[i];
        var el = document.createElement("option");
        el.textContent = opt;
        el.value = opt;
        select.appendChild(el);
        }
  });
}

function start() {
    get_drop_down_list();
    get_root_dir();
}

window.onload=start;
document.getElementById("setconfig").addEventListener("click", setconfig);
document.getElementById("rootdir").addEventListener("click", setlocation);