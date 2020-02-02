const notifier = require("node-notifier");
const remote = require('electron').remote;
let client = remote.getGlobal('client');

function login() {
  var username = document.getElementById("username").value;
  var password = document.getElementById("password").value;
  client.invoke("get_token", username, password, null, function(error, res, more) {
    if (res){
      if (res == "Token obtained.") {
        var window = remote.getCurrentWindow();
        window.close();
      }
      notifier.notify({"title" : "Radiam", "message" : res});
    } else {
      setTimeout(notifier.notify({"title" : "Radiam", "message" : "No response from server. Is it running and configured properly?"}), 1500)
    }
  });
}

document.getElementById("login").addEventListener("click", login);
