@"%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe" -NoProfile -InputFormat None -ExecutionPolicy Bypass -Command "iex ((New-Object System.Net.WebClient).DownloadString('https://chocolatey.org/install.ps1'))" && SET "PATH=%PATH%;%ALLUSERSPROFILE%\chocolatey\bin"
choco install python
choco install nodejs
choco install git
git clone https://git.computecanada.ca/radiam/radiam-agent
dir radiam-agent
pip install -r requirements.txt --user
pip install pyinstaller --user
bash pyinstaller.sh
npm i -g electron-packager
npm i -g electron-installer-windows
dir tray
npm install
electron-packager . --icon=resources/icon.ico
electron-installer-windows --src radiamagent-win32-x64/ --dest install/ --config config.json --certificateFile radiam.pfx --certificatePassword [password]