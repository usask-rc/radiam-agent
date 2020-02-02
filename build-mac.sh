#!/bin/bash

/usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
brew install python3
brew install node
git clone https://git.computecanada.ca/radiam/radiam-agent
cd radiam-agent
pip3 install -r requirements.txt --user
bash pyinstaller.sh
pyinstaller -w radiam_tray.py --distpath tray
npm i -g electron-packager
cd tray
npm install
electron-packager . --icon=resources/icon.icns
hdiutil create tmp.dmg -ov -volname "RadiamAgent" -fs HFS+ -srcfolder radiamagent-darwin-x64/ && hdiutil convert tmp.dmg -format UDZO -o RadiamAgent.dmg && rm tmp.dmg