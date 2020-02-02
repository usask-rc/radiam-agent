#!/bin/bash

if [[ "$OSTYPE" == "darwin"* ]]; then
	cp /usr/local/lib/python3.*/site-packages/magic/magic.py .
	cp -R /usr/local/lib/python3.*/site-packages/magic/libmagic/ .
elif [[ "$OSTYPE" == "msys" ]]; then
	if [ -d ~/AppData/Local/Programs/Python ]; then
		cp ~/AppData/Local/Programs/Python/Python3*/Lib/site-packages/magic/magic.py .
		cp -R ~/AppData/Local/Programs/Python/Python3*/Lib/site-packages/magic/libmagic/ .
	elif [ -d /c/ProgramData/chocolatey/lib/python3 ]; then
		cp /c/ProgramData/chocolatey/lib/python3/tools/Lib/site-packages/magic/magic.py .
		cp -R /c/ProgramData/chocolatey/lib/python3/tools/Lib/site-packages/magic/libmagic/ .
	elif [ -d /c/Program\ Files/Python37-32 ]; then
		cp /c/Program\ Files/Python37-32/Lib/site-packages/magic/magic.py .
		cp -R /c/Program\ Files/Python37-32/Lib/site-packages/magic/libmagic/ .
	fi
fi
sed -E "s/__file__\), 'libmagic'\)/sys.executable), 'libmagic')/g" -i magic.py
pyinstaller -w radiam_tray.py --distpath tray
cp -R libmagic tray/radiam_tray/libmagic
rm magic.py
rm libmagic/*
rmdir libmagic