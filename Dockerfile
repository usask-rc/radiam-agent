# This needs a working X on the host, and may also need you to run:
# export DISPLAY=':0.0' && xhost local:root && xhost local:docker 
# Runs as docker run -v /tmp/.X11-unix:/tmp/.X11-unix -v /dev/tty0:/dev/tty0 radiam-agent 

# Pull base image
FROM python:3.5.7

# Set environment varibles
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
ENV DISPLAY :0

# Set work directory
WORKDIR /code

# Copy project
COPY . /code/

# Edit dependencies for Linux
RUN sed -i "s/python-magic-bin/python-magic/g" requirements.txt

# Install Python dependencies
RUN pip3 install -r requirements.txt

# Run Python tests
RUN python3 -m unittest test

# Switch to Electron environment
WORKDIR /code/tray

# Set up npm environment
RUN apt-get clean
RUN apt-get update
RUN apt-get -y install software-properties-common
RUN curl -sL https://deb.nodesource.com/setup_10.x | bash -
RUN DEBIAN_FRONTEND=noninteractive apt-get -y install nodejs libgtk-3-0 libgconf-2-4 libnss3 libasound2 dbus dbus-x11 libx11-xcb1

# Install Electron dependencies
RUN npm install

# Run Electron tests
CMD ["npm", "test"]
