#!/usr/bin/env bash

# NOTE: this script is a reminder for project environment setup.
# It is not well-tested for start-to-finish execution.

# Update and upgrade the system
sudo apt-get update
sudo apt-get upgrade -y

# Install Java 21
sudo apt-get install -y default-jdk # development kit
sudo apt-get install -y openjdk-21-jdk

# install Python 3.12
sudo apt-get install -y python3.12
sudo apt-get install -y python3-pip
sudo apt-get install -y python3.12-venv

# Install virtual environment
python3.12 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip

# Install project dependencies
if [ -f requirements.txt ]; then    
    pip install -r requirements.txt
    echo "[DEPENDENCIES] Project dependencies installed from requirements.txt."
else
    pip install pyspark==4.1.* plotly==6.5.* pandas==2.3.*
    pip freeze > requirements.txt
    echo "[DEPENDENCIES] Created requirements.txt with current dependencies."
fi

# Confirm installations
unset INSTALL_STATUS
INSTALL_STATUS="==== Versions ================\n"
INSTALL_STATUS+=("[UBUNTU] $(lsb_release -r)\n")
INSTALL_STATUS+=("[JAVA] $(java --version | grep openjdk)\n")
INSTALL_STATUS+=("[PYTHON] $(python3 --version)\n")
INSTALL_STATUS+=("[VENV] $(basename "$VIRTUAL_ENV")\n")
INSTALL_STATUS+=("[PANDAS] $(pip show pandas | grep Version | head -n 1)\n")
INSTALL_STATUS+=("[PYSPARK] $(pip show pyspark | grep Version | head -n 1)\n")
INSTALL_STATUS+=("[PLOTLY] $(pip show plotly | grep Version | head -n 1)\n")
echo -e ${INSTALL_STATUS[@]}
unset INSTALL_STATUS
