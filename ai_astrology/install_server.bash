sudo apt install git-all
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install.sh)"
eval $(/home/linuxbrew/.linuxbrew/bin/brew shellenv)
sudo apt-get install build-essential
brew install gcc
brew install wget
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
bash Anaconda3-5.1.0-Linux-x86-64.sh
source ~/.bashrc

conda env create --name brain python=3.6 pandas numpy scikit-learn pyjwt uszipcode simplejson
pip install uszipcode
pip install markdown==2.6.1
pip install flatlib==0.2.1
pip install pyswisseph==2.00.00-2