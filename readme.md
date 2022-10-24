### Setup

- #### How to install Python

🚨 This setup works with Python 3.7.15

check your python3 version
```bash 
$ which python3
/usr/local/bin/python3
```
If the version doesn't match the version installed, set the path to python in your bash_profile file or zshrc file
or add the following line at the end of the file
```
alias python='/usr/local/bin/python3'
```

- #### How to Setup the env 
In order to setup your dev environment, launch the following commands in the root directory:

```bash
python -m venv venv  # only the first time
pip install pip-tools   # only the first time, in order to get pip-compile
source venv/bin/activate  # every time you start working on the project
pip install --upgrade pip # the first time and every time a dependency changes
pip install -r requirements-tests.txt  # the first time and every time a dependency changes
pip install -e .  # only the first time