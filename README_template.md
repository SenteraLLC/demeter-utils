# <repo_name>

``<pkg_name>``: <pkg_description>

## Installation 

1) [Set up SSH](https://github.com/SenteraLLC/install-instructions/blob/master/ssh_setup.md)
2) Install [pyenv](https://github.com/SenteraLLC/install-instructions/blob/master/pyenv.md) and [poetry](https://python-poetry.org/docs/#installation)
3) Install package

        git clone git@github.com:SenteraLLC/<repo_name>.git
        cd <repo_name>
        pyenv install $(cat .python-version)
        poetry install
        
4) Set up ``pre-commit`` to ensure all commits to adhere to **black** and **PEP8** style conventions.

        poetry run pre-commit install
        
## Usage

Within the correct poetry/conda shell, run ``<pkg_name> --help`` to view available CLI commands.
