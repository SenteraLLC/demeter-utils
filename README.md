# demeter-utils

A Python library that provides wrapper functions for interacting with the demeter database at a higher level.

## Setup and Installation (for development)
1) [Set up SSH](https://github.com/SenteraLLC/install-instructions/blob/master/ssh_setup.md)
2) Install [pyenv](https://github.com/SenteraLLC/install-instructions/blob/master/pyenv.md) and [poetry](https://python-poetry.org/docs/#installation).
3) Install package
``` bash
git clone git@github.com:SenteraLLC/demeter-utils.git
cd demeter-utils
pyenv install $(cat .python-version)
poetry config virtualenvs.in-project true
poetry env use $(cat .python-version)
poetry install
```
**CAUTION**: On WSL, some dependencies (e.g., psycopg2) return a `ChefBuildError` and fail to install. Installing `libpq-dev` and `python3-dev` should solve this.
``` bash
sudo apt install libpq-dev python3-dev
```
4) Set up `pre-commit` to ensure all commits to adhere to **black** and **PEP8** style conventions.
``` bash
poetry run pre-commit install
```
### Release/Tags
- A GitHub release is created on every push to the main branch using the `create_github_release.yml` Github Action Workflow
- Releases can be created manually through the GitHub Actions UI as well.
- The name of the Release/Tag will match the value of the version field specified in `pyproject.toml`
- Release Notes will be generated automatically and linked to the Release/Tag

## Setup and Installation (used as a library)
If using `demeter-utils` as a dependency in your script, simply add it to the `pyproject.toml` in your project repo. Be sure to uuse the `ssh:` prefix so Travis has access to the repo for the library build process.

<h5 a><strong><code>pyproject.toml</code></strong></h5>

``` toml
[tool.poetry.dependencies]
pixels_utils = { git = "ssh://git@github.com/SenteraLLC/demeter-utils.git", branch = "main"}
```

Install `demeter-utils` and all its dependencies via `poetry install`.

``` console
poetry install
```

## Usage Example
TBD