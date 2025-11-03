# dagster_london_walks

This is a [Dagster](https://dagster.io/) project scaffolded with [`dagster project scaffold`](https://docs.dagster.io/guides/build/projects/creating-a-new-project).

## Getting started

First, install your Dagster code location as a Python package. By using the --editable flag, pip will install your Python package in ["editable mode"](https://pip.pypa.io/en/latest/topics/local-project-installs/#editable-installs) so that as you develop, local code changes will automatically apply.

```bash
pip install -e ".[dev]"
```

Then, start the Dagster UI web server:

```bash
dagster dev
```

Open http://localhost:3000 with your browser to see the project.

You can start writing assets in `dagster_london_walks/assets.py`. The assets are automatically loaded into the Dagster code location as you define them.

## Change Log

Date | Change | Reason
---- | ------ | ------
2025-11-03 | Added CSV of London Walks data | S3 bucket that the data was written to - `david-dagster-input` - has now been deleted

## Using uv for Dependency Management

```{bash}
pip install uv # install uv package & project manager

uv sync # install python dependencies

.venv\Scripts\activate # activate venv

uv pip install dagster-aws # Install specific package
```

### Conventional Commits

This repo uses [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) to create an explicit and easily-traceable commit history. Every Git commit message is prefixed with one of these words:

* **build**: Changes that affect the build system or external dependencies (example scopes: gulp, broccoli, npm)
* **ci**: Changes to our CI configuration files and scripts (example scopes: Travis, Circle, BrowserStack, SauceLabs)
* **docs**: Documentation only changes
* **feat**: A new feature
* **fix**: A bug fix
* **perf**: A code change that improves performance
* **refactor**: A code change that neither fixes a bug nor adds a feature
* **style**: Changes that do not affect the meaning of the code (white-space, formatting, missing semi-colons, etc)
* **test**: Adding missing tests or correcting existing tests

[Source](https://github.com/angular/angular/blob/22b96b9/CONTRIBUTING.md#type "List of Conventional Commit Types")

To see examples, look at the commit history of this repo.