Contributions
=============

Hi there! We're thrilled that you'd like to contribute to this project. Your help is essential for keeping it great.

Please note that this project is released with a `Contributor Code of Conduct <CODE_OF_CONDUCT.md>`_.
By participating in this project you agree to abide by its terms.

Overview
========

To contribute to the **Astro Anyscale** project:

#. Please create a `GitHub Issue <https://github.com/astronomer/astro-provider-anyscale/issues>`_ describing your contribution.
#. Open a feature branch off of the ``main`` branch and create a Pull Request into the ``main`` branch from your feature branch.
#. Link your issue to the pull request.
#. Once developments are complete on your feature branch, request a review and it will be merged once approved.

Test Changes Locally
====================

Pre-requisites
--------------

* pytest

.. code-block:: bash

    pip install pytest

Run tests
---------

All tests are inside ``./tests`` directory.

- Just run ``pytest filepath+filename`` to run the tests.

Static Code Checks
==================

We check our code quality via static code checks. The static code checks in astro-provider-anyscale are used to verify
that the code meets certain quality standards. All the static code checks can be run through pre-commit hooks.

Your code must pass all the static code checks in the CI in order to be eligible for Code Review.
The easiest way to make sure your code is good before pushing is to use pre-commit checks locally.

Pre-Commit
----------

We use pre-commit to run a number of checks on the code before committing. To install pre-commit, run the following from
your cloned ``astro-provider-anyscale`` directory:

.. code-block:: bash

    pip install pre-commit
    pre-commit install

To run the checks manually, run:

.. code-block:: bash

    pre-commit run --all-files

Our pre-commit configuration includes the following checks:

* Check if .env file is empty
* Check for added large files
* Check for merge conflicts
* Check TOML files
* Check YAML files (with --unsafe flag)
* Remove debug statements
* Fix end-of-file issues
* Normalize mixed line endings
* Pretty format JSON files (with --autofix)
* Remove trailing whitespace
* Check for common misspellings with codespell
* Ensure correct usage of backticks in reStructuredText files
* Check Python mock methods
* Remove CRLF line endings
* Remove tabs
* Upgrade Python syntax with pyupgrade (for Python 3.7+)
* Lint Python code with ruff (with --fix)
* Format Python code with black (configured with pyproject.toml)
* Format Python code in documentation with blacken-docs
* Type check with mypy (configured with pyproject.toml)

For more details on each hook and additional configuration options, refer to the official pre-commit documentation: https://pre-commit.com/hooks.html

Building
========

We use `hatch <https://hatch.pypa.io/latest/>`_ to build the project. To build the project, run:

.. code-block:: bash

    hatch build

Releasing
=========

We use GitHub actions to create and deploy new releases. To create a new release, first create a new version using:

.. code-block:: bash

    hatch version minor

hatch will automatically update the version for you. Then, create a new release on GitHub with the new version. The release will be automatically deployed to PyPI.

.. note::
    You can update the version in a few different ways. Check out the `hatch docs <https://hatch.pypa.io/latest/version/#updating>`_ to learn more.

To validate a release locally, it is possible to build it using:

.. code-block:: bash

    hatch build

To publish a release to PyPI, use:

.. code-block:: bash

    hatch publish