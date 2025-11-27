# Changelog

## 1.3.0 (2025-12-02)

Breaking changes

* `SubmitAnyscaleJob` and `RolloutAnyscaleService` are no longer Airflow deferrable operators. For more information check https://github.com/astronomer/astro-provider-anyscale/pull/99.

Bug fixes

* Fix Triggerer's async thread was blocked from being blocked by @tatiana in https://github.com/astronomer/astro-provider-anyscale/pull/99


## 1.2.0 (2025-11-24)

Breaking changes

* Python 2.8 and 2.9 reached end-of-life and are no longer supported from this release onwards.

Features

* Support setting any job parameters via the `SubmitAnyscaleJob` operator by @nxlouie in https://github.com/astronomer/astro-provider-anyscale/pull/69
* Support `RolloutAnyscaleService` non-default cloud and project by @p1c2u in https://github.com/astronomer/astro-provider-anyscale/pull/67
* Add support to Airflow 2.10, 2.11, 3.0 and 3.1 by @tatiana in https://github.com/astronomer/astro-provider-anyscale/pull/92

Bug fixes

* Fix ``RolloutAnyscaleService`` after Anyscale SDK breaking change by @tatiana in https://github.com/astronomer/astro-provider-anyscale/pull/94

Others

* Fix CI Anyscale Service id name by @tatiana in https://github.com/astronomer/astro-provider-anyscale/pull/97
* Fix Sphinx docs generation by @tatiana in https://github.com/astronomer/astro-provider-anyscale/pull/95
* Fix MyPy checks by @tatiana in https://github.com/astronomer/astro-provider-anyscale/pull/78
* Remove support to Python versions that reached end-of-life by @tatiana in
 https://github.com/astronomer/astro-provider-anyscale/pull/88
* Change GH Actions workflow trigger type to run tests from external contributors by @tatiana in https://github.com/astronomer/astro-provider-anyscale/pull/90
* Add missing unit test for PR #67 by @tatiana in https://github.com/astronomer/astro-provider-anyscale/pull/91
* Fix CODEOWNERS by @schnie in https://github.com/astronomer/astro-provider-anyscale/pull/83
* Update `test.yml`: Use pull_request for authorize as we don't have pull_request_target event configured by @pankajkoti in https://github.com/astronomer/astro-provider-anyscale/pull/72
* Update `release.yml`: Remove unnecessary Authorize step by @pankajkoti in https://github.com/astronomer/astro-provider-anyscale/pull/73
* Update `docs.yml`: Remove unneeded Authorize step by @pankajkoti in https://github.com/astronomer/astro-provider-anyscale/pull/74
* Improve type-checking implementation by @tatiana in https://github.com/astronomer/astro-provider-anyscale/pull/98
* Add Authorize Job in CI by @pankajastro in https://github.com/astronomer/astro-provider-anyscale/pull/71
* Bump GitHub Actions versions in https://github.com/astronomer/astro-provider-anyscale/pull/96, https://github.com/astronomer/astro-provider-anyscale/pull/77, https://github.com/astronomer/astro-provider-anyscale/pull/79, https://github.com/astronomer/astro-provider-anyscale/pull/80, https://github.com/astronomer/astro-provider-anyscale/pull/84, https://github.com/astronomer/astro-provider-anyscale/pull/85




## 1.1.0 (2025-03-31)

Features

* Add `target_job_queue` to `SubmitAnyscaleJob` operator by @RCdeWit in https://github.com/astronomer/astro-provider-anyscale/pull/52

Improvements

* Use an Airflow config to manage provider configuration by @tatiana in https://github.com/astronomer/astro-provider-anyscale/pull/41

Others

* Upgrade GitHub action artifacts upload-artifact & download-artifact to v4 by @pankajkoti in https://github.com/astronomer/astro-provider-anyscale/pull/53
* Add Dependabot configuration by @tatiana in https://github.com/astronomer/astro-provider-anyscale/pull/54
* Bump actions/checkout from 2 to 4 by @dependabot in https://github.com/astronomer/astro-provider-anyscale/pull/56
* Bump codecov/codecov-action from 4 to 5 by @dependabot in https://github.com/astronomer/astro-provider-anyscale/pull/55
* Bump actions/setup-python from 2 to 5 by @dependabot in https://github.com/astronomer/astro-provider-anyscale/pull/58
* Bump pypa/gh-action-pypi-publish from 1.8.14 to 1.12.4 by @dependabot in https://github.com/astronomer/astro-provider-anyscale/pull/59
* Fix running unittests locally when ANYSCALE_CLI_TOKEN is set by @tatiana in https://github.com/astronomer/astro-provider-anyscale/pull/61
* Fix example DAG sample_anyscale_service_workflow by @tatiana in https://github.com/astronomer/astro-provider-anyscale/pull/62
* Fix integration test hanging in the CI  by @tatiana in https://github.com/astronomer/astro-provider-anyscale/pull/63
* Fix broken tests by @tatiana in https://github.com/astronomer/astro-provider-anyscale/pull/64
