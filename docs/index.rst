Welcome to astro-provider-anyscale's documentation!
===================================================

.. toctree::
   :maxdepth: 1
   :caption: Contents:

   Home <self>
   API Reference <api/anyscale_provider>
   Contributing <CONTRIBUTING>
   Code of Conduct <CODE_OF_CONDUCT>

This repository provides a set of tools for integrating Anyscale with Apache Airflow, enabling the orchestration of Anyscale jobs and services within Airflow workflows. It includes a custom hook, two operators, and two triggers specifically designed for managing and monitoring Anyscale jobs and services.

Components
----------

Hook
~~~~
- **AnyscaleHook**: Facilitates communication between Airflow and Anyscale. It uses the Anyscale API to interact with the Anyscale platform, providing methods to submit jobs, query their status, and manage services.

Operators
~~~~~~~~~
- **SubmitAnyscaleJob**: This operator submits a job to Anyscale. It takes configuration parameters for the job, such as the entrypoint, image URI, and compute configuration. The operator uses ``AnyscaleHook`` to handle the submission process.
- **RolloutAnyscaleService**: Similar to the job submission operator, this operator is designed to manage services on Anyscale. It can be used to deploy new services or update existing ones, leveraging ``AnyscaleHook`` for all interactions with the Anyscale API.

Triggers
~~~~~~~~
- **AnyscaleJobTrigger**: Monitors the status of asynchronous jobs submitted via the ``SubmitAnyscaleJob`` operator. It ensures that the Airflow task waits until the job is completed before moving forward in the DAG.
- **AnyscaleServiceTrigger**: Works in a similar fashion to the ``AnyscaleJobTrigger`` but is focused on service rollout processes. It checks the status of the service being deployed or updated and returns control to Airflow upon completion.

Configuration Details for Anyscale Integration
----------------------------------------------

To integrate Airflow with Anyscale, you will need to provide several configuration details:

- **Anyscale API Token**: Obtain your API token either by using the anyscale cli or through the `Anyscale console <https://console.anyscale.com/v2/api-keys?api-keys-tab=platform>`_.

- **Compute Config (optional)**: This ID specifies the machines that will execute your Ray script. You can either:

  - Dynamically provide this via the ``compute_config`` input parameter, or
  - Create a compute configuration in Anyscale and use the resulting ID in the ``compute_config_id`` parameter.

- **Image URI**: Specify the docker image you would like your operator to use. Make sure your image is accessible within your Anyscale account. Note, you can alternatively specify a containerfile that can be used to dynamically build the image.

Usage
-----

Install the Anyscale provider using the command below:

.. code-block:: sh

    pip install astro-provider-anyscale

Airflow Connection Configuration
--------------------------------

To integrate Airflow with Anyscale, configure an Airflow connection with a unique name and set the password as the API token gathered through the Anyscale console.

1. **Access Airflow Web UI:**
   - Open the Airflow web interface and log in using your Airflow credentials.

2. **Create a New Connection in Airflow:**
   - Go to the "Admin" tab and select "Connections" from the dropdown menu.
   - Click the "Add a new record" button to create a new connection.

3. **Configure the Connection:**

   - **Conn Id:** Enter a unique identifier for the connection, e.g., ``anyscale_conn``.
   - **Conn Type:** Select ``Anyscale``
   - **Password:** Paste the API token you copied from the Anyscale console.

4. **Save the Connection:**
   - After filling in the required details, click the "Save" button at the bottom of the form to save the new connection.

Code samples
------------

The below script is an example of how to configure and use the ``SubmitAnyscaleJob`` operator within an Airflow DAG:

.. literalinclude:: ../example_dags/anyscale_job.py


The below script uses the ``RolloutAnyscaleService`` operator to deploy a service on Anyscale:

.. literalinclude:: ../example_dags/anyscale_service.py

Changelog
---------

We follow `Semantic Versioning <https://semver.org/>`_ for releases.
Check `CHANGELOG.rst <https://github.com/astronomer/astro-provider-anyscale/blob/main/docs/CHANGELOG.rst>`_
for the latest changes.

License
-------

`Apache License 2.0 <https://github.com/astronomer/astronomer-cosmos/blob/main/LICENSE>`_
