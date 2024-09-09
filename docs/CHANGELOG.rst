CHANGELOG
=========

1.0.1 (2024-09-09)
------------------

* Minor update to return job_id from execute() and execute_complete() methods

1.0.0 (2024-06-28)
------------------

* Initial release, with the following hooks, operators, and triggers:

.. list-table::
   :header-rows: 1

   * - Hook Class
     - Import Path

   * - ``AnyscaleHook``
     - .. code-block:: python

            from anyscale_provider.hooks.anyscale import AnyscaleHook

.. list-table::
   :header-rows: 1

   * - Operator Class
     - Import Path
     - Example DAG

   * - ``SubmitAnyscaleJob``
     - .. code-block:: python

            from anyscale_provider.operators.anyscale import SubmitAnyscaleJob
     - `anyscale_job <https://github.com/astronomer/astro-provider-anyscale/blob/main/example_dags/anyscale_job.py>`_

   * - ``RolloutAnyscaleService``
     - .. code-block:: python

            from anyscale_provider.operators.anyscale import RolloutAnyscaleService
     - `anyscale_service <https://github.com/astronomer/astro-provider-anyscale/blob/main/example_dags/anyscale_service.py>`_

.. list-table::
   :header-rows: 1

   * - Trigger Class
     - Import Path

   * - ``AnyscaleJobTrigger``
     - .. code-block:: python

            from anyscale_provider.triggers.anyscale import AnyscaleJobTrigger

   * - ``AnyscaleServiceTrigger``
     - .. code-block:: python

            from anyscale_provider.triggers.anyscale import AnyscaleServiceTrigger
