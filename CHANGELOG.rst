
1.0.0 (2024-05-29)
------------------

    Initial release, with the following hooks, operators, and triggers:

Hook Class             Import Path                                           Example DAG
----------             ---------------------------------------               -----------
AnyscaleHook           ``from anyscale_provider.hooks.anyscale`` import AnyscaleHook              N/A

Operator Class         Import Path                                           Example DAG
--------------         ---------------------------------------               -----------
AnyscaleSubmitJob      ``from anyscale_provider.operators.anyscale import AnyscaleSubmitJob``     Example DAG
AnyscaleDeployService  ``from anyscale_provider.operators.anyscale import AnyscaleDeployService`` Example DAG

Trigger Class          Import Path                                           Example DAG
-------------          ---------------------------------------               -----------
AnyscaleJobTrigger     ``from anyscale_provider.triggers.anyscale import AnyscaleJobTrigger``     N/A
AnyscaleServiceTrigger ``from anyscale_provider.triggers.anyscale import AnyscaleServiceTrigger`` N/A