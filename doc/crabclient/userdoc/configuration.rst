.. _config-label:

Configuration
=============
The configuration has significantly changed from previous versions. In this section and related sub sections it is possible to see how to create a configuration for CRAB-3. Also, to help users switching to the new version a script that will convert an old configuration to the new one will be provided,

Modify the CRAB configuration file ``crabConfig.py`` according to your needs: a fully documented template will be soon available at ``$CRABDIR/doc/FullConfiguration.py``, a template with essential parameters is available at ``$CRABDIR/doc/ExampleConfiguration.py``.


Description
+++++++++++

In CRAB-3 era the configuration is in Python language and it consists of configuration object imported from the core library deployed together with the client.
It can be instanciated as in the following example:

.. literalinclude:: ../../config/ExampleConfiguration.py
   :lines: 5,8

Once the Configuration object has been created it is possible to add new sections directly into it. To create a new section and to add parameters it is enough to use the following code:

.. literalinclude:: ../../config/ExampleConfiguration.py
   :lines: 10-12

Currently 5 sections are supported: **General**, **JobType**, **Data**, **User** and **Site**.
In this table below you can find a list of the most relevant section paramters with the corresponding description and format.

+---------+-------------+--------+-------------------+--------------------------------------------------------------------------------+
| Section | Parameter   | Format | Default           | Description                                                                    |
+=========+=============+========+===================+================================================================================+
| General | requestName | string |                   | The name of the workflow you want to create                                    |
+         +-------------+--------+-------------------+--------------------------------------------------------------------------------+           
|         | serverUrl   | string | crab.cern.ch:8080 | The server url to use as endpoint                                              |
+---------+-------------+--------+-------------------+--------------------------------------------------------------------------------+
| JobType | pluginName  | string |                   | The JobType you want to use (only Cmssw is currently supported)                |
+         +-------------+--------+-------------------+--------------------------------------------------------------------------------+
|         | psetName    | string |                   | The file name of your CMSSW pset file; this can be a relative or absolute path |
+         +-------------+--------+-------------------+--------------------------------------------------------------------------------+
|         | inputFiles  | list   |                   | Additional input files required.                                               |
+         +-------------+--------+-------------------+--------------------------------------------------------------------------------+
|         | outputFiles | list   |                   | The name of produced output files.                                             |
+---------+-------------+--------+-------------------+--------------------------------------------------------------------------------+
| Data    | inputDataset| string |                   | The name of the input dataset you want to analyze                              |
+         +-------------+--------+-------------------+--------------------------------------------------------------------------------+
|         | splitting   | string |                   | One of 'LumiBased', 'EventBased' and 'FileBased'                               |
+         +-------------+--------+-------------------+--------------------------------------------------------------------------------+
|         | lumiMask    | string |                   | The name of the json file you want to use for the lumi mask                    |
+         +-------------+--------+-------------------+--------------------------------------------------------------------------------+
|         | unitsPerJob | integer|                   | A number indicating the number of files/event/lumis per job                    |
+---------+-------------+--------+-------------------+--------------------------------------------------------------------------------+
| User    | voRole      | string |                   | The role used for the submission                                               |
+         +-------------+--------+-------------------+--------------------------------------------------------------------------------+
|         | voGroup     | string |                   | The group used for the submission                                              |
+---------+-------------+--------+-------------------+--------------------------------------------------------------------------------+
| Site    | storageSite | string |                   | The site where job's output will be put                                        |
+         +-------------+--------+-------------------+--------------------------------------------------------------------------------+
|         | whitelist   | list   |                   | List of sites where you want to run your jobs                                  |
+         +-------------+--------+-------------------+--------------------------------------------------------------------------------+
|         | blacklist   | list   |                   | List of sites where you do not want to run your jobs                           |
+---------+-------------+--------+-------------------+--------------------------------------------------------------------------------+


.. _basic-config:

Basic Example
+++++++++++++

This is a basic example of configuration:

.. literalinclude:: ../../config/ExampleConfiguration.py


Job Splitting
+++++++++++++

We support three kinds of job splitting currently: Split by event, lumi, or file. Unlike CRAB2 you can only specify how many units are in each job. Two variables in the config file are used to specify this::

    config.Data.splitting = 'SplitMethod'
    config.Data.unitsPerJob = 5

There are defaults for the unitsPerJob, but they may not be what you want. Unlike CRAB2, one setting is used to configure how big the job is regardless of which type of splitting is used. The three types of splitting are configured like this:

Config for splitting by lumi::

    config.Data.splitting = 'LumiBased'
    config.Data.unitsPerJob = 50

Config for splitting by event::

    config.Data.splitting = 'EventBased'
    config.Data.unitsPerJob = 1000

Config for splitting by file::

    config.Data.splitting = 'FileBased'
    config.Data.unitsPerJob = 5

Finally, you can supply a .json file of good lumi sections. In that case you must currently split by lumi::

    config.Data.lumiMask = 'lumiInput.json'
    config.Data.splitting = 'LumiBased'
    config.Data.unitsPerJob = 50


Set Site White and Black Lists
++++++++++++++++++++++++++++++

It is possible to add a whitelist (or a blacklist) for sites where you whish your jobs are run (or not run)::

    config.Site.whitelist = ['T2_XX_YYY', 'T2_XX_ZZZ']
    config.Site.blacklist = ['T2_XX_YYY', 'T2_XX_ZZZ']


.. _convert-config:

Converting CRAB2 configurations
+++++++++++++++++++++++++++++++

There is an utility which can help you converting an existing CRAB2 config file to a config file which can be understood by CRAB3::

    $ crab2cfgTOcrab3py crab2configName.cfg crab3configName.py

The command will take care of generating the crab3 config file, and will use the crab3 filename specified in the command line for the task name. In the generated configuration file you need to specify the config.General.serverUrl parameter.

If another required CRAB3 parameter is not specified in the CRAB2 config file you will get a message like that::

    [lxplus316] ~ $ crab2cfgTOcrab3py crab2.cfg crab3taskname.py
    WARNING: Not all the mandatory parameters were found in the crab2 configuration file. Open the crab3 generated config file to know which

And opening the config file you will see which parameters you need to modify::

    from CRABClient.Configuration import Configuration
    config = Configuration()
    config.section_('General')
    config.General.requestName = 'crab3taskname'
    config.General.serverUrl = 'You need to set the server URL'
    config.section_('JobType')
    config.JobType.psetName = 'simple_PAT_data_387_Only_MuEle_cfg.py'
    config.JobType.pluginName = 'Cmssw'
    config.section_('Data')
    config.Data.inputDataset = '/Mu/Run2010B-Nov4ReReco_v1/AOD'
    config.Data.unitsPerJob = 10
    config.Data.runWhitelist = '149011-150000'
    config.Data.splitting = 'LumiBased'
    config.Data.processingVersion = ''
    config.Data.lumiMask = 'Cert_136033-149442_7TeV_Nov4ReReco_Collisions10_JSON.txt'
    config.section_('User')
    config.User.email = ''
    config.section_('Site')
    config.Site.storageSite = 'Cannot find parameter "storage_element" in section "[USER]" of the crab2 config file'

In this case config.Site.storageSite must be modified by hand.

You can also call the command specifying only the CRAB2 config::

    $ crab2cfgTOcrab3py crab2configName.cfg

In that case a default crabConfig.py is generated. Be caraful that in this case the task name will be crabConfig, and it will be advisable to change it.

Passing configuration parameter from command line
+++++++++++++++++++++++++++++++++++++++++++++++++

It is possible to override parameters specified in the configuration files by passing them through the command line when the submission command is executed. Parameters can be set with Name=Value convention and can be sequentially listed just separating them with a blank space::

    Data.inputDataset=/higgs/is/here General.requestName=an_higgs_01

An example of the command looks like this::

    crab submit Data.inputDataset=/higgs/is/here General.requestName=an_higgs_01


CMSSW Configuration
+++++++++++++++++++

Like CRAB2, you should be able to run most CMSSW config files. Currently there are two differences with CRAB2.

* To pass parameters to your python config (pycfg_params in CRAB2) use config settings like this::

.. code-block:: python

    config.JobType.pyCfgParams    = ['var1=value1', 'var2=value2']

* CRAB3 has the ability to handle multiple output files belonging to different datasets. To specify this, you must include a production-like fragment in your CMSSW.py output modules like this::

.. code-block:: python

    process.copyOne = cms.OutputModule("PoolOutputModule",
        outputCommands = cms.untracked.vstring("drop *", "keep TriggerResults_*_*_*"),
        fileName = cms.untracked.string('output.root'),
        dataset = cms.untracked.PSet(
            filterName = cms.untracked.string('OutOne'),
        )
    )

    process.copyTwo= cms.OutputModule("PoolOutputModule",
        outputCommands = cms.untracked.vstring("drop *", "keep TriggerResults_*_*_*"),
        fileName = cms.untracked.string('output2.root'),
        dataset = cms.untracked.PSet(
           filterName = cms.untracked.string('OutTwo'),
        )
    )

The datasets will be named ``"config.Data.publishName"-"filterName"-"config.Data.processingVersion"``. The "dataset" PSet is optional if you are only writing one file.  
