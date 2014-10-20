Workflow execution
==================
Once your ``crabConfig.cfg`` file is ready and the whole underlying environment is ready, you can start running CRAB client. CRAB-3 client supports command line help which can be useful for the first time you are running it and when you need to look for detailed information.

Note that most of the documentation on how to run it is hosted in this page, but next releases of the client will improve the commmand line documentaion.

.. note:: To execute the following commands you need to set up the client environment (see Section :ref:`Installation <installation-label>`)

You can get command line help of the client by running::

    $ crab -h

The help will also list the available commands you can ask to the client to run. To retrieve the help for a specific command just type::

    $ crab command -h

This is because each option passed between the crab executable and the command refers to general options not command dependent, while options passed after the crab command refers to specific command options. As example, if you want to execute commands with detailed information add the --debug option after crab::

    $ crab -d command


CRAB3 basic commands workflow
+++++++++++++++++++++++++++++

These are the main CRAB commands available in the **3.0.x** version. In the picture you can see the logic of the workflow of when you can type them:

    * ``crab submit``: this is the first operation to do when a new workflow has to be submitted to the server;

    * ``crab status``: shows the status progress of your tasks, or detailed information of the task;

    * ``crab get-output``: retrieves the output files of the successful jobs specified by -r/--range option part of the task identified by -t/--task option;

    * ``crab get-log``: retrieves the log files of the finished jobs specified by -r/--range option part of the task identified by -t/--task option;

    * ``crab get-error``: retrieves the post mortem infromation of all the failed jobs in the task, or detailed information of just one failed job;

    * ``crab report``: gets the list of good lumis for your task identified by the -t/--task option.

    * ``crab kill``: abort the task identified by the -t/--task option and all kill all the jobs associated to it.

    * ``crab resubmit``: resubmit the failed jobs of a finished task.

.. image:: images/CRAB3wf.png

.. _submission:

Submission
++++++++++
At this point in order to submit a new workflow to the CRAB Server you should already have corretcly installed the environment.
To perform the submission you can use the **submit** command of CRABClient:

.. TODO: we should make this automatic when building the documentation

.. code-block:: console

   $ crab submit -h
     Usage: crab submit [options] [args]

     Perform the submission to the CRABServer

     Options:
       -h, --help            show this help message and exit
       -c FILE, --config=FILE
                             CRAB configuration file
       --proxy=PROXYFILE
                             Skip Grid proxy creation and myproxy delegation
       -s http://HOSTNAME:PORT, --server=http://HOSTNAME:PORT
                             Endpoint server url to use

.. code-block:: console

   $ crab submit -c crabConfig.py

How to prepare a CRAB configuration is described in the :ref:`Configuration <config-label>` section.


Job Submission
++++++++++++++

The job creation step of CRAB-2 release series is not anymore needed in CRAB-3 version. This step of the workflow has been moved at the server level, which means that data (location) discovery, job splitting and final sandbox definition are delegated through the client submission command. Given this, the submission of a workflow in CRAB-3 corresponds to a full delegation of actions to the server level, reducing the time spent by the user waiting the command execution and optimizing the result of the creation/submission step at server level.

This also explains why the submission command does not accept anymore the range of jobs option. In fact the number of jobs created is not known till the server has performed the data location and splitting operations. The only way currently available to reduce the number of jobs being submitted processing less data is to define in the Data section of the configuraion run/block whit/black list.

To submit a new task/workflow simply executes the command below:

.. code-block:: console

    $ crab submit

By default the command above will use the CRAB configuration file names ``crabConfig.py`` presents in the current working directory. If you want, you can specify the CRAB configuration file through a specific command line option:

.. code-block:: console

    $ crab submit -c  my-crab-config.py

which should produce an output on the terminal screen similar to this one::

    [lxplus432] ~/scratch1/MyTests $ crab submit
    Checking credentials
    Enter GRID pass phrase:
    Registering user credentials
    Enter GRID pass phrase for this identity:
    Sending the request to the server
    Submission completed


Job Status Check
++++++++++++++++

To check the status of your request you can type:

.. code-block:: console

    $ crab status

to check a specific task just add the task optiojn:

.. code-block:: console

    $ crab status -t  <dir name>

which should produce a similar output on the terminale screen like::

    #1 username_crab_taskname_111225_165620
       Task Status:        running
       Analysis jobs
         State: failure       Count:      1  Jobs: 5
         State: running       Count:      4  Jobs: 1-4

(merry Christmas!)

Description of possible task status.

+--------------+-------------------------------------------------------------------------------------+
| Status       | Meaning                                                                             |
+==============+=====================================================================================+
| assigned     | The workflow is taken by the server and it is in the central system                 |
+--------------+-------------------------------------------------------------------------------------+
| negotiating  | The workflow is taken by the server and it is being queued in the central queue     |
+--------------+-------------------------------------------------------------------------------------+
| acquired     | The workflow has been pulled from the submission server                             |
+--------------+-------------------------------------------------------------------------------------+
| running      | Jobs of the workflow have been submitted to the sites                               |
+--------------+-------------------------------------------------------------------------------------+
| failed       | The workflow is failed                                                              |
+--------------+-------------------------------------------------------------------------------------+
| completed    | The workflow has been completed to run                                              |
+--------------+-------------------------------------------------------------------------------------+
| aborted      | The workflow has been centrally aborted by the operator.                            |
+--------------+-------------------------------------------------------------------------------------+

Description of possible status for the jobs.

+----------+-----------------------------------------------------------------------------+
|Status    |  Meaning                                                                    |
+----------+-----------------------------------------------------------------------------+
|pending   |  job is in the queue to be submitted by the agent                           |
+----------+-----------------------------------------------------------------------------+
|running   |  job submitted to the site                                                  |
+----------+-----------------------------------------------------------------------------+
|cooloff   |  job finished and failed - automatic resubmissions will be re-done          |
+----------+-----------------------------------------------------------------------------+
|success   |  job finished with success                                                  |
+----------+-----------------------------------------------------------------------------+
|failure   |  job finished and failed - automatic resubmissions have been already done   |
+----------+-----------------------------------------------------------------------------+
|cleanout  |  job has been cleaned from the server                                       |
+----------+-----------------------------------------------------------------------------+

In some cases it is possilbe to see jobs in the transitioning status: this means that the job is about to change from one status to another one; generally the job sits in this state for very short time.

Troubleshooting by task status of possible issues
+++++++++++++++++++++++++++++++++++++++++++++++++

    * Requests stuck in the negotiating state: it's currently possible for a request to become stuck in the "negotiating" state if something bad happens to the Global WorkQueue while it is working on the request. If this happens you need expert support.

    * Requests that immediately go to the failed state: if a request goes immediately to the "failed" state it means that either that there was a problem in the Global WorkQueue while it was processing it or no data is available to process. Double check that the input dataset exists and that white/black lists of blocks are correctly set in the CRAB configuration.

    * Requests are stuck in acquired state:

    * Requests are stuck in assigned state:

    * Requests aren't being completed: ask for expert support.


Job Output Retrieval
++++++++++++++++++++

One of the major changes between CRAB-2 and CRAB-3 is on the output file managements. CRAB-3 first will always make the local jobs to stage out in the storage element of the site where the job run and then the AsyncStageout component will take care to transfer the files to the final destination (the site defined in the CRAB configuration file sipplied at submission time). Then the output retrieval operation is basically the execution of a series of copy commands of the job output files from the remote storage element to your local host. For the jobs which are succeeded is possible to retrieve the output through the command:

.. code-block:: console

    $ crab get-output -t <dir name> -r [job lists/ranges]

the job results will be copied in the res subdirectory of your crab project, but you can always define through the command line outputpath option to store the files directly in another path. Here below an example of how the screen output of the command should look like::

    [lxplus432] ~/scratch1/MyTests $ crab get-output --task=crab_MyAnalysis1 --range=1-5
    Checking credentials
    Starting retrieving remote files for requested jobs ['1', '2', '3', '4', '5']
    Job 1: output in /afs/cern.ch/user/m/mcinquil/scratch1/MyTests/crab_MyAnalysis1/results/output_1.1.root
    Job 2: output in /afs/cern.ch/user/m/mcinquil/scratch1/MyTests/crab_MyAnalysis1/results/output_2.1.root
    Job 3: output in /afs/cern.ch/user/m/mcinquil/scratch1/MyTests/crab_MyAnalysis1/results/output_3.1.root
    Job 4: output in /afs/cern.ch/user/m/mcinquil/scratch1/MyTests/crab_MyAnalysis1/results/output_4.1.root
    Job 5: output in /afs/cern.ch/user/m/mcinquil/scratch1/MyTests/crab_MyAnalysis1/results/output_5.1.root
    Retrieval completed


Job Log Retrieval
+++++++++++++++++

Another change between CRAB-2 and CRAB-3 is on the log file management. As for the output the log files are always stored in the storage element. Then the log retrieval operation is basically the execution of a series of copy commands of the job output files from the remote storage element to your local host. For the jobs which are finished it is possible to retrieve the log through the command:

.. code-block:: console

    $ crab get-log -t <dir name> -r [job lists/ranges]

The job log files will be copied in the res subdirectory of your CRAB project, but you can always define through the command line outputpath option to store the files directly in another path.

Here below an example of how the screen output of the command should look like::

    [lxplus432] ~/scratch1/MyTests $ crab get-log -t crab_MyAnalysis1 -r 1-5
    Checking credentials
    Starting retrieving remote files for requested jobs ['1', '2', '3', '4', '5']
    Job 1: output in /afs/cern.ch/user/m/mcinquil/scratch1/MyTests/crab_MyAnalysis1/results/1.tgz
    Job 2: output in /afs/cern.ch/user/m/mcinquil/scratch1/MyTests/crab_MyAnalysis1/results/2.tgz
    Job 3: output in /afs/cern.ch/user/m/mcinquil/scratch1/MyTests/crab_MyAnalysis1/results/3.tgz
    Job 4: output in /afs/cern.ch/user/m/mcinquil/scratch1/MyTests/crab_MyAnalysis1/results/4.tgz
    Job 5: output in /afs/cern.ch/user/m/mcinquil/scratch1/MyTests/crab_MyAnalysis1/results/5.tgz
    Retrieval completed


Job Report
++++++++++

In order to retrieve the list of good lumi sections that the task has analyzed it is possible to use the following command:

.. code-block:: console

    $ crab report -t <dir name>

Here below an example of how the screen output of the command should look like::

    [lxplus406] ~/scratch1/MyTests $ crab report -t crab_MyAnalysis1/
    Sucessfully analyzed 84 lumi(s) from 1 run(s)
    Summary of processed lumi sections written to lumiReport.json

In the ``lumiReport.json`` file you can find the processed lumi sections listed by run, as example in the case above there was just one single run processed and 84 lumis::

    {"1": [[7, 7], [22, 22], [54, 54], [76, 76], [97, 97], [108, 108], [162, 162], [173, 173], [179, 179], [187, 187], [199, 199], [201, 201], [231, 231], [241, 241], [243, 243], [245, 245], [247, 247], [250, 250], [307, 307], [315, 315], [323, 323], [338, 338], [353, 353], [369, 369], [519, 519], [523, 524], [526, 526], [548, 548], [550, 550], [555, 555], [557, 557], [562, 562], [566, 566], [578, 578], [606, 606], [761, 761], [763, 764], [767, 768], [770, 770], [812, 812], [842, 842], [851, 851], [862, 862], [870, 870], [880, 880], [1555, 1556], [1560, 1562], [1565, 1565], [2032, 2032], [2036, 2036], [2120, 2120], [2143, 2143], [2163, 2163], [2175, 2176], [2180, 2180], [2198, 2199], [2490, 2490], [2510, 2510], [2515, 2516], [2527, 2527], [2530, 2530], [2604, 2604], [2643, 2643], [2646, 2646], [2648, 2648], [2865, 2865], [2873, 2873], [2926, 2926], [2932, 2932], [2940, 2943], [2947, 2948]]}

Killing a Task
++++++++++++++

To kill a task you can type:

.. code-block:: console

    $ crab kill -t <dir name>

which should simply produce a similar output on the terminale screen like::

    Task killed


Resubmitting a Task
+++++++++++++++++++

To resubmit a task you can type:

.. code-block:: console

    $ crab resubmit -t <dir name>

You can only resubmit completed tasks, so if you resubmit a running one you will get something similar to::

    ERROR: Bad Request (400):  'Request 'username_crab_taskname_111225_190306' not yet completed; impossible to resubmit.'

Otherwise, if you submit a finished task you will see::

    Resubmission succesfully requested

The failed jobs will be successfully resubmitted, and you will be able to track them with a get-status::

    #1 username_crab_taskname_111225_165620
       Task Status:        completed
       Analysis jobs
         State: failure       Count:      1  Jobs: 5
         State: running       Count:      4  Jobs: 1-4
    #2 username_crab_taskname_resubmit_111225_192828
       Task Status:        acquired

As long as the failed job(s) is running, you will see it under the second taskname.

You can also use the --force option during resubmission which will kill a task if it is still running

.. code-block:: console

    $ crab resubmit --force -t crab_taskname

which willl produce::

    Resubmission succesfully requested


Publishing your output
++++++++++++++++++++++

To publish your output, type:

.. code-block:: console

    $ crab publish -t <dir name> -c [config_file] -u [dbs_url]

only one of -c/-u is required. If you use -c, you must have specified::

    config.Data.publishDBS = 'https://cmsdbsprod.cern.ch:8443/cms_dbs_ph_analysis_02_writer/servlet/DBSServlet'

or similar in your config file. If you specify the DBS URL with -u, it will override any setting in the config file.

You can only publish data for completed tasks, the publication information does not exist on the server for incomplete tasks or for tasks that have completed very recently. You may publish before resubmitting a task and publish again after the resubmission has completed. Alternatively, you may wait until all resubmissions have completed before issuing the publish command. In either case, output from all successful jobs is published.

Datasets are named according to the config parameters::

    config.Data.publishDataName = 'EWVTestCRAB3Skim'
    config.Data.processingVersion = 'v%s' % myVersion

as as described in the next section in the case of multiple output files. Defaults are taken for both publishDataName and processingVersion if one or both are left unspecified.

