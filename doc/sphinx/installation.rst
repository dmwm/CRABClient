.. _installation-label:

Installation
============

The setup of the local environment has the same logic than in previous `CRAB <https://twiki.cern.ch/twiki/bin/view/CMS/CRAB>`_ versions. You need to setup the environment in the proper order, as listed here below:

* source the LCG User Interface environment to get access to WLCG-affiliated resources in a fully transparent way; on LXPLUS users have it on AFS::

.. code-block:: console

    $ source /afs/cern.ch/cms/LCG/LCG-2/UI/cms_ui_env.csh


* install CMSSW project and setup the relative environment::

.. code-block:: console

    $ mkdir MyTasks
    $ cd MyTasks
    $ cmsrel CMSSW_4_1_X
    $ cd CMSSW_4_1_X/src/
    $ cmsenv


* source the CRAB script file: in order to setup and use CRAB-3 from any directory, source the script /afs/cern.ch/user/g/grandi/public/CRAB3/client/CRABClient/crab.(c)sh, which points to the version of CRAB-3 maintained by the Integration team::

.. code-block:: console

    $ source /afs/cern.ch/user/g/grandi/public/CRAB3/client/CRABClient/crab.sh

.. note:: The order in which are listed the points above it is important to avoid conflicts between the various software stacks involved.

.. TODO Add RPM based installation
