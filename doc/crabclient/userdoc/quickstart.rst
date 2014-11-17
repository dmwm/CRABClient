Quickstart
==========
This section is intended for impatient users who are already confident with the work environment and tools. Please, read all the way through in any case!


* source crab.(c)sh from the `CRAB <https://twiki.cern.ch/twiki/bin/view/CMS/CRAB>`_ installation area, which have been setup either by you or by someone else for you;

* modify the `CRAB <https://twiki.cern.ch/twiki/bin/view/CMS/CRAB>`_ configuration file crab.cfg according to your need: see :ref:`basic-config` for an example;

* submit the jobs::

    $ crab submit


* check the status of all the jobs::

    $ crab status -d <task dir>


* retrieve the output of the jobs 1,2,3,5::

    $ crab get-output -d <task dir> -r 1-3,5


* see the list of good lumis for your task::

    $ crab report -d <task dir>
