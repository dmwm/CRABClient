Introduction
===============
CRAB is an utility to create and submit CMSSW jobs to distributed computing resources.

This twiki aims to help users already confident with `CRAB <https://twiki.cern.ch/twiki/bin/view/CMS/CRAB>`_ submission tool to be able to access CMS distributed data through the next version of the tool, CRAB-3. This major version change of the tool has a relevant impact on user routines for job submission and job tracking since the whole architecture of the tool is changed, including the user interface.

Important information
+++++++++++++++++++++

* The CRAB-3 configuration is Python based; a script to translate old crab configuration to new one is available (:ref:`convert-config`). In this section you can find a description on how to create it manually with an example.

* Client commands does not require anymore the ``-`` in front; as example ``crab -submit`` becomes ``crab submit``.

* The ``crab (-)create`` command is not anymore available because the creation part is moved at server level; the workflow starts directly from the submission command (:ref:`submission`).

* The ``--continue/-c`` is now replaced by ``--dir/-d`` option.

* All outputs and logs are stored on a remote SE by default: ``get-output`` is no longer needed except to check your data.

* Direct submission is not anymore supported.
