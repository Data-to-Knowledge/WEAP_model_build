WEAP model build
===========================================

This repository contains Python scripts that enables automatic WEAP model building for river catchments in Canterbury, New Zealand. These scripts aim
to make model building less time-consuming, and consistent among Canterbury's river catchments.

The scripts were used to build a Water Allocation Model for the Rakaia River Catchment in Canterbury, New Zealand.

The scripts rely on data that exists in the databases of Environment Canterbury. A change to the structure of any of these databases means that some of
the scripts needs to be modified to be able to work with these databases changes.

About WEAP
-----------
WEAP ("Water Evaluation And Planning" system) is a user-friendly software tool that takes an integrated approach to water resources planning.
Allocation of limited water resources between agricultural, municipal and environmental uses now requires the full integration of supply, 
demand, water quality and ecological considerations. WEAP aims to incorporate these issues into a practical yet robust tool for integrated water resources planning.
WEAP is developed by the Stockholm Environment Institute's U.S. Center.

More info about WEAP can be found `here <http://www.weap21.org/>`_.

Usage
------
As a user you have to edit the ``config.cfg`` file (the configuration file) to make it work for your catchment of interest. You can choose to build the entire model from start
to end by running all the sections (denoted in square brackets) in the configuration file, or you can choose to run it section by section. Some sections in the
configuration file may be irrelevant for your area of interest, which can then be turned off so they are not used when building the WEAP model.

Once the settings in the configuration file have been set, the main script can be executed by::

    python WEAP.py

More help regarding controlling the WEAP API using scripting can be found `here <http://www.weap21.org/WebHelp/API.htm>`_.


Copyright
---------
Copyright (C) 2020 Wilco Terink. The WEAP_model_build is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General Public License along with this program. If not, see `http://www.gnu.org/licenses/ <http://www.gnu.org/licenses/>`__.
