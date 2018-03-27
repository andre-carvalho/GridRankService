# rankservice
The purpose of this service is sort cells from a specific input grid based on percent of cloud cover, an external kernel statistics and the last date of cell interpretation.

## The input and output data
The input data is the cloud and the grid cells from the postgis tables.
We have two outputs. First output is the cloud polygons after clipping by the intercept against the grid cells and scene cell. Another output is the ranking value to cells. Both outputs are stored in postgis tables.

## The configuration
The configurations is stored in the files at src/config directory. It's two files, one to database connection parameters and another to adjust the prefixes and sufixes of input and output tables and intermediary tables too.

## The code
This service is written in Python 3 and its dependencies is defined in requirements.txt file at config directory.

## Run in command line in terminal or cron
We have two entry points to run this service, start_single_thread.py and start_multi_thread.py

# Run as a HTTP service
This code are incomplete but its started using Flask.