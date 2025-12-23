<h1 align="center">
    SensEURCity-ETL
</h1>

Standalone ETL pipeline for [SensEURCity](https://zenodo.org/doi/10.5281/zenodo.7256405) measurements.

**Contact**: [id.hayward@caegeidwad.dev](mailto:id.hayward@caegeidwad.dev)

[![tests](https://github.com/CaderIdris/SensEURCity-InfluxDB-Upload/actions/workflows/tests.yml/badge.svg)](https://github.com/CaderIdris/SensEURCity-InfluxDB-Upload/actions/workflows/tests.yml)

---

## Table of Contents

1. [Summary](##summary)
1. [Configuration](##configuration)
1. [Installation](##how-to-install)
1. [Schema](##schema)
1. [Known Issues](##known-issues)
1. [Development](##development)
1. [Acknowledgements](##acknowledgements)

---

## Summary

A standalone Python program that performs a series of ETL operations on the open access [SensEURCity](https://zenodo.org/doi/10.5281/zenodo.7256405) dataset.

### Operations

1. (Setup) Setup database
    - Create tables (See [Schema](##schema))
1. (Setup) Download SensEURCity dataset, if not already downloaded
1. (L) Save static data to db
    - [Devices](./src/senseurcity/files/json/devices.json)
    - [Measurement headers](./src/senseurcity/files/json/headers.json)
    - [Unit conversions](./src/senseurcity/files/json/conversion.json)
1. (E) Iterate over each csv file present in dataset
    - (T) Split into measurements, reference measurements and metadata (e.g. flags)
    - (T) Determine co-location periods
    - (L) Save measurements and metadata
    - (L) Save reference measurements
    - (L) Save co-location periods

---

## Configuration

Flags can be appended to the command to configure the ETL pipeline e.g. `uv run senseurcity [OPTIONS]`.

#### Help

|Short Flag|Flag|Description|Default|
|---|---|---|---|
|`-h`|`--help`|Show help message and exit||

#### Configuration

|Short Flag|Flag|Description|Default|
|---|---|---|---|
|`-u`|`--url`|Download link to SensEURCity zip file|https://zenodo.org/records/7669644/files/senseurcity_data_v02.zip?download=1|
|`-p`|`--path`|Destination of SensEURCity zip file|`$HOME/Data/SensEURCity/senseurcity_data.zip`|
|`-d`|`--db`|Connection string for the database ([See More](#####extra))|`duckdb://$HOME/SensEURCity/SensEURCity.db`|

#### Overrides

|Short Flag|Flag|Description|Default|
|---|---|---|---|
|`-f`|`--force`|Overwrite SensEURCity zip file if it already exists|False|

#### City


|Short Flag|Flag|Description|Default|
|---|---|---|---|
|`-a`|`--antwerp`|Use Antwerp data|False|
|`-o`|`--oslo`|Use Oslo data|False|
|`-z`|`--zagreb`|Use Zagreb data|False|

*Note: If all three are false, all three are used. Otherwise, only specified cities are used*


##### Extra

###### Connection String

The connection string corresponds to a [SQLAlchemy connection string](https://docs.sqlalchemy.org/en/20/core/engines.html#database-urls).
The three supported options are:

|DB|String prefix|
|---|---|
|DuckDB|`duckdb://`|
|SQLite|`sqlite+pysqlite://`|
|PostgreSQL|`postgresql+psycopg://`|

Other DBs are unsupported but may work if you install the required packages.

---

## Installation

### Supported

This tool was developed using `uv` to manage Python versions and dependencies. The easiest way to install this tool is to clone the repository and run `uv sync` in the project root.
You can then run the tool using `uv run senseurcity [OPTIONS]`.

### Unsupported

You may be able to run this tool directly using `uvx` via the github URL e.g. `uvx --from https://github.com/CaderIdris/SensEURCity-ETL senseurcity [OPTIONS]`. However, this is untested and your mileage may vary. 

You can also set it up using the virtual environment tool built into python but the [requirements](./requirements.txt) file may not be kept up to date.

####  MacOS/Linux
```bash
> python -m venv .venv
> source .venv/bin/activate
> pip install -r requirements.txt
> senseurcity [OPTIONS]
```

#### Windows
```powershell
> python -m venv .venv
> .venv/Scripts/Activate.ps1
> pip install -r requirements.txt
> senseurcity [OPTIONS]
```

---

## Schema

#### dim_device

This table contains information relevant to each device in this study.
The information is pulled from [devices.json](./src/senseurcity/files/json/devices.json), which takes information from multiple sources including the `metadata_sites.csv` and `metadata_sampling_site_description.pdf` files.
Where information about a device was missing from both, it was either left blank or in the case of names, inferred based on the location information.
The name and short name fields for the low-cost sensors were all created for the purposes of this package and are not official names.

|Column|Type|PK|Unique|Nullable|Description|
|---|---|---|---|---|---|
|key|VARCHAR|Y|Y|N|The key used for the device in the measurements table, used to relate the two tables|
|name|VARCHAR|N|Y|N|The full name of the device|
|short_name|VARCHAR|N|Y|N|A short name for the device, to be used in places such as graph axis labels where space is a premium|
|dataset|VARCHAR|N|N|N|Which dataset the device came from, in the case of this pipeline it is always `senseurcity`|
|reference|BOOLEAN|N|N|N|Will this device be used as a reference or reference equivalent device?|
|other|JSON|N|N|Y|Any other information, stored as a json for flexibility|

#### dim_header

This table contains information relevant to each measurement header.
The information is taken from the `metadata_sensors.csv` file.

|Column|Type|PK|Unique|Nullable|Description|
|---|---|---|---|---|---|
|header|VARCHAR|Y|Y|N|The measurement header (column name in the measurement csvs)|
|parameter|VARCHAR|N|N|N|The parameter the measurement represents (i.e. NO2, T)|
|unit|VARCHAR|N|N|N|The unit of measurement (e.g. nA, %)|
|other|JSON|N|N|Y|Any other information, stored as a json for flexibility|

#### bridge_device_headers

This table contains headers corresponding to a measurement made by a device.

|Column|Type|PK|Unique|Nullable|Description|
|---|---|---|---|---|---|
|device_key|VARCHAR|Y|N|N|The device key that made the measurements|
|header|VARCHAR|Y|N|N|The header corresponding to a measurement|
|flag|VARCHAR|N|N|N|Any flag associated with the measurement|

###### Constraints

`device_key` must be a PK in the `dim_device` table.

`header` must be a PK in the `dim_header` table.

#### dim_unit_conversion

Information on how to convert from one unit of measurement to another.
May not be useful for anything more than reference, but could be used to automate standardisation of reference measurements (some are in ppb, some in μgm-3).

|Column|Type|PK|Unique|Nullable|Description|
|---|---|---|---|---|---|
|unit_in|VARCHAR|Y|N|N|The initial unit of measurement (i.e. ppb)|
|unit_out|VARCHAR|Y|N|N|The resulting unit of measurement (i.e. μgm-3)|
|parameter|VARCHAR|Y|N|N|What parameter the unit of measurement represents (i.e. NO2, O3)|
|scale|FLOAT|N|N|N|Value to multiply the measurements by for conversion|

#### fact_measurement

All measurements made by both low-cost sensors and reference devices.
The measurement columns are json to maximise flexibility, but this may come at the cost of performance.

|Column|Type|PK|Unique|Nullable|Description|
|---|---|---|---|---|---|
|time|DATETIME|Y|N|N|The time the measurement was made|
|device_key|VARCHAR|Y|N|N|The device that made the measurement|
|measurements|JSON|N|N|N|The measurements|
|flags|JSON|N|N|Y|The flags corresponding to the measurement|
|meta|JSON|N|N|Y|Any other metadata related to the measurement|

###### Constraints

`device_key` must be a PK in the `dim_device` table.

#### dim_colocation

Periods where a device was co-located with another.
In this case, only periods where a device was co-located with a reference instrument were recorded.

|Column|Type|PK|Unique|Nullable|Description|
|---|---|---|---|---|---|
|device_key|VARCHAR|Y|N|N|The co-located device|
|other_key|VARCHAR|Y|N|N|The reference device|
|start_date|DATETIME|Y|N|N|The start of the co-location|
|end_date|DATETIME|Y|N|N|The end of the co-location|

###### Constraints

`device_key` must be a PK in the `dim_device` table.

`other_key` must be a PK in the `dim_device` table.

#### meta_files_processed

Which files have already been processed?
Only useful for this tool, not for any wider analysis.

|Column|Type|PK|Unique|Nullable|Description|
|---|---|---|---|---|---|
|filename|VARCHAR|Y|Y|N|The file that has been processed|
|timestamp|DATETIME|N|N|Y|The time it was processed|

![DB Diagram](./img/dbdiagram.png)
*DB Diagram generated by DBeaver*

---

## Known Issues

#### Low-cost ozone measurements not present

This is an issue with v1 and v2 of the dataset which has been reported and should be fixed in a future v3 release.

#### Temperature and humidity measurements swapped

This is an issue with v1 and v2 of the dataset which has been reported and should be fixed in a future v3 release.

---

## Development

This code is fully unit tested, with 100% coverage.
It also undergoes a suite of static tests and linting.

Of particular note are the DB tests in [test_orm.py](./tests/test_orm.py), which test all of the unique and foreign key constraints in each table for each DB, ensuring they all behave as intended.
This is especially important for SQLite which has some weird behaviour, particularly as it doesn't strictly enforce foreign key constraints by default.
It also means that behaviour shouldn't deviate depending on which DB the user chooses to use.

---

## Acknowledgements

Massive thanks to Martine Van Poppel, Michel Gerboles and the rest of the team behind the SensEURCity project who provided this fantastic dataset as open access for anyone to use.
It very likely saved my PhD.

Paper: https://www.nature.com/articles/s41597-023-02135-w

Dataset: https://zenodo.org/records/7669644

---
