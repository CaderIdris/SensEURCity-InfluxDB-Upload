<h1 align="center">
    SensEURCity-InfluxDB-Upload
</h1>

**Contact**: [CaderIdrisGH@outlook.com](mailto:CaderIdrisGH@outlook.com)

---

## Table of Contents

1. [Summary](##summary)
1. [Main Features](##main-features)
1. [How to Install](##how-to-install)
1. [Dependencies](##dependencies)
1. [Example Usage](##example-usage)
1. [Acknowledgements](##acknowledgements)

---

## Summary

Uploads [SensEURCity](https://zenodo.org/doi/10.5281/zenodo.7256405) data to an InfluxDB 2.x database

---

## Main Features

- Parse csv files from SensEURCity study
- Upload files to InfluxDB database

---

## How to install

`pip install git+https://github.com/CaderIdris/SensEURCity-InfluxDB-Upload/`

---

## Dependencies

Please see [Pipfile](./Pipfile).

---

## Example Usage

```bash

path/to/python_venv/pip install https://github.com/CaderIdris/SensEURCity-InfluxDB-Upload/

/path/to/python_venv/senseurcity -i /path/to/influx.json -d /path/to/senseurcity_folder/dataset/


```


---

## Acknowledgements

Many thanks to James Murphy at [Mcoding](https://mcoding.io) who's excellent tutorial [Automated Testing in Python](https://www.youtube.com/watch?v=DhUpxWjOhME) and [associated repository](https://github.com/mCodingLLC/SlapThatLikeButton-TestingStarterProject) helped a lot when structuring this package
