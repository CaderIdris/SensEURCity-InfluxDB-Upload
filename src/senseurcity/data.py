"""Download, format and store SensEURCity measurements.

Parse measurement data from the SensEURCity project [^article],
Format them and upload to a SQL database.
The dataset can be downloaded from Zenodo [^zenodo]

[^article]: https://www.nature.com/articles/s41597-023-02135-w
[^zenodo]: https://zenodo.org/doi/10.5281/zenodo.7256405
"""

import logging

logger = logging.getLogger(f'__main__.{__name__}')
