#!/usr/bin/env python
#
# =============================================================================
#
# Downloads the data fron given url and 
# saves the data in a file inside downloads folder
#
# =============================================================================

import urllib.request
import time

# Initialize variables
url = 'http://rbi.ddns.net/getBreadCrumbData'
folder_name = "./downloads"
download_date = time.strftime("%Y%m%d")
file_format = ".json"
file_name = folder_name + "/" + download_date + file_format

# Download data and save it to a file
# As a convention, the files are saved as yearmonthdate.json
# Example: 20210122.json
with urllib.request.urlopen(url) as response, open(file_name, 'wb') as out_file:
    data = response.read()  # a `bytes` object
    out_file.write(data)
