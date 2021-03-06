import pandas as pd
from urllib.request import urlopen
from bs4 import BeautifulSoup, Tag

def getStops(url):
    # url = "http://34.83.136.192:8000/getStopEvents/"
    # url = "http://localhost:8000"
    html = urlopen(url)

    soup = BeautifulSoup(html, 'html.parser')

    # Get the title
    all_links = soup.find_all("h3")

    res = []
    for link in all_links:
        splitted_title = link.get_text().split()
        trip_id = splitted_title[4]
        table = link.find_next("table")
        table_rows = table.find_all('tr')
        tr = table_rows[1]
        td = tr.find_all('td')
        row = [tr.text for tr in td]
        if row:
            direction = 'Back' if row[4] == 1 else 'Out'
            serviceKey = 'Weekday' if row[5] != 'S' else 'Saturday'
            res.append([trip_id, row[0], row[3], direction, serviceKey])

    columnNames = ["trip_id", "vehicle_number", "route_number", "direction", "service_key"]
    df = pd.DataFrame(res, columns=columnNames)
    print(df)
    return res
