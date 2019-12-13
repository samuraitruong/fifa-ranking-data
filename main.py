import requests
from pyquery import PyQuery as pq
import concurrent.futures
import json
from datetime import datetime
import pandas as pd

def getRankingSchedule(url):
    response = requests.get(url);
    html = response.text
    query = pq(html);
    links = ["https://www.fifa.com" + query(x).attr("href") for x in query(".fi-ranking-schedule__nav__item a")];
    print(links)
    return links;
def convertNumber(str):
    if str == '':
         return None
    try:
        return int(str)
    except expression as err:
        return None

def fetchOnePage(url):
    response = requests.get(url);
    html = response.text
    query = pq(html);
    rows = query("#rank-table tbody tr");
    datestr = query(".fi-selected-item").text().strip()
    date = datetime.strptime(datestr, "%d %B %Y")
    items = []
    for row in rows:
        item = {
            "date":  date.isoformat(),
            "rank": query(".fi-table__rank", row).text(),
            "country": query(".fi-t__nText ", row).text(),
            "ct": query(".fi-t__nTri", row).text(),
            "points": convertNumber(query(".fi-table__points", row).text()),
            "previousPoints": convertNumber(query(".fi-table__prevpoints", row).text()),
            "flag": query(".fi-t__i img", row).attr("src"),
        }
        if item["points"] and item["previousPoints"]:
            item["delta"] = item["points"] - item["previousPoints"]
        items.append(item)
    # print(html)
    return items;
def writeJson(data, filename):
    with open(filename, "w+") as f:
        f.seek(0);
        f.truncate();
        json.dump(data, f, indent=4)
    
def output(data):
    filename = data[0]["date"]
    writeJson(data, f'data/group-by-published/json/{filename}.json')
    df = pd.DataFrame(data)
    df.to_csv(f'data/group-by-published/csv/{filename}.csv')

def main():
    #fetchOnePage("https://www.fifa.com/fifa-world-ranking/ranking-table/men/rank/id103/");
    #return;
    urls = getRankingSchedule("https://www.fifa.com/fifa-world-ranking/ranking-table/men/");
    # fetchOnePage("https://www.fifa.com/fifa-world-ranking/ranking-table/men/")
    # We can use a with statement to ensure threads are cleaned up promptly
    fullData =[]
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        # Start the load operations and mark each future with its URL
        jobs = {executor.submit(fetchOnePage, url): url for url in urls}
        for future in concurrent.futures.as_completed(jobs):
            url = jobs[future]
            try:
                data = future.result();
                fullData = fullData + data
                output(data);
            except Exception as exc:
                print('%r Error loading page : %s' % (url, exc))
            else:
                print( "Finished:  %s : %d" % (url, len(data)));
    writeJson(fullData, "data/data.json") 
main()