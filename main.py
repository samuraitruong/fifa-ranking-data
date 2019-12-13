import requests
from pyquery import PyQuery as pq
import concurrent.futures
import json
from datetime import datetime
import pandas as pd

def get_ranking_schedule(url):
    response = requests.get(url);
    html = response.text
    query = pq(html);
    links = ["https://www.fifa.com" + query(x).attr("href") for x in query(".fi-ranking-schedule__nav__item a")];
    print(links)
    return links;
def convert_number(str):
    if str == '':
         return None
    try:
        return int(str)
    except expression as err:
        return None

def fetch_page(url):
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
            "points": convert_number(query(".fi-table__points", row).text()),
            "previousPoints": convert_number(query(".fi-table__prevpoints", row).text()),
            "flag": query(".fi-t__i img", row).attr("src"),
        }
        if item["points"] and item["previousPoints"]:
            item["delta"] = item["points"] - item["previousPoints"]
        items.append(item)
    # print(html)
    return items;
def write_json(data, filename):
    with open(filename, "w+") as f:
        f.seek(0);
        f.truncate();
        json.dump(data, f, indent=4)
    
def output(data):
    filename = data[0]["date"]
    write_json(data, f'data/json/group-by-published/{filename}.json')
    df = pd.DataFrame(data)
    df.to_csv(f'data/csv/group-by-published/{filename}.csv')
    
def main():
    #fetch_page("https://www.fifa.com/fifa-world-ranking/ranking-table/men/rank/id103/");
    #return;
    urls = get_ranking_schedule("https://www.fifa.com/fifa-world-ranking/ranking-table/men/");
    # fetch_page("https://www.fifa.com/fifa-world-ranking/ranking-table/men/")
    # We can use a with statement to ensure threads are cleaned up promptly
    fullData =[]
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        # Start the load operations and mark each future with its URL
        jobs = {executor.submit(fetch_page, url): url for url in urls}
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
    write_json(fullData, "data/json/all.json")
    df = pd.DataFrame(fullData)
    df.to_csv(f'data/csv/all.csv')
def download_flags(ct: str):
    print("download flag: " + ct)
    url = 'https://api.fifa.com/api/v1/picture/flags-sq-4/' + ct.lower()
    r = requests.get(url, allow_redirects=True)
    print("Download file : " + url)
    with open(f'images/png/{ct.lower()}.png', 'wb')as file:
        file.write(r.content);

    code = ct[:2]
    url = f"https://media.api-football.com/flags/{code.lower()}.svg"

    r = requests.get(url, allow_redirects=True)
    print("Download file : " + url)
    with open(f'images/svg/{ct.lower()}.svg', 'wb')as file:
        file.write(r.content);

    return "OK"

def generate_metadata():

    pages = fetch_page("https://www.fifa.com/fifa-world-ranking/ranking-table/men");
    counntries = [{ "name": item["country"],
                    "shortName": item["ct"]
                }  for item in pages];

    counntries = sorted(counntries, key = lambda x: x["name"]);
    write_json(counntries , "data/json/countries.json")
    pd.DataFrame(counntries).to_csv(f'data/csv/countries.csv')

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        # Start the load operations and mark each future with its URL
        jobs = {executor.submit(download_flags, c["shortName"]) : c for c in counntries}
        for future in concurrent.futures.as_completed(jobs):
            try:
                data = future.result();
            except Exception as exc:
                print('Error %' %(exc) )

    return;
# main()
generate_metadata();
