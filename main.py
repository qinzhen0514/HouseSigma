from utils import *
import csv
import requests
from requests import RequestException
import json
import time


def methodPost(url, params=None, data='', headers=None, proxy=True, redo=8, getid=True):

    if not headers:
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
    if proxy:
        proxies = getproxy()
    else:
        proxies = None
    for i in range(redo):
        try:
            t = requests.post(url, timeout=8.8, params=params, proxies=proxies, data=data, headers=headers)
            state = t.status_code
            # print(state, t.url, 'POST----')
            if state in [400, 403]:
                headers['Proxy-Switch-Ip'] = "yes"
                time.sleep(0.1)
                continue
            if state in [429, 407]:
                time.sleep(0.5)
                continue
            if 399 < state < 500:
                return None
            t.raise_for_status()
            # close connection
            t.close()
            # return t
        except RequestException as e:
            if getid:
                time.sleep(2)
                #print(e)
            else:
                time.sleep(2)
                #print(e)
                bad_id.append(str(json.loads(data)["id_listing"]))

        else:
            return t
    return None


# Get unique listing id from map search api
def get_id(year, lat1, lat2, lon1, lon2 ):

    post_url = 'https://housesigma.com/bkv2/api/search/mapsearchv2/listing2'

    headers = {
     'User-Agent': 'xxx',
     'Authorization': 'xxx',
     'Content-Type': 'xxx',
     'cookie': 'xxx'

    }

    payload_data = json.dumps({"sold_days":year,"house_type":["all"],"list_type":[3],"lat1":lat1,"lon1":lon1,"lat2":lat2,"lon2":lon2,"price":[],"zoom":25,"lang":"en_US","province":"ON"})

    resp = methodPost(post_url, headers=headers, data=payload_data, getid=True)
    result = resp.json()

    data = result['data']["list"]

    with open(f"id_list_{year}.txt", "a", encoding="utf-8") as f:
        for item in data:
            for i in item['ids']:
                f.write(str(i) + "\n")
                id_list.append(i)


# parse each listing page
def parse_id(id, writer):

    post_url = 'https://housesigma.com/bkv2/api/listing/detail/findone'

    headers = {
     'User-Agent': 'xxx',
     'Authorization': 'xxx',
     'Content-Type': 'xxx',
     'cookie': 'xxx'
    }

    payload_data = json.dumps({"id_listing": id, "lang": "en_US", "province": "ON"})

    resp = methodPost(post_url, headers=headers, data=payload_data, getid=False)
    result = resp.json()

    listing = result['data']["house"]['ml_num']
    lat = result['data']["house"]['map']['lat']
    lon = result['data']["house"]['map']['lon']
    address = result['data']["house"]['address']
    city = result['data']["house"]['municipality_name']
    property_type = result['data']["house"]['house_type_name']
    list_price = result['data']["house"]['price_int']
    sold_price = result['data']["house"]['price_sold_int']

    if result['data']["house"]['text']['rooms_long']:
        room_desp = result['data']["house"]['text']['rooms_long'].split(',')
        bedroom = room_desp[0].strip()
        bathroom = room_desp[1].strip()
        garage = room_desp[2].strip()
    else:
        bedroom = None
        bathroom = None
        garage = None
    tax = result['data']["house"]['tax_int']
    maintenance = result['data']["house"]['maintenance']
    age = result['data']["house"]['build_year']
    size = str(result['data']["house"]['house_area']['area']) + ' ' + result['data']["house"]['house_area']['unit']
    if result['data']["house"]['land']:
        lot_size = result['data']["house"]['land']['text']
    else:
        lot_size = None
    days = result['data']["house"]['list_days']
    list_date = result['data']["house"]['date_start']

    result = [listing, lat, lon, address, city, property_type, list_price, sold_price, bedroom, bathroom, garage, tax,
              maintenance, age, size, lot_size, days, list_date]
    #print(result)
    writer.writerow(result)


def main():
    threadPool = ThreadPool(10)
    years = ["Y2018", "Y2019"]

    for year in years:
        global id_list
        global bad_id
        id_list = []
        bad_id = []
        print(f'{year} Begin:   ')
        print('Step 1 Start')
        # following loops cover the majority area of Ontario
        for i in range(44000, 43500, -5):
            lat1 = i / 1000
            lat2 = (i - 5) / 1000
            for j in range(79000, 80000, 10):
                lon1 = -j / 1000
                lon2 = -(j + 10) / 1000
                threadPool.add_task(get_id, year, lat1, lat2, lon1, lon2)
        threadPool.wait_complete()
        print('Step 1 Finished')

        id_list = list(set(id_list))
        print(f'URL Number:  {len(id_list)}')

        print('Step 2 Start')
        headers = ['Listing', 'Lat', 'Lon', 'Address', 'City', 'Type', 'List_Price', 'Sold_Price', 'Bedroom', 'Bathroom',
                   'Garage', 'Tax', 'Maintenance', 'Age', 'Size', 'Lot_Size', 'Days', 'List_Date']
        with open(f"House_{year}.csv", 'w', encoding='utf-8', newline='') as ff:
            writer = csv.writer(ff)
            writer.writerow(headers)
            for index, id in enumerate(id_list):
                if index % 1000 == 0:
                    print(f'{index} Finished!')
                threadPool.add_task(parse_id, id, writer)
            threadPool.wait_complete()

        print(f'Number of bad url: {len(list(set(bad_id)))}')
        with open(f"bad_id_{year}.txt", "a", encoding="utf-8") as f:
            for id in list(set(bad_id)):
                f.write(str(id) + "\n")


if __name__ == '__main__':
    start = time.time()
    main()
    print(time.time()-start)