# import json
# import csv

# with open('scams.json') as jfile:
#     data = json.load(jfile)

# with open('scams.csv', 'w', newline='') as csvfile:
#     fieldnames = ['identity', 'name', 'url', 'coin', 'category', 'subcategory', 'index', 'status']
#     writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

#     writer.writeheader()

#     for address in data['result']:
#         for i in data['result'][address]['addresses']:
#             identity = str(data['result'][address]['id'])
#             name = str(data['result'][address]['name'])
#             url = str(data['result'][address]['url'])
#             coin = str(data['result'][address]['coin'])
#             category = 'Scamming' if data['result'][address]['category'] == 'Scam' else str(data['result'][address]['category'])
#             subcategory = str(data['result'][address].get('subcategory', ''))
#             index = str(i)
#             status = str(data['result'][address]['status'])

#         writer.writerow({'identity': identity, 'name': name, 'url': url, 'coin': coin, 'category': category, 
#                             'subcategory': subcategory, 'index': index, 'status': status})


import json
import csv

# Define constants
FIELDNAMES = ['identity', 'name', 'url', 'coin', 'category', 'subcategory', 'index', 'status']
CATEGORY_MAP = {'Scam': 'Scamming'}

# Load JSON data
with open('scams.json') as jfile:
    data = json.load(jfile)

# Write CSV file
with open('scams.csv', 'w', newline='') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=FIELDNAMES)
    writer.writeheader()

    # Loop over addresses in JSON data
    for address_data in data['result']:
        address_info = data['result'][address_data]

        # Extract common fields from address info
        identity = str(address_info['id'])
        name = str(address_info['name'])
        url = str(address_info['url'])
        coin = str(address_info['coin'])
        category = CATEGORY_MAP.get(address_info['category'], address_info['category'])
        subcategory = str(address_info.get('subcategory', ''))

        # Loop over addresses for this address info
        for i, address in enumerate(address_info['addresses']):
            index = str(i)
            status = str(address_info['status'])

            # Write row to CSV
            writer.writerow({'identity': identity, 'name': name, 'url': url, 'coin': coin, 'category': category, 
                            'subcategory': subcategory, 'index': index, 'status': status})
