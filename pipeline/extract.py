# DEVELOPER : Ali Anwar
# FILE      : extract.py

import boto3
import pandas as pd
import json
import requests
from bs4 import BeautifulSoup
from datetime import datetime



# FUNCTION : lambda_handler
# PURPOSE  : To extract data from Remax Ontario listings.
def lambda_handler(event, context):

    real_estate = {
        'street_address': [],
        'address_locality': [],
        'address_region': [],
        'postal_code': [],
        'latitude': [],
        'longitude': [],
        'price': [],
        'listing_url': []
    }

    ontario_cities = [
    "Ajax", "Aurora", "Barrie", "Brampton", "Brantford", "Burlington",
    "Cambridge", "Guelph", "Hamilton", "Kingston", "Kitchener",
    "London", "Markham", "Milton", "Mississauga", "Newmarket",
    "Niagara Falls", "Oakville", "Ottawa", "Oshawa", "Pickering",
    "Richmond Hill", "Sarnia", "St. Catharines", "Sudbury", "Thunder Bay",
    "Toronto", "Vaughan", "Waterloo", "Whitby", "Windsor"]

    for city in ontario_cities:
        for i in range(1, 21):

            # collect html data
            real_estate_listings_page = requests.get(f'https://www.remax.ca/on/{city}-real-estate?pageNumber={i}')
            soup = BeautifulSoup(real_estate_listings_page.content, "html.parser")

            # extract information from each script tag
            script_tags = list(soup.find_all('script', {'type': 'application/ld+json'}))
            for script in script_tags[2:]:
                script_data = str(script).replace("""<script data-next-head="true" type="application/ld+json">{"@context":"https://schema.org","@graph":""", "").replace("}</script>", "")
                json_data = json.loads(script_data)

                real_estate['street_address'].append(json_data[0]['address'].get('streetAddress', ''))
                real_estate['address_locality'].append(json_data[0]['address'].get('addressLocality', ''))
                real_estate['address_region'].append(json_data[0]['address'].get('addressRegion', ''))
                real_estate['postal_code'].append(json_data[0]['address'].get('postalCode', ''))
                real_estate['latitude'].append(json_data[0]['geo'].get('latitude', ''))
                real_estate['longitude'].append(json_data[0]['geo'].get('longitude', ''))
                real_estate['price'].append(json_data[2]['offers'].get('price', ''))
                real_estate['listing_url'].append("https://www.remax.ca/" + json_data[2]['offers'].get('url', ''))


    # create a dataframe for the current city
    df = pd.DataFrame(real_estate)
    df['street_address'] = df['street_address'].str.replace(',', '')

    # find length of parameters
    print('street_address:', len(real_estate['street_address']))
    print('address_locality:', len(real_estate['address_locality']))
    print('address_region:', len(real_estate['address_region']))
    print('postal_code:', len(real_estate['postal_code']))
    print('latitude:', len(real_estate['latitude']))
    print('longitude:', len(real_estate['longitude']))
    print('listing_url:', len(real_estate['listing_url']))
    print('price:', len(real_estate['price']))

    # get the current date and time
    current_datetime = datetime.now()
    formatted_datetime = current_datetime.strftime("%Y-%m-%d-%H-%M-%S")

    # write to csv file
    file = f'real-estate-listings-{formatted_datetime}.csv'
    listings_data = df.to_csv(index=False)

    # write file to s3
    s3 = boto3.client('s3')

    s3.put_object(
        Bucket='real-estate-canada-etl-project',
        Key= 'raw-data/' + file,
        Body=listings_data.encode('utf-8')
    )