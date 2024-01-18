# DEVELOPER : Ali Anwar
# FILE      : map_generator.py

import folium


# FUNCTION  : fetch_df_from_s3
# PURPOSE   : To get the latest file from s3 bucket.
def display_map(df, first_city, selected_city):

    selected_city_df = ''
    if selected_city == None:
        selected_city_df = df[df['address_locality'] == first_city]
    else:
        selected_city_df = df[df['address_locality'] == selected_city]

    ontario_map = folium.Map(location=[43.651070, -79.347015],
                            zoom_start=4,
                            tiles="OpenStreetMap",
                            max_zoom=3,
                            min_zoom=6)

    for index, row in selected_city_df.iterrows():
        content = f"""Address:{row['street_address']}
                    <br>Postal Code: {row['postal_code']}
                    <br>Price: ${row['price']}
                    <br>Listing URL: <a href={row['listing_url']} target="_blank">{row['listing_url']}</a>"""
        marker_popup = folium.Popup(content)

        folium.Marker(location=[row['latitude'], row['longitude']],
                        popup=marker_popup,
                        icon=folium.Icon(color='red')).add_to(ontario_map)

    return ontario_map