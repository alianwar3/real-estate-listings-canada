# DEVELOPER : Ali Anwar
# FILE      : table_view_generator.py

import pandas as pd


# FUNCTION  : table_view_generator
# PURPOSE   : To generate HTML hyperlinks for listing urls and organize
#             df to display in table view.
def table_view_generator(df, selected_city) -> pd.DataFrame:

    df['rendered_links'] = [f'<a href="{url}" target="_blank">{url}</a>' for url in df['listing_url']]
    df = df.drop(columns=['timestamp', 'listing_url'])
    df = df[df['address_locality'] == selected_city]

    return df

