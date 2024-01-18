# DEVELOPER : Ali Anwar
# FILE      : app.py


from flask import Flask, render_template, request
from functions.fetch_df_from_s3 import fetch_df_from_s3
from functions.map_generator import display_map
from functions.table_view_generator import table_view_generator
from markupsafe import Markup


app = Flask(__name__)


# FUNCTION : about_project
# PURPOSE  : To display /about-project webpage.
@app.route('/')
def about_project():
    svg = open('static/aws-architecture.svg').read()
    return render_template('about-project.html',
                            svg = Markup(svg))



# FUNCTION : interactive_map
# PURPOSE  : To display /about-project webpage.
@app.route('/interactive-map', methods=['GET', 'POST'])
def interactive_map():

    data = fetch_df_from_s3()
    df = data['df']
    address_locality = sorted(list(df['address_locality'].unique()))

    # locality metrics
    selected_city = request.form.get('address_locality')
    if selected_city == None:
        selected_city = address_locality[0]

    total_results = len(df[df['address_locality'] == selected_city])
    highest_price = str(round(df[ df['address_locality'] == selected_city]['price'].max(), 2))
    lowest_price  = str(round(df[(df['address_locality'] == selected_city) & (df['price'] > 100000)]['price'].min(), 2))
    average_price = str(round(df[ df['address_locality'] == selected_city]['price'].mean(), 2))
    map = display_map(df, address_locality[0], selected_city)

    return render_template('interactive-map.html',
                            refresh_date = data['refresh_date'],
                            address_locality = address_locality,
                            selected_city = selected_city,
                            total_results = total_results,
                            highest_price = highest_price,
                            lowest_price = lowest_price,
                            average_price = average_price,
                            map=map._repr_html_())



# FUNCTION : table_view
# PURPOSE  : To display /table-view webpage.
@app.route('/table-view', methods=['GET', 'POST'])
def table_view():
    data = fetch_df_from_s3()
    df = data['df']

    address_locality = sorted(list(df['address_locality'].unique()))

    selected_city = request.form.get('address_locality')
    if selected_city is None:
        selected_city = address_locality[0]

    total_results = len(df[df['address_locality'] == selected_city])
    highest_price = str(round(df[df['address_locality'] == selected_city]['price'].max(), 2))
    lowest_price = str(round(df[(df['address_locality'] == selected_city) & (df['price'] > 100000)]['price'].min(), 2))
    average_price = str(round(df[df['address_locality'] == selected_city]['price'].mean(), 2))
    df_generated = table_view_generator(df, selected_city)

    return render_template('table-view.html',
                            refresh_date=data['refresh_date'],
                            address_locality=address_locality,
                            selected_city=selected_city,
                            total_results=total_results,
                            highest_price=highest_price,
                            lowest_price=lowest_price,
                            average_price=average_price,
                            df=df_generated)



# FUNCTION : __main__
# PURPOSE  : To run app.py
if __name__ == '__main__':
    app.run()