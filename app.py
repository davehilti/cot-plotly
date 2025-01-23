from dash import Dash, dash_table, dcc, callback, Output, Input
import pandas as pd
import plotly.express as px
import dash_mantine_components as dmc
from influxdb_client import InfluxDBClient
import dash_bootstrap_components as dbc

import ssl
ssl._create_default_https_context = ssl._create_unverified_context
df = pd.read_csv('https://raw.githubusercontent.com/plotly/datasets/master/gapminder2007.csv')


app = Dash()

token = "3baLLLDojDOW9jpoBOx1ejzprCzsMHPpBhFADeEZuKJToIP6h_MjU3fsCwgtBIKC9Aaz3ufBNiL-cREirFbXCQ=="

client = InfluxDBClient(url="https://eu-central-1-1.aws.cloud2.influxdata.com", token=token, org="cot-plotly")

app.layout = dbc.Container([
    dmc.Title('CoT Data Dashboard', color="blue", size="h3"),
    dmc.RadioGroup(
        [dmc.Radio(field, value=field) for field in fields],
        id='my-dmc-radio-item',
        value=fields[0] if fields else "default",
        size="sm"
    ),
    dbc.Row([
        dbc.Col([
            dash_table.DataTable(data=data.to_dict('records'), page_size=12, style_table={'overflowX': 'auto'})
        ], width=6),
        dbc.Col([
            dcc.Graph(figure={}, id='graph-placeholder')
        ], width=6),
    ]),
], fluid=True)

@callback(
    Output(component_id='graph-placeholder', component_property='figure'),
    Input(component_id='my-dmc-radio-item', component_property='value')
)
def update_graph(col_chosen):
    fig = px.histogram(df, x='continent', y=col_chosen, histfunc='avg')
    return fig

if __name__ == '__main__':
    app.run(debug=True)
