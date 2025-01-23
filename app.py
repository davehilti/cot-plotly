from influxdb_client import InfluxDBClient
import boto3
import json
import pandas as pd
import dash_mantine_components as dmc
import dash_bootstrap_components as dbc
from dash import Dash, Input, Output, dash_table, dcc, html, callback
import plotly.express as px


# Funktion zum Abrufen der InfluxDB-Zugangsdaten
def get_influxdb_credentials():
    secret_name = "InfluxDB_Credentials"
    region_name = "eu-north-1"

    client = boto3.client("secretsmanager", region_name=region_name)
    response = client.get_secret_value(SecretId=secret_name)
    secret_data = json.loads(response["SecretString"])

    return secret_data["influx_url"], secret_data["influx_token"]

# Verbindung zur InfluxDB einrichten
influx_url, influx_token = get_influxdb_credentials()
client = InfluxDBClient(url=influx_url, token=influx_token, org="cot-plotly")


def fetch_influxdb_data():
    # Adjusted query to fetch data for the last 10 years
    query = 'from(bucket: "CoT-Data") |> range(start: -10y) |> filter(fn: (r) => r._measurement == "cot_data")'
    result = client.query_api().query(org="cot-plotly", query=query)
    
    # Convert results to Pandas DataFrame
    records = []
    for table in result:
        for record in table.records:
            records.append({
                "time": record["_time"],
                "value": record["_value"],
                "field": record["_field"]
            })
    return pd.DataFrame(records)


# Daten von InfluxDB abrufen
try:
    data = fetch_influxdb_data()
    fields = data["field"].unique().tolist() if "field" in data.columns else []
except Exception as e:
    print(f"Fehler beim Abrufen der Daten: {e}")
    data = pd.DataFrame()  # Leeres DataFrame
    fields = []

# Dash-App initialisieren
app = Dash(__name__)

# Dash-Layout aktualisieren, um InfluxDB-Daten zu verwenden
app.layout = dmc.Container([
    dmc.Title('CoT Data Dashboard', size="h3"),
    dmc.RadioGroup(
        [dmc.Radio(field, value=field) for field in fields],
        id='my-dmc-radio-item',
        value=fields[0] if fields else None,
        size="sm"
    ) if fields else dmc.Text("Keine Daten verfügbar."),
    html.Div([
        html.Div([
            dash_table.DataTable(
                data=data.to_dict('records'),
                page_size=12,
                style_table={'overflowX': 'auto'}
            )
        ], style={"width": "48%", "display": "inline-block"}) if not data.empty else dmc.Text("Keine Tabellendaten verfügbar."),
        html.Div([
            dcc.Graph(figure={}, id='graph-placeholder')
        ], style={"width": "48%", "display": "inline-block"})   # Zweite Spalte
    ])
], fluid=True)

# Callback zur Aktualisierung der Grafik
@app.callback(
    Output(component_id='graph-placeholder', component_property='figure'),
    Input(component_id='my-dmc-radio-item', component_property='value')
)
def update_graph(field_chosen):
    if field_chosen and not data.empty:
        filtered_data = data[data["field"] == field_chosen]
        fig = px.histogram(filtered_data, x="time", y="value", title=f"Data for {field_chosen}")
    else:
        fig = px.histogram(title="Keine Daten verfügbar.")
    return fig

# Dash-App starten
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8050)
