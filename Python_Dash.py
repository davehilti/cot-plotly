import dash
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output
import pandas as pd
from influxdb_client import InfluxDBClient
import plotly.graph_objs as go
import webbrowser
from threading import Timer
from datetime import datetime as dt, timedelta
import dash_bootstrap_components as dbc
import numpy as np

# Function to open the web browser
def open_browser():
    webbrowser.open_new("http://127.0.0.1:8051/")

# Connect to InfluxDB
url = "http://localhost:8086"
token = "D7cFFZ7LzmMJn0qO-85EYhjRfhKqydGLSE-gkoR-e1RLivHN_Ud_mj_r5s4kGXKHtUg-n0xI7I3L0-6Uv8ZVhw=="
org = "Student"
bucket = "CoT-Data"

client = InfluxDBClient(url=url, token=token, org=org)

# Define query to fetch data
query = """
from(bucket: "CoT-Data")
  |> range(start: -10y)
  |> filter(fn: (r) => r["_measurement"] == "cot_data")
"""

# Execute query and process result
result = client.query_api().query(org=org, query=query)
records = []
for table in result:
    for record in table.records:
        records.append(record.values)

df = pd.DataFrame(records)

# Close the client
client.close()

# Pivot the dataframe
df_pivoted = df.pivot_table(values='_value', index=['_time', 'market_names'], columns='_field').reset_index()

# Rename columns for convenience
df_pivoted.columns.name = None
df_pivoted.rename(columns={'_time': 'Date', 'market_names': 'Market Names'}, inplace=True)

# Berechnung der Spalte 'Total Number of Traders'
df_pivoted['Total Number of Traders'] = df_pivoted[['Traders Prod/Merc Short', 'Traders Swap Long', 'Traders M Money Long']].sum(axis=1)

# Berechnung von MML (S) T% für Long und Short
df_pivoted['MML_Long_T_Percent'] = df_pivoted['Managed Money Long'] / df_pivoted['Total Number of Traders']
df_pivoted['MML_Short_T_Percent'] = df_pivoted['Managed Money Short'] / df_pivoted['Total Number of Traders']

# Berechnung der Clustering Range für Long Positionen
df_pivoted['Long Clustering'] = (
    (df_pivoted['MML_Long_T_Percent'] - df_pivoted['MML_Long_T_Percent'].rolling(365, min_periods=1).min())
    / (df_pivoted['MML_Long_T_Percent'].rolling(365, min_periods=1).max() - df_pivoted['MML_Long_T_Percent'].rolling(365, min_periods=1).min())
) * 100

# Berechnung der Clustering Range für Short Positionen
df_pivoted['Short Clustering'] = (
    (df_pivoted['MML_Short_T_Percent'] - df_pivoted['MML_Short_T_Percent'].rolling(365, min_periods=1).min())
    / (df_pivoted['MML_Short_T_Percent'].rolling(365, min_periods=1).max() - df_pivoted['MML_Short_T_Percent'].rolling(365, min_periods=1).min())
) * 100



df_pivoted['Rolling Min'] = df_pivoted['Producer/Merchant/Processor/User Long'].rolling(365, min_periods=1).min()
df_pivoted['Rolling Max'] = df_pivoted['Producer/Merchant/Processor/User Long'].rolling(365, min_periods=1).max()

# Assuming there's a column for the number of traders
df_pivoted['Total Number of Traders'] = df_pivoted[['Traders Prod/Merc Short', 'Traders Swap Long', 'Traders M Money Long']].sum(axis=1)

# Define size categories for traders
df_pivoted['Trader Size'] = pd.cut(
    df_pivoted['Total Number of Traders'],
    bins=[0, 50, 100, 150],
    labels=['≤ 50 Traders', '51–100 Traders', '101–150 Traders']
)

print(df.columns)  # Zeigt alle Spalten in df
print(df.head())   # Zeigt die ersten Zeilen


# Additional calculations for the new graphs
df_pivoted['Total Long Traders'] = df_pivoted[['Traders Prod/Merc Short', 'Traders Swap Long', 'Traders M Money Long']].sum(axis=1)
df_pivoted['Total Short Traders'] = df_pivoted[['Traders Prod/Merc Short', 'Traders Swap Short', 'Traders M Money Short']].sum(axis=1)
df_pivoted['Long Position Size'] = df_pivoted['Producer/Merchant/Processor/User Long']
df_pivoted['Short Position Size'] = df_pivoted['Producer/Merchant/Processor/User Short']
df_pivoted['Net Short Position Size'] = (
    df_pivoted['Short Position Size'] - df_pivoted['Long Position Size']
)

df_pivoted['MML Long OI'] = df_pivoted['Managed Money Long']
df_pivoted['MML Short OI'] = -df_pivoted['Managed Money Short']
df_pivoted['MMS Long OI'] = df_pivoted['Managed Money Long']
df_pivoted['MMS Short OI'] = -df_pivoted['Managed Money Short']
df_pivoted['MML Traders'] = df_pivoted['Traders M Money Long']
df_pivoted['MMS Traders'] = df_pivoted['Traders M Money Short']

df_pivoted['MML Position Size'] = df_pivoted['Managed Money Long'] / df_pivoted['Traders M Money Long']
df_pivoted['MMS Position Size'] = df_pivoted['Managed Money Short'] / df_pivoted['Traders M Money Short']

max_bubble_size = 100
max_oi = max(df_pivoted['MML Long OI'].max(), abs(df_pivoted['MML Short OI'].max()))
max_oi = max(df_pivoted['MMS Short OI'].max(), abs(df_pivoted['MML Short OI'].max()))

sizeref = 2. * max_oi / (max_bubble_size**3.2)

# Calculate relative concentration for each trader group
df_pivoted['PMPUL Relative Concentration'] = df_pivoted['Producer/Merchant/Processor/User Long'] - df_pivoted['Producer/Merchant/Processor/User Short']
df_pivoted['PMPUS Relative Concentration'] = df_pivoted['Producer/Merchant/Processor/User Short'] - df_pivoted['Producer/Merchant/Processor/User Long']
df_pivoted['SDL Relative Concentration'] = df_pivoted['Swap Dealer Long'] - df_pivoted['Swap Dealer Short']
df_pivoted['SDS Relative Concentration'] = df_pivoted['Swap Dealer Short'] - df_pivoted['Swap Dealer Long']
df_pivoted['MML Relative Concentration'] = df_pivoted['Managed Money Long'] - df_pivoted['Managed Money Short']
df_pivoted['MMS Relative Concentration'] = df_pivoted['Managed Money Short'] - df_pivoted['Managed Money Long']
df_pivoted['ORL Relative Concentration'] = df_pivoted['Other Reportables Long'] - df_pivoted['Other Reportables Short']
df_pivoted['ORS Relative Concentration'] = df_pivoted['Other Reportables Short'] - df_pivoted['Other Reportables Long']

# Columns for the number of traders for each group
df_pivoted['PMPUL Traders'] = df_pivoted['Traders Prod/Merc Long']
df_pivoted['PMPUS Traders'] = df_pivoted['Traders Prod/Merc Short']
df_pivoted['SDL Traders'] = df_pivoted['Traders Swap Long']
df_pivoted['SDS Traders'] = df_pivoted['Traders Swap Short']
df_pivoted['MML Traders'] = df_pivoted['Traders M Money Long']
df_pivoted['MMS Traders'] = df_pivoted['Traders M Money Short']
df_pivoted['ORL Traders'] = df_pivoted['Traders Other Rept Long']
df_pivoted['ORS Traders'] = df_pivoted['Traders Other Rept Short']

# Determine the quarter for each date
df_pivoted['Quarter'] = df_pivoted['Date'].dt.quarter.map({1: 'Q1', 2: 'Q2', 3: 'Q3', 4: 'Q4'})

# Calculate a global sizeref to ensure consistency across markets
max_bubble_size = 100  # Adjusted for better visualization
max_oi = max(df_pivoted[['PMPUL Relative Concentration', 'PMPUS Relative Concentration', 
                         'SDL Relative Concentration', 'SDS Relative Concentration', 
                         'MML Relative Concentration', 'MMS Relative Concentration', 
                         'ORL Relative Concentration', 'ORS Relative Concentration']].max().max(),
             abs(df_pivoted[['PMPUL Relative Concentration', 'PMPUS Relative Concentration', 
                             'SDL Relative Concentration', 'SDS Relative Concentration', 
                             'MML Relative Concentration', 'MMS Relative Concentration', 
                             'ORL Relative Concentration', 'ORS Relative Concentration']].min().min()))
sizeref = 2. * max_oi / (max_bubble_size**2.5)

min_bubble_size = 10  # Set minimum bubble size

# Add Year column for color coding
df_pivoted['Year'] = df_pivoted['Date'].dt.year

# Calculate Net OI for Managed Money (MM)
df_pivoted['MM Net OI'] = df_pivoted['Managed Money Long'] - df_pivoted['Managed Money Short']

# Calculate Net Number of Traders for MM
df_pivoted['MM Net Traders'] = df_pivoted['Traders M Money Long'] - df_pivoted['Traders M Money Short']

# Define the default end date (most recent date)
default_end_date = df_pivoted['Date'].max()

# Define the default start date (6 months prior to the end date)
default_start_date = default_end_date - timedelta(days=182)



def get_global_xaxis():
    return dict(
        tickmode='array',
        tickvals=df_pivoted['Date'].dt.year.unique(),
        ticktext=[str(year) for year in df_pivoted['Date'].dt.year.unique()],
        showgrid=True,
        ticks="outside",
        tickangle=45
    )

global_xaxis = dict(
    tickmode='array',
    tickvals=df_pivoted['Date'].dt.year.unique(),  # Unique years
    ticktext=[str(year) for year in df_pivoted['Date'].dt.year.unique()],  # Format as strings
    showgrid=True,
    ticks="outside",
    tickangle=45  # Rotate for better visibility
)

def add_last_point_highlight(fig, df, x_col, y_col, inner_size=10, outer_line_width=4, outer_color='red', inner_color='black'):
    if not df.empty:  # Sicherstellen, dass die Daten nicht leer sind
        last_point = df.iloc[-1]

        # Innerer Punkt mit rotem Rand
        fig.add_trace(go.Scatter(
            x=[last_point[x_col]],
            y=[last_point[y_col]],
            mode='markers',
            marker=dict(
                size=inner_size,  # Größe des inneren Punkts
                color=inner_color,  # Farbe des inneren Punkts
                opacity=1.0,
                line=dict(
                    width=outer_line_width,  # Breite des äußeren Rands
                    color=outer_color  # Farbe des äußeren Rands
                )
            ),
            showlegend=False  # Spur nicht in der Legende anzeigen
        ))





# Function to calculate medians
def calculate_medians(df):
    median_oi = df['MM Net OI'].median()
    median_traders = df['MM Net Traders'].median()
    return median_oi, median_traders

# Function to calculate the scaling factors for long and short positions
def calculate_scaling_factors(df):
    max_long_position_size = df['Long Position Size'].max()
    max_short_position_size = df['Short Position Size'].max()
    long_scaling_factor = max_long_position_size / 50  # Adjust divisor as needed
    short_scaling_factor = max_short_position_size / 50  # Adjust divisor as needed
    return long_scaling_factor, short_scaling_factor

# Function to calculate concentration and clustering ranges
def calculate_ranges(agg_df, indicator):
    if indicator == 'MML':
        concentration_col = 'MML Relative Concentration'
        clustering_col = 'Long Clustering'
    elif indicator == 'MMS':
        concentration_col = 'MMS Relative Concentration'
        clustering_col = 'Short Clustering'
    else:
        raise ValueError("Invalid indicator. Must be 'MML' or 'MMS'.")

    # Filter to keep only numeric columns
    agg_df = agg_df.select_dtypes(include='number')

    # Calculate Concentration Range
    concentration_range = (agg_df[concentration_col] - agg_df[concentration_col].min()) / (agg_df[concentration_col].max() - agg_df[concentration_col].min())

    # Calculate Clustering Range
    clustering_range = (agg_df[clustering_col] - agg_df[clustering_col].min()) / (agg_df[clustering_col].max() - agg_df[clustering_col].min())

    return concentration_range * 100, clustering_range * 100

# Example calculation
median_oi, median_traders = calculate_medians(df_pivoted)

# Initialize the Dash app
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# Layout of the app
app.layout = html.Div([
    dbc.NavbarSimple(
        brand="COT-Data Overview/Analysis Dashboard",
        color="primary",
        dark=True,
        className="mb-4"
    ),
    dbc.Container([
        dbc.Row([
            dbc.Col([
                html.H2("Market Overview"),
                dcc.Dropdown(
                    id='market-dropdown',
                    options=[{'label': market, 'value': market} for market in df_pivoted['Market Names'].unique()],
                    value='Palladium',  # Default value
                    style={'width': '100%'}
                ),
                dcc.DatePickerRange(
                    id='date-picker-range',
                    start_date=df_pivoted['Date'].min(),
                    end_date=df_pivoted['Date'].max(),
                    display_format='YYYY-MM-DD',
                    className='mb-4'
                ),
                dash_table.DataTable(
                    id='overview-table',
                    columns=[
                        {'name': 'Trader Group', 'id': 'Trader Group'},
                        {'name': 'Long', 'id': 'Long'},
                        {'name': 'Short', 'id': 'Short'},
                        {'name': 'Spread', 'id': 'Spread'},
                        {'name': 'Difference (Long %)', 'id': 'Difference (Long %)'},
                        {'name': 'Difference (Short %)', 'id': 'Difference (Short %)'},
                        {'name': 'Difference (Spread %)', 'id': 'Difference (Spread %)'},
                        {'name': '% of Traders', 'id': '% of Traders'},
                        {'name': 'Number of Traders', 'id': 'Number of Traders'}
                    ],
                    style_data_conditional=[
                        {
                            'if': {
                                'filter_query': '{Difference (Long %)} < 0',
                                'column_id': 'Difference (Long %)'
                            },
                            'color': 'red'
                        },
                        {
                            'if': {
                                'filter_query': '{Difference (Long %)} > 0',
                                'column_id': 'Difference (Long %)'
                            },
                            'color': 'green'
                        },
                        {
                            'if': {
                                'filter_query': '{Difference (Short %)} < 0',
                                'column_id': 'Difference (Short %)'
                            },
                            'color': 'red'
                        },
                        {
                            'if': {
                                'filter_query': '{Difference (Short %)} > 0',
                                'column_id': 'Difference (Short %)'
                            },
                            'color': 'green'
                        },
                        {
                            'if': {
                                'filter_query': '{Difference (Spread %)} < 0',
                                'column_id': 'Difference (Spread %)'
                            },
                            'color': 'red'
                        },
                        {
                            'if': {
                                'filter_query': '{Difference (Spread %)} > 0',
                                'column_id': 'Difference (Spread %)'
                            },
                            'color': 'green'
                        },
                    ],
                    style_table={'overflowX': 'auto'},
                    style_cell={
                        'textAlign': 'left',
                        'padding': '5px',
                        'border': '1px solid grey',
                        'whiteSpace': 'normal',
                        'height': 'auto',
                    },
                    style_header={
                        'backgroundColor': 'rgb(230, 230, 230)',
                        'fontWeight': 'bold'
                    }
                )
            ], width=12)
        ]),
        html.Hr(),  # Separator
        dbc.Row([
            dbc.Col([
                html.H2("Position Price Clustering Indicator"),
                dcc.Graph(id='long-clustering-graph'),
                html.Div([
                
            ], style={'marginTop': '10px'})
            ], width=12),
            dbc.Col([
                html.H2("Position Price Clustering Indicator"),
                dcc.Graph(id='short-clustering-graph'),
                html.Br(),
        html.H4("Formula for the Position Price Clustering Indicator:"),
        html.Img(
            src="/assets/clustering_formula.png",  # Bildpfad relativ zum Projekt
            style={"width": "80%", "display": "block", "margin": "auto"}
        ),
        html.Img(
            src="/assets/clustering.png",  # Bildpfad relativ zum Projekt
            style={"width": "80%", "display": "block", "margin": "auto"}
        ) ,
        html.H4("Meaning of Shortcuts:"),
        html.Ul([
    html.Li(html.B("MML (S): Managed Money Long (Short) Positionen.")),
    html.Li(html.B("T%: Percentage distribution of positions.")),
    html.Li(html.B("TTF: Total number of traders trading futures.")),
], style={"padding": "10px", "font-size": "16px", "line-height": "1.5"})

    ], width=12)
]),
        html.Hr(),  # Separator
        dbc.Row([
            dbc.Col([
                html.H2("Position Size Indicator"),
                dcc.Graph(id='long-position-size-graph')
            ], width=12),
            dbc.Col([
                html.H2("Position Size Indicator"),
                dcc.Graph(id='short-position-size-graph')
            ], width=12)
        ]),
        dbc.Row([
    dbc.Col([
        html.Img(
            src="/assets/position_size_formula.png",  # Bild aus dem assets-Ordner
            style={"width": "80%", "display": "block", "margin": "0 auto"}  # Optional: Stil für zentrierte Anzeige
        ),
    html.H4("Meaning of Shortcuts:"),
    html.Ul([
        html.Li(html.B("MM: Managed Money.")),
        html.Li(html.B("PMPU: Producer/Merchant/Processor/User.")),
        html.Li(html.B("OR: Other Reportables.")),
        html.Li(html.B("SD: Swap Dealer.")),
        html.Li(html.B("L: Long positions.")),
        html.Li(html.B("S: Short positions.")),
    ], style={"padding": "10px", "font-size": "16px", "line-height": "1.5"})
], width=12)
    
]),
        html.Hr(),  # Separator
        dbc.Row([
            dbc.Col([
                html.H2("Dry Powder Indicator"),
                dcc.Graph(id='dry-powder-indicator-graph')
            ], width=12)
        ]),
        html.Hr(),  # Separator
        dbc.Row([
            dbc.Col([
                html.H2("DP Relative Concentration Indicator"),
                dcc.Graph(id='dp-relative-concentration-graph')
            ], width=12)
        ]),
        html.Hr(),  # Separator
        dbc.Row([
            dbc.Col([
                html.H2("DP Seasonal Indicator"),
                dcc.Graph(id='dp-seasonal-indicator-graph')
            ], width=12)
        ]),
        html.Hr(),  # Separator
        dbc.Row([
            dbc.Col([
                html.H2("DP Net Indicators with Medians"),
                dcc.Graph(id='dp-net-indicators-graph')
            ], width=12)
        ]),
        html.Hr(),  # Separator
        dbc.Row([
            dbc.Col([
                html.H2("Dry Powder Position Size Indicator"),
                dcc.RadioItems(
                    id='mm-radio',
                    options=[
                        {'label': 'MML', 'value': 'MML'},
                        {'label': 'MMS', 'value': 'MMS'}
                    ],
                    value='MML',
                    className='mb-4'
                ),
                dcc.Graph(id='dp-position-size-indicator')
            ], width=12)
        ]),
        html.Hr(),  # Separator
        dbc.Row([
            dbc.Col([
                html.H2("Dry Powder Hedging Indicator"),
                dcc.RadioItems(
                    id='trader-group-radio',
                    options=[
                        {'label': 'MML', 'value': 'MML'},
                        {'label': 'MMS', 'value': 'MMS'}
                    ],
                    value='MML',
                    className='mb-4'
                ),
                dcc.Graph(id='hedging-indicator-graph')
            ], width=12)
        ]),
        html.Hr(),  # Separator
        dbc.Row([
            dbc.Col([
                html.H2("Dry Powder Concentration/Clustering Indicator"),
                dcc.DatePickerRange(
                    id='concentration-clustering-date-picker-range',
                    start_date=default_start_date,
                    end_date=default_end_date,
                    display_format='YYYY-MM-DD',
                    className='mb-4'
                ),
                dcc.RadioItems(
                    id='concentration-clustering-radio',
                    options=[
                        {'label': 'MML', 'value': 'MML'},
                        {'label': 'MMS', 'value': 'MMS'}
                    ],
                    value='MML',  # Default value
                    inline=True,
                    className='mb-4'
                ),
                dcc.Graph(id='dp-concentration-clustering-graph')
            ], width=12)
        ]),
        html.Hr(),  # Separator
        dbc.Row([
            dbc.Col([
                html.Footer('© 2024 Market Analysis Dashboard', className='text-center mt-4')
            ])
        ])
    ], fluid=True)
])

# Callback to update the table
@app.callback(
    Output('overview-table', 'data'),
    [
        Input('market-dropdown', 'value'),
        Input('date-picker-range', 'start_date'),
        Input('date-picker-range', 'end_date')
    ]
)
def update_table(selected_market, start_date, end_date):
    filtered_df = df_pivoted[(df_pivoted['Market Names'] == selected_market) & 
                             (df_pivoted['Date'] >= start_date) & (df_pivoted['Date'] <= end_date)]

    if filtered_df.empty:
        return []

    first_row = filtered_df.iloc[0]
    current_row = filtered_df.iloc[-1]

    data = {
        'Trader Group': [
            'Producer/Merchant/Processor/User',
            'Swap Dealer',
            'Managed Money',
            'Other Reportables'
        ],
        'Long': [
            first_row['Producer/Merchant/Processor/User Long'],
            first_row['Swap Dealer Long'],
            first_row['Managed Money Long'],
            first_row['Other Reportables Long']
        ],
        'Short': [
            first_row['Producer/Merchant/Processor/User Short'],
            first_row['Swap Dealer Short'],
            first_row['Managed Money Short'],
            first_row['Other Reportables Short']
        ],
        'Spread': [
            0,  # Assuming no spread for Producer/Merchant/Processor/User
            first_row['Swap Dealer Spread'],
            first_row['Managed Money Spread'],
            first_row['Other Reportables Spread']
        ],
        'Difference (Long %)': [
            round(((current_row['Producer/Merchant/Processor/User Long'] - first_row['Producer/Merchant/Processor/User Long']) / first_row['Producer/Merchant/Processor/User Long']) * 100, 2),
            round(((current_row['Swap Dealer Long'] - first_row['Swap Dealer Long']) / first_row['Swap Dealer Long']) * 100, 2),
            round(((current_row['Managed Money Long'] - first_row['Managed Money Long']) / first_row['Managed Money Long']) * 100, 2),
            round(((current_row['Other Reportables Long'] - first_row['Other Reportables Long']) / first_row['Other Reportables Long']) * 100, 2)
        ],
        'Difference (Short %)': [
            round(((current_row['Producer/Merchant/Processor/User Short'] - first_row['Producer/Merchant/Processor/User Short']) / first_row['Producer/Merchant/Processor/User Short']) * 100, 2),
            round(((current_row['Swap Dealer Short'] - first_row['Swap Dealer Short']) / first_row['Swap Dealer Short']) * 100, 2),
            round(((current_row['Managed Money Short'] - first_row['Managed Money Short']) / first_row['Managed Money Short']) * 100, 2),
            round(((current_row['Other Reportables Short'] - first_row['Other Reportables Short']) / first_row['Other Reportables Short']) * 100, 2)
        ],
        'Difference (Spread %)': [
            0,  # Assuming no spread for Producer/Merchant/Processor/User
            round(((current_row['Swap Dealer Spread'] - first_row['Swap Dealer Spread']) / first_row['Swap Dealer Spread']) * 100, 2),
            round(((current_row['Managed Money Spread'] - first_row['Managed Money Spread']) / first_row['Managed Money Spread']) * 100, 2),
            round(((current_row['Other Reportables Spread'] - first_row['Other Reportables Spread']) / first_row['Other Reportables Spread']) * 100, 2)
        ],
        '% of Traders': [
            f"Long: {round(current_row['Traders Prod/Merc Long'] / current_row['Total Number of Traders'] * 100, 2)}%, Short: {round(current_row['Traders Prod/Merc Short'] / current_row['Total Number of Traders'] * 100, 2)}%",
            f"Long: {round(current_row['Traders Swap Long'] / current_row['Total Number of Traders'] * 100, 2)}%, Short: {round(current_row['Traders Swap Short'] / current_row['Total Number of Traders'] * 100, 2)}%, Spread: {round(current_row['Traders Swap Spread'] / current_row['Total Number of Traders'] * 100, 2)}%",
            f"Long: {round(current_row['Traders M Money Long'] / current_row['Total Number of Traders'] * 100, 2)}%, Short: {round(current_row['Traders M Money Short'] / current_row['Total Number of Traders'] * 100, 2)}%, Spread: {round(current_row['Traders M Money Spread'] / current_row['Total Number of Traders'] * 100, 2)}%",
            f"Long: {round(current_row['Traders Other Rept Long'] / current_row['Total Number of Traders'] * 100, 2)}%, Short: {round(current_row['Traders Other Rept Short'] / current_row['Total Number of Traders'] * 100, 2)}%, Spread: {round(current_row['Traders Other Rept Spread'] / current_row['Total Number of Traders'] * 100, 2)}%"
        ],
        'Number of Traders': [
            f"Long: {current_row['Traders Prod/Merc Long']}, Short: {current_row['Traders Prod/Merc Short']}",
            f"Long: {current_row['Traders Swap Long']}, Short: {current_row['Traders Swap Short']}, Spread: {current_row['Traders Swap Spread']}",
            f"Long: {current_row['Traders M Money Long']}, Short: {current_row['Traders M Money Short']}, Spread: {current_row['Traders M Money Spread']}",
            f"Long: {current_row['Traders Other Rept Long']}, Short: {current_row['Traders Other Rept Short']}, Spread: {current_row['Traders Other Rept Spread']}"
        ]
    }

    return pd.DataFrame(data).to_dict('records')

# Callback to update graphs based on selected market and date range
@app.callback(
    [Output('long-clustering-graph', 'figure'),
     Output('short-clustering-graph', 'figure'),
     Output('long-position-size-graph', 'figure'),
     Output('short-position-size-graph', 'figure'),
     Output('dry-powder-indicator-graph', 'figure'),
     Output('dp-relative-concentration-graph', 'figure'),
     Output('dp-seasonal-indicator-graph', 'figure'),
     Output('dp-net-indicators-graph', 'figure'),
     Output('dp-position-size-indicator', 'figure'),
     Output('hedging-indicator-graph', 'figure')],
    [Input('market-dropdown', 'value'),
     Input('date-picker-range', 'start_date'),
     Input('date-picker-range', 'end_date'),
     Input('mm-radio', 'value'),
     Input('trader-group-radio', 'value')]
)
def update_graphs(selected_market, start_date, end_date, mm_type, trader_group):
    filtered_df = df_pivoted[(df_pivoted['Market Names'] == selected_market) &
                             (df_pivoted['Date'] >= start_date) & 
                             (df_pivoted['Date'] <= end_date)]

    long_scaling_factor, short_scaling_factor = calculate_scaling_factors(filtered_df)
    
    # Long Positions Clustering
    long_clustering_fig = go.Figure()

    max_bubble_size = 100  # Maximale gewünschte Größe der Bubbles
    min_bubble_size = 10  # Mindestpunktgröße für die kleinsten Punkte
    max_traders = df_pivoted['Total Number of Traders'].max()

# Neue sizeref-Berechnung basierend auf dem angepassten Divisor
    sizeref = 2 * max_traders / (max_bubble_size**2.5)

# Add the scatterplot
    long_clustering_fig.add_trace(go.Scatter(
    x=filtered_df['Date'],
    y=filtered_df['Open Interest'],
    mode='markers',
    marker=dict(
        size=filtered_df['Total Number of Traders'] / 10,  # Adjusted bubble size for clarity
        color=filtered_df['Long Clustering'],  # Color based on clustering
        colorscale='Viridis',
        showscale=True,  # Display color scale
        colorbar=dict(
            title="Long Clustering (%)",  # Title for color scale
            thickness=15,
            len=0.75,
            yanchor='middle',
            y=0.5  # Position of the color bar
        ),
    ),
    text=[f"Traders: {traders}" for traders in filtered_df['Total Number of Traders']],  # Tooltip
    hoverinfo='text',
    showlegend=False
))

# Add bubble size legend
    bubble_sizes = [50, 100, 150]  
    for size in bubble_sizes:
        long_clustering_fig.add_trace(go.Scatter(
        x=[None], y=[None],
        mode='markers',
        marker=dict(
            size=size / 10,  # Adjusted to match main scatterplot scaling
            color='gray',  # Neutral color for legend bubbles
            opacity=0.6
        ),
        legendgroup="Bubble Size",
        showlegend=True,
        name=f"{size} Traders"  # Label for the legend
    ))

    

# Update layout
    long_clustering_fig.update_layout(
    title='Long Positions Clustering Indicator',
    xaxis_title='Date',
    yaxis_title='Open Interest',
    xaxis=dict(
        tickmode='array',
        tickvals=filtered_df['Date'].dt.year.unique(),
        ticktext=[str(year) for year in filtered_df['Date'].dt.year.unique()],
        showgrid=True,
        ticks="outside",
        tickangle=45
    ),
    yaxis=dict(
        title='Open Interest',
        showgrid=True,
        tick0=0,  # Startwert
        dtick=20000 if selected_market in ['Gold', 'Silver', 'Copper'] else 5000,  # Dynamische Schrittweite        gridwidth=1.5  # Dicke der Gitterlinien
    ),
    legend=dict(
        title=dict(text="Number of Traders"),  # Legend title
        x=1.2,  # Adjust position of legend
        y=0.5,
        font=dict(size=12)
    ),
    annotations=[
        dict(
            x=1.35,  # Position rechts neben der Legende
            y=0.01,   # Vertikale Position
            xref='paper',
            yref='paper',
            text=(
                "Die Punktgröße im Scatterplot <br>zeigt die Anzahl der Trader:<br>"
                "- ≤ 50 Trader: Kleinere Punkte<br>"
                "- 51–100 Trader: Mittlere Punkte<br>"
                "- 101–150 Trader: Größte Punkte"
            ),
            showarrow=False,
            align="left",
            font=dict(
                size=12,
                color="black"
            )
        )
    ]
)


    add_last_point_highlight(
    fig=long_clustering_fig,
    df=filtered_df,
    x_col='Date',
    y_col='Open Interest',
    inner_size=2,
    inner_color='black'
)



    
    # Short Positions Clustering
    short_clustering_fig = go.Figure()

# Add the scatterplot for short clustering
    short_clustering_fig.add_trace(go.Scatter(
    x=filtered_df['Date'],
    y=filtered_df['Open Interest'],
    mode='markers',
    marker=dict(
        size=filtered_df['Total Number of Traders'] / 10,  # Adjust size for better visualization
        color=filtered_df['Short Clustering'],
        colorscale='Viridis',
        showscale=True,
        colorbar=dict(
            title="Short Clustering (%)",
            thickness=15,
            len=0.75,
            yanchor='middle',
            y=0.5  # Position of the color bar
        ),
    ),
    text=[f"Traders: {traders}" for traders in filtered_df['Total Number of Traders']],  # Tooltip
    hoverinfo='text',
    showlegend=False
))

# Add bubble size legend for short clustering
    bubble_sizes = [50, 100, 150]  # Example values
    for size in bubble_sizes:
        short_clustering_fig.add_trace(go.Scatter(
        x=[None], y=[None],
        mode='markers',
        marker=dict(
            size=size / 10,  # Adjusted to match main scatterplot scaling
            color='gray',  # Neutral color for legend bubbles
            opacity=0.6
        ),
        legendgroup="Bubble Size",
        showlegend=True,
        name=f"{size} Traders"  # Label for the legend
    ))

# Update layout for short clustering graph
    short_clustering_fig.update_layout(
    title='Short Positions Clustering Indicator',
    xaxis_title='Date',
    yaxis_title='Open Interest',
    xaxis=dict(
        tickmode='array',
        tickvals=filtered_df['Date'].dt.year.unique(),
        ticktext=[str(year) for year in filtered_df['Date'].dt.year.unique()],
        showgrid=True,
        ticks="outside",
        tickangle=45
    ),
    yaxis=dict(
        title='Open Interest',
        showgrid=True,
        tick0=0,  # Startwert
        dtick=20000 if selected_market in ['Gold', 'Silver', 'Copper'] else 5000,  # Dynamische Schrittweite        gridwidth=1.5  # Dicke der Gitterlinien
    ),
    legend=dict(
        title=dict(text="Number of Traders"),  # Legend title
        x=1.2,  # Adjust position of legend
        y=0.5,
        font=dict(size=12)
    )
)

# Add last point highlight
    add_last_point_highlight(
    fig=short_clustering_fig,
    df=filtered_df,
    x_col='Date',
    y_col='Open Interest',
    inner_size=2,
    inner_color='black'
)

    # Long Position Size Indicator
    long_position_size_fig = go.Figure()

# Add scatterplot for long position size
    long_position_size_fig.add_trace(go.Scatter(
    x=filtered_df['Date'],
    y=filtered_df['Open Interest'],
    mode='markers',
    marker=dict(
        size=(filtered_df['Long Position Size'] ** (1/3.5)),  # Dynamically adjusted size
        color=filtered_df['Long Position Size'],  # Color based on number of traders
        colorscale='Viridis',
        showscale=True,
        colorbar=dict(
            title="Long Position Size ($)",  # Title for colorbar
            thickness=15,
            len=0.75,
            yanchor='middle',
            y=0.5
        )
    ),
    text=[f"Position Size: {size}" for size in filtered_df['Long Position Size']],  # Tooltip
    hoverinfo='text',
    showlegend=False
))
    """
# Add bubble size legend
    bubble_sizes = [100, 200, 300, 400]  # Example values
    for size in bubble_sizes:
        long_position_size_fig.add_trace(go.Scatter(
        x=[None], y=[None],
        mode='markers',
        marker=dict(
            size=(filtered_df['Long Position Size'] ** (1/3.5)),  # Adjusted to match main scatterplot scaling
            color='gray',  # Neutral color for legend bubbles
            opacity=0.6
        ),
        legendgroup="Bubble Size",
        showlegend=True,
        name=f"{size} Traders"  # Label for the legend
    ))
    """
# Update layout for long position size graph
    long_position_size_fig.update_layout(
    title='Long Position Size Indicator',
    xaxis_title='Date',
    yaxis_title='Open Interest',
    xaxis=dict(
        tickmode='array',
        tickvals=filtered_df['Date'].dt.year.unique(),
        ticktext=[str(year) for year in filtered_df['Date'].dt.year.unique()],
        showgrid=True,
        ticks="outside",
        tickangle=45
    ),
    yaxis=dict(
        title='Open Interest',
        showgrid=True,
        tick0=0,  # Startwert
        dtick=20000 if selected_market in ['Gold', 'Silver', 'Copper'] else 5000,  # Dynamische Schrittweite        gridwidth=1.5  # Dicke der Gitterlinien
    ),
    legend=dict(
        title=dict(text="Number of Traders"),  # Legend title
        x=1.2,  # Adjust position of legend
        y=0.5,
        font=dict(size=12)
    )
)

# Highlight last point
    add_last_point_highlight(
    fig=long_position_size_fig,
    df=filtered_df,
    x_col='Date',
    y_col='Open Interest',
    inner_size=2,
    inner_color='black'
)

    
    # Short Position Size Indicator
    short_position_size_fig = go.Figure()

# Add scatterplot for short position size
    short_position_size_fig.add_trace(go.Scatter(
    x=filtered_df['Date'],
    y=filtered_df['Open Interest'],
    mode='markers',
    marker=dict(
        size=(filtered_df['Short Position Size'] ** (1/3.5)),  # Dynamically adjusted size
        color=filtered_df['Short Position Size'],  # Color based on number of traders
        colorscale='Viridis',
        showscale=True,
        colorbar=dict(
            title="Short Position Size ($)",  # Title for colorbar
            thickness=15,
            len=0.75,
            yanchor='middle',
            y=0.5
        )
    ),
    text=[f"Position Size: {size}" for size in filtered_df['Short Position Size']],  # Tooltip
    hoverinfo='text',
    showlegend=False
))
    """
# Add bubble size legend
    bubble_sizes = [50, 100, 150, 200, 250]  # Example values
    for size in bubble_sizes:
        short_position_size_fig.add_trace(go.Scatter(
        x=[None], y=[None],
        mode='markers',
        marker=dict(
            size=size / 10,  # Adjusted to match main scatterplot scaling
            color='gray',  # Neutral color for legend bubbles
            opacity=0.6
        ),
        legendgroup="Bubble Size",
        showlegend=True,
        name=f"{size} Traders"  # Label for the legend
    ))
    """
# Update layout for short position size graph
    short_position_size_fig.update_layout(
    title='Short Position Size Indicator',
    xaxis_title='Date',
    yaxis_title='Open Interest',
    xaxis=dict(
        tickmode='array',
        tickvals=filtered_df['Date'].dt.year.unique(),
        ticktext=[str(year) for year in filtered_df['Date'].dt.year.unique()],
        showgrid=True,
        ticks="outside",
        tickangle=45
    ),
    yaxis=dict(
        title='Open Interest',
        showgrid=True,
        tick0=0,  # Startwert
        dtick=50000 if selected_market in ['Gold', 'Silver', 'Copper'] else 5000,  # Dynamische Schrittweite        gridwidth=1.5  # Dicke der Gitterlinien
    ),
    legend=dict(
        title=dict(text="Number of Traders"),  # Legend title
        x=1.2,  # Adjust position of legend
        y=0.5,
        font=dict(size=12)
    )
)

# Highlight last point
    add_last_point_highlight(
    fig=short_position_size_fig,
    df=filtered_df,
    x_col='Date',
    y_col='Open Interest',
    inner_size=2,
    inner_color='black'
)

    
    # Dry Powder Indicator
    dry_powder_fig = go.Figure()

    # Add trace for MML Long
    dry_powder_fig.add_trace(go.Scatter(
        x=filtered_df['MML Traders'],
        y=filtered_df['MML Long OI'],
        mode='markers',
        marker=dict(
            size=(filtered_df['MML Long OI'] + abs(filtered_df['MML Short OI'])) / 80,
            color='cyan',
            opacity=0.6,
            sizeref=sizeref,
            line=dict(width=1, color='DarkSlateGrey')
        ),
        name='MML Long'
    ))

    # Add trace for MML Short
    dry_powder_fig.add_trace(go.Scatter(
        x=filtered_df['MMS Traders'],
        y=filtered_df['MML Short OI'],
        mode='markers',
        marker=dict(
            size=(filtered_df['MML Long OI'] + abs(filtered_df['MML Short OI'])) / 80,
            color='white',
            opacity=0.6,
            sizeref=sizeref,
            line=dict(width=1, color='DarkSlateGrey')
        ),
        name='MML Short'
    ))

    # Mark first and last entry for MML Long
    dry_powder_fig.add_trace(go.Scatter(
        x=[filtered_df['MML Traders'].iloc[0]],
        y=[filtered_df['MML Long OI'].iloc[0]],
        mode='markers',
        marker=dict(
            size=15,
            color='green',
            symbol='star',
            line=dict(width=2, color='black')
        ),
        name='MML Long (First)'
    ))
    dry_powder_fig.add_trace(go.Scatter(
        x=[filtered_df['MML Traders'].iloc[-1]],
        y=[filtered_df['MML Long OI'].iloc[-1]],
        mode='markers',
        marker=dict(
            size=15,
            color='yellow',
            symbol='star',
            line=dict(width=2, color='black')
        ),
        name='MML Long (Last)'
    ))

    # Mark first and last entry for MML Short
    dry_powder_fig.add_trace(go.Scatter(
        x=[filtered_df['MMS Traders'].iloc[0]],
        y=[filtered_df['MML Short OI'].iloc[0]],
        mode='markers',
        marker=dict(
            size=15,
            color='green',
            symbol='star',
            line=dict(width=2, color='black')
        ),
        name='MML Short (First)'
    ))
    dry_powder_fig.add_trace(go.Scatter(
        x=[filtered_df['MMS Traders'].iloc[-1]],
        y=[filtered_df['MML Short OI'].iloc[-1]],
        mode='markers',
        marker=dict(
            size=15,
            color='yellow',
            symbol='star',
            line=dict(width=2, color='black')
        ),
        name='MML Short (Last)'
    ))

    dry_powder_fig.update_layout(
    xaxis=dict(
        title='Number of Traders',
        showgrid=True,  # Aktiviert das Grid
        gridcolor='LightGray',  # Farbe des Grids
        gridwidth=2,  # Dicke des Grids
        zeroline=False  # Keine zusätzliche Null-Linie
    ),
    yaxis=dict(
        title='Long and Short OI',
        showgrid=True,
        gridcolor='LightGray',
        gridwidth=2,
        zeroline=False
    ),
    plot_bgcolor='white',  # Weißer Hintergrund für besseren Kontrast
    template=None
)




    
    # DP Relative Concentration Indicator
    dp_relative_concentration_fig = go.Figure()

    # Define the groups and colors
    groups = [
        ('PMPUL', 'PMPUL Relative Concentration', 'PMPUL Traders', 'darkgreen'),
        ('PMPUS', 'PMPUS Relative Concentration', 'PMPUS Traders', 'lime'),
        ('SDL', 'SDL Relative Concentration', 'SDL Traders', 'darkorange'),
        ('SDS', 'SDS Relative Concentration', 'SDS Traders', 'moccasin'),
        ('MML', 'MML Relative Concentration', 'MML Traders', 'royalblue'),
        ('MMS', 'MMS Relative Concentration', 'MMS Traders', 'cyan'),
        ('ORL', 'ORL Relative Concentration', 'ORL Traders', 'indigo'),
        ('ORS', 'ORS Relative Concentration', 'ORS Traders', 'plum')
    ]

    for group, y_col, x_col, color in groups:
        # Add trace for each group
        dp_relative_concentration_fig.add_trace(go.Scatter(
            x=filtered_df[x_col],
            y=filtered_df[y_col],
            mode='markers',
            marker=dict(
                size=abs(filtered_df[y_col]) / sizeref,  # Adjust size for better visualization
                color=color,
                opacity=0.6,
                sizeref=sizeref,
                line=dict(width=1, color='DarkSlateGrey')
            ),
            name=group,
            visible='legendonly'  # Initially hide all traces
        ))

        # Mark first and last entry for each group with 'legendonly' visibility
        dp_relative_concentration_fig.add_trace(go.Scatter(
            x=[filtered_df[x_col].iloc[0]],
            y=[filtered_df[y_col].iloc[0]],
            mode='markers',
            marker=dict(
                size=15,
                color=color,
                symbol='star',
                line=dict(width=2, color='black')
            ),
            name=f'{group} (First)',
            visible='legendonly'  # Initially hide
        ))
        dp_relative_concentration_fig.add_trace(go.Scatter(
            x=[filtered_df[x_col].iloc[-1]],
            y=[filtered_df[y_col].iloc[-1]],
            mode='markers',
            marker=dict(
                size=15,
                color=color,
                symbol='star',
                line=dict(width=2, color='black')
            ),
            name=f'{group} (Last)',
            visible='legendonly'  # Initially hide
        ))

    dp_relative_concentration_fig.update_layout(
        title='DP Relative Concentration Indicator',
        xaxis_title='Number of Traders',
        yaxis_title='Long and Short Concentration',
        legend_title='Trader Group'
    )

    # Calculate a global sizeref to ensure consistency across markets
    max_bubble_size = 100  # Adjusted for better visualization
    min_bubble_size = 5   # Set minimum bubble size

    # DP Seasonal Indicator
    dp_seasonal_indicator_fig = go.Figure()

    quarters = ['Q1', 'Q2', 'Q3', 'Q4']
    colors = ['blue', 'cyan', 'orange', 'red']

    for quarter, color in zip(quarters, colors):
        quarter_data = filtered_df[filtered_df['Quarter'] == quarter]

        # Normalize the sizes to a 0-1 range
        norm_sizes = (quarter_data['PMPUL Relative Concentration'] - quarter_data['PMPUL Relative Concentration'].min()) / (quarter_data['PMPUL Relative Concentration'].max() - quarter_data['PMPUL Relative Concentration'].min())
    
        # Scale sizes to the desired range
        scaled_sizes = norm_sizes * (max_bubble_size - min_bubble_size) + min_bubble_size

        dp_seasonal_indicator_fig.add_trace(go.Scatter(
            x=quarter_data['PMPUL Traders'],
            y=quarter_data['PMPUL Relative Concentration'],
            mode='markers',
            marker=dict(
                size=scaled_sizes,
                color=color,
                opacity=0.6,
                line=dict(width=1, color='DarkSlateGrey')
            ),
            name=quarter
        ))

    most_recent_date = filtered_df['Date'].max()
    recent_data = filtered_df[filtered_df['Date'] == most_recent_date]
    dp_seasonal_indicator_fig.add_trace(go.Scatter(
        x=recent_data['PMPUL Traders'],
        y=recent_data['PMPUL Relative Concentration'],
        mode='markers',
        marker=dict(
            size=15,
            color='black',
            symbol='circle',
            line=dict(width=2, color='black')
        ),
        name='Most Recent Week'
    ))

    dp_seasonal_indicator_fig.update_layout(
        title='DP Seasonal Indicator',
        xaxis_title='Number of Traders',
        yaxis_title='Long and Short Concentration',
        legend_title='Quarter'
    )
    
    # DP Net Indicators with Medians
    most_recent_date = filtered_df['Date'].max()
    first_date = filtered_df['Date'].min()
    median_oi, median_traders = calculate_medians(filtered_df)
    
    dp_net_indicators_fig = go.Figure()

    # Color coding by Year
    for year in filtered_df['Year'].unique():
        year_data = filtered_df[filtered_df['Year'] == year]

        dp_net_indicators_fig.add_trace(go.Scatter(
            x=year_data['MM Net Traders'],
            y=year_data['MM Net OI'],
            mode='markers',
            marker=dict(size=10, opacity=0.6),
            name=str(year)
        ))

    # Adding markers for the most recent and first weeks
    recent_data = filtered_df[filtered_df['Date'] == most_recent_date]
    first_data = filtered_df[filtered_df['Date'] == first_date]
    
    dp_net_indicators_fig.add_trace(go.Scatter(
        x=recent_data['MM Net Traders'],
        y=recent_data['MM Net OI'],
        mode='markers',
        marker=dict(size=12, color='black', symbol='circle'),
        name='Most Recent Week'
    ))

    dp_net_indicators_fig.add_trace(go.Scatter(
        x=first_data['MM Net Traders'],
        y=first_data['MM Net OI'],
        mode='markers',
        marker=dict(size=12, color='red', symbol='circle'),
        name='First Week'
    ))

    # Adding medians
    dp_net_indicators_fig.add_trace(go.Scatter(
        x=[median_traders, median_traders],
        y=[filtered_df['MM Net OI'].min(), filtered_df['MM Net OI'].max()],
        mode='lines',
        line=dict(color='gray', dash='dash'),
        name='Median Net Traders'
    ))

    dp_net_indicators_fig.add_trace(go.Scatter(
        x=[filtered_df['MM Net Traders'].min(), filtered_df['MM Net Traders'].max()],
        y=[median_oi, median_oi],
        mode='lines',
        line=dict(color='gray', dash='dash'),
        name='Median Net OI'
    ))

    dp_net_indicators_fig.update_layout(
        title='DP Net Indicators with Medians',
        xaxis_title='MM Net Number of Traders',
        yaxis_title='MM Net OI',
        legend_title='Year'
    )
    
    # Dry Powder Position Size Indicator (MML & MMS)
    dff = filtered_df
    if mm_type == 'MML':
        x = dff['Traders M Money Long']
        y = dff['MML Position Size']
        color = dff['Open Interest']
        recent_week = dff['MML Position Size'].iloc[-1]
        recent_x = dff['Traders M Money Long'].iloc[-1]
        first_week = dff['MML Position Size'].iloc[0]
        first_x = dff['Traders M Money Long'].iloc[0]
    else:
        x = dff['Traders M Money Short']
        y = dff['MMS Position Size']
        color = dff['Open Interest']
        recent_week = dff['MMS Position Size'].iloc[-1]
        recent_x = dff['Traders M Money Short'].iloc[-1]
        first_week = dff['MMS Position Size'].iloc[0]
        first_x = dff['Traders M Money Short'].iloc[0]

    median_x = x.median()
    median_y = y.median()

    dp_position_size_fig = go.Figure()

    dp_position_size_fig.add_trace(go.Scatter(
        x=x,
        y=y,
        mode='markers',
        marker=dict(
            size=10,
            color=color,
            colorscale='Viridis',
            showscale=True,
            colorbar=dict(
                title='',
                thickness=15,
                len=0.75,
                yanchor='middle'
            )
        ),
        text=dff['Date'],
        hoverinfo='text',
        showlegend=False  # Hide the legend for this trace
    ))

    dp_position_size_fig.add_trace(go.Scatter(
        x=[recent_x],
        y=[recent_week],
        mode='markers',
        marker=dict(
            size=12,
            color='black'
        ),
        name='Most Recent Week'
    ))

    dp_position_size_fig.add_trace(go.Scatter(
        x=[first_x],
        y=[first_week],
        mode='markers',
        marker=dict(
            size=12,
            color='red'
        ),
        name='First Week'
    ))

    dp_position_size_fig.add_shape(type="line",
                  x0=median_x, y0=0, x1=median_x, y1=max(y),
                  line=dict(color="Gray", width=1, dash="dash"))

    dp_position_size_fig.add_shape(type="line",
                  x0=0, y0=median_y, x1=max(x), y1=median_y,
                  line=dict(color="Gray", width=1, dash="dash"))

    dp_position_size_fig.update_layout(
        title='Dry Powder Position Size Indicator ({})'.format(mm_type),
        xaxis_title='Number of {} Traders'.format(mm_type),
        yaxis_title='{} Position Size'.format(mm_type),
        showlegend=True,
    )
    
    # Dry Powder Hedging Indicator (MML vs PMPUL / MMS vs PMPUS)
    hedging_fig = create_hedging_indicator(filtered_df, trader_group, start_date, end_date)

    return (long_clustering_fig, short_clustering_fig, long_position_size_fig, 
            short_position_size_fig, dry_powder_fig, dp_relative_concentration_fig,
            dp_seasonal_indicator_fig, dp_net_indicators_fig, dp_position_size_fig, hedging_fig)

# Function to create the hedging indicator
def create_hedging_indicator(data, trader_group, start_date, end_date):
    # Filter data by date range
    mask = (data['Date'] >= start_date) & (data['Date'] <= end_date)
    data = data.loc[mask]

    if trader_group == "MML":
        x = 'Traders M Money Long'
        y = 'MML Long OI'
        color = 'PMPUL Relative Concentration'
        title = 'Dry Powder Hedging Indicator (MML vs PMPUL)'
        colorbar_title = 'PMPUL OI Range'
    else:
        x = 'Traders M Money Short'
        y = 'MMS Short OI'
        color = 'PMPUS Relative Concentration'
        title = 'Dry Powder Hedging Indicator (MMS vs PMPUS)'
        colorbar_title = 'PMPUS OI Range'

    # Create the scatter plot
    trace = go.Scatter(
        x=data[x],
        y=data[y],
        mode='markers',
        marker=dict(
            size=data['Open Interest'] / 1500,  # Adjust the size scale if necessary
            color=data[color],
            colorscale='RdBu',
            showscale=True,
            colorbar=dict(
                title=colorbar_title,
                len=0.5,
                x=1.1  # Adjust the x position of the color bar
            )
        ),
        text=data['Market Names'],
        hoverinfo='text',
        showlegend=False  # Remove the default trace name from the legend
    )

    # Add markers for the first and last week
    first_week = data.iloc[0]
    last_week = data.iloc[-1]

    first_week_trace = go.Scatter(
        x=[first_week[x]],
        y=[first_week[y]],
        mode='markers',
        marker=dict(color='red', size=15),
        name='First Week'
    )

    last_week_trace = go.Scatter(
        x=[last_week[x]],
        y=[last_week[y]],
        mode='markers',
        marker=dict(color='black', size=15),
        name='Most Recent Week'
    )

    # Create the layout
    layout = go.Layout(
        title=title,
        xaxis=dict(
            title='MM Number of Long Traders' if trader_group == "MML" else 'MM Number of Short Traders',
            range=[min(data[x]) - 10, max(data[x]) + 10]
        ),
        yaxis=dict(
            title='MM Long OI' if trader_group == "MML" else 'MM Short OI',
            range=[min(data[y]) - 50000, max(data[y]) + 50000]
        ),
        showlegend=True,
        width=1000,
        height=600
    )

    # Create the figure
    fig = go.Figure(data=[trace, first_week_trace, last_week_trace], layout=layout)

    return fig

# Callback to update the Dry Powder Concentration/Clustering Indicator graph
@app.callback(
    Output('dp-concentration-clustering-graph', 'figure'),
    [Input('concentration-clustering-date-picker-range', 'start_date'),
     Input('concentration-clustering-date-picker-range', 'end_date'),
     Input('concentration-clustering-radio', 'value')]
)
def update_concentration_clustering_graph(start_date, end_date, selected_indicator):
    filtered_df = df_pivoted[(df_pivoted['Date'] >= start_date) & 
                             (df_pivoted['Date'] <= end_date)]
    
    # Aggregate the data by market, keeping only numeric columns
    agg_df = filtered_df.groupby('Market Names').mean(numeric_only=True).reset_index()
    
    concentration_range, clustering_range = calculate_ranges(agg_df, selected_indicator)
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=clustering_range,
        y=concentration_range,
        mode='markers+text',
        text=agg_df['Market Names'],
        textposition='top center',
        marker=dict(size=10, opacity=0.6, color='green', line=dict(width=1, color='black'))
    ))
    
    fig.update_layout(
        title=f'Dry Powder Concentration/Clustering Indicator ({selected_indicator})',
        xaxis_title='MM Long Clustering Range' if selected_indicator == 'MML' else 'MM Short Clustering Range',
        yaxis_title='MM Long Concentration Range' if selected_indicator == 'MML' else 'MM Short Concentration Range',
        xaxis=dict(range=[-5, 110]),  # Adjusted to ensure all bubbles are visible
        yaxis=dict(range=[-5, 110]),  # Adjusted to ensure all bubbles are visible
        showlegend=False
    )
    
    return fig

# Open browser automatically
if __name__ == '__main__':
    Timer(1, open_browser).start()
    app.run_server(debug=True, port=8051)
