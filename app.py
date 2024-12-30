from flask import Flask, render_template
import mysql.connector
import pandas as pd

app = Flask(__name__)

# Database connection configuration
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'password',
    'database': 'crypto_db'
}

def get_db_connection():
    """Establish a connection to the MySQL database."""
    return mysql.connector.connect(**DB_CONFIG)

def fetch_data_as_dataframe():
    """Fetch data from the 'coins' table and return as a Pandas DataFrame."""
    connection = get_db_connection()
    query = "SELECT * FROM coins"
    try:
        df = pd.read_sql(query, connection)
    finally:
        connection.close()
    return df

@app.route('/')
def display_coins():
    """Route to display coins data on an HTML page."""
    df = fetch_data_as_dataframe()
    coins = df.to_dict(orient='records')  # Convert DataFrame to list of dictionaries
    return render_template('index.html', coins=coins)

if __name__ == '__main__':
    app.run(debug=True)
