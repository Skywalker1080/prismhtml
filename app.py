from flask import Flask, render_template
import pandas as pd
from sqlalchemy import create_engine

app = Flask(__name__)

# Database connection configuration
DB_CONFIG = {
    'host': '34.55.195.199',        # GCP PostgreSQL instance public IP
    'database': 'dbcp',             # Database name
    'user': 'yogass09',             # Username
    'password': 'jaimaakamakhya',   # Password
    'port': 5432                    # PostgreSQL default port
}

def get_gcp_engine():
    """Create and return a SQLAlchemy engine for the GCP PostgreSQL database."""
    connection_url = f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@" \
                     f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    return create_engine(connection_url)

# Initialize the GCP engine
gcp_engine = get_gcp_engine()
def fetch_data_as_dataframe():
    """Fetch data from the 'coins' table and return as a Pandas DataFrame."""
    query_top_100 = """
      SELECT slug, cmc_rank, last_updated, symbol, price, percent_change24h, market_cap, last_updated
      FROM crypto_listings_latest_1000
      WHERE cmc_rank < 50
      """
    try:
        # Use gcp_engine to execute the query and fetch data as a DataFrame
        top_100_cc  = pd.read_sql_query(query_top_100, gcp_engine)
        # Convert market_cap to billions and round to 2 decimal places
        top_100_cc['market_cap'] = (top_100_cc['market_cap'] / 1_000_000_000).round(2)
    except Exception as e:
        print(f"Error fetching data: {e}")
        top_100_cc = pd.DataFrame()  # Return an empty DataFrame in case of error
    return top_100_cc

@app.route('/')
def display_coins():
    """Route to display coins data on an HTML page."""
    df = fetch_data_as_dataframe()
    # Convert DataFrame to list of dictionaries
    coins = df.to_dict(orient='records')  
    return render_template('index.html', coins=coins)

@app.route('/2')
def display_page2():
    """Route to display the second page with coins ranked 16-36, split into three equal parts."""
    df2 = fetch_data_as_dataframe()
    # Skip first 15 rows and take next 21 rows (ranks 16-36)
    df2 = df2.iloc[15:36]
    
    # Split into three DataFrames of 7 rows each
    df2_part1 = df2.iloc[0:7]    # First 7 rows (ranks 16-22)
    df2_part2 = df2.iloc[7:14]   # Next 7 rows (ranks 23-29)
    df2_part3 = df2.iloc[14:21]  # Last 7 rows (ranks 30-36)
    
    # Convert each DataFrame to dictionary
    coins_part1 = df2_part1.to_dict(orient='records')
    coins_part2 = df2_part2.to_dict(orient='records')
    coins_part3 = df2_part3.to_dict(orient='records')
    
    return render_template('2.html', 
                         coins1=coins_part1, 
                         coins2=coins_part2, 
                         coins3=coins_part3)

@app.teardown_appcontext
def shutdown_session(exception=None):
    """Dispose of the database connection after each request."""
    gcp_engine.dispose()

if __name__ == '__main__':
    app.run(debug=True)
