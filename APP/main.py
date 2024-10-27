import clickhouse_connect
import requests

def fetch_crypto_data():
    """Fetch cryptocurrency data from CoinGecko API."""
    response = requests.get("https://api.coingecko.com/api/v3/coins/markets", params={
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 10,
        "page": 1,
        "sparkline": "false"
    })
    response.raise_for_status()  # Raise an error for bad responses
    return response.json()

def main():
    # Initialize the ClickHouse client
    client = clickhouse_connect.get_client(
        host='',
        user='',
        password='',
        secure=True
    )

    # 1. Check if tables exist and create them if not
    existing_tables = client.query("SHOW TABLES IN default")
    table_names = [table[0] for table in existing_tables.result_rows]

    # Create crypto_data table if it doesn't exist
    if 'crypto_data' not in table_names:
        print("Creating 'crypto_data' table in 'default' database.")
        create_crypto_table_query = """
        CREATE TABLE default.crypto_data (
            id String,
            name String,
            symbol String,
            current_price String,
            market_cap String,
            last_updated String
        ) ENGINE = MergeTree()
        ORDER BY id;
        """
        client.command(create_crypto_table_query)
        print("Table 'crypto_data' created.")
    else:
        print("'crypto_data' already exists.")

    # 2. Fetch cryptocurrency data from the API
    crypto_data = fetch_crypto_data()

    # 3. Prepare data for insertion into crypto_data
    data_to_insert = []
    for crypto in crypto_data:
        data_to_insert.append({
            "id": str(crypto['id']),
            "name": crypto['name'],
            "symbol": crypto['symbol'],
            "current_price": str(crypto['current_price']),
            "market_cap": str(crypto['market_cap']),
            # Keep last_updated as a string
            "last_updated": crypto['last_updated']
        })

    # Prepare values for crypto_data insertion
    columns = list(data_to_insert[0].keys())
    values = [[row[col] for col in columns] for row in data_to_insert]

    # 4. Insert data into crypto_data
    client.insert('default.crypto_data', values)
    print("Data inserted into 'crypto_data'.")

    # 5. Verify the inserted data from crypto_data
    print("Inserted rows from 'crypto_data':")
    result = client.query("SELECT * FROM default.crypto_data")
    for row in result.result_rows:
        print(row)


main()
