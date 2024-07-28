import pandas as pd
import pyodbc

# Define the configuration
DATABASE_SERVER = 'projecthsa.database.windows.net'
DATABASE_NAME = 'practice'
AZURE_AD_USERNAME = 'selfupskill8@gmail.com'
APPLICATION_CLIENT_ID = '6c7eb11c-15e4-4742-b280-7e15a8bc371c'
DIRECTORY_TENANT_ID = '6a556fc5-b25a-4d58-8efd-f5475befb0a7'
CSV_FILE = r'C:\Users\ogoch\Documents\Jobs\Civil service\UKHSA\testdata.csv' 

def etl_pipeline():
    try:
        # Step 1: Read the CSV file
        df = pd.read_csv(CSV_FILE)
        
        # Step 2: Data Manipulation
        df['Total_sale'] = df['Quantity'] * df['Price']
        df.rename(columns={'ProductName': 'Product'}, inplace=True)
        
        # Step 3: Data Validation
        if df['Quantity'].isnull().any():
            raise ValueError("Quantity column contains blank values.")
        
        # Step 4: Database Connection String
        connection_string = (
            f"Driver={{ODBC Driver 17 for SQL Server}};"
            f"Server=tcp:{DATABASE_SERVER};"
            f"Database={DATABASE_NAME};"
            f"Uid={AZURE_AD_USERNAME};"
            f"Authentication=ActiveDirectoryInteractive;"
        )
        
        # Step 5: Connect to the Database and Insert Data
        with pyodbc.connect(connection_string) as conn:
            cursor = conn.cursor()
            
            # Create table if it does not exist
            cursor.execute('''
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Sales' and xtype='U')
            CREATE TABLE Sales (
                Channel NVARCHAR(50),
                Product NVARCHAR(50),
                Quantity INT,
                Price FLOAT,
                Total_sale FLOAT
            )
            ''')
            
            # Insert data into the table
            for index, row in df.iterrows():
                cursor.execute('''
                    INSERT INTO Sales (Channel, Product, Quantity, Price, Total_sale)
                    VALUES (?, ?, ?, ?, ?)
                ''', row['Channel'], row['Product'], row['Quantity'], row['Price'], row['Total_sale'])
            
            # Commit the transaction
            conn.commit()
        
        print("ETL process completed successfully.")
    
    except Exception as e:
        print(f"An error occurred: {e}")

# Run the ETL pipeline
etl_pipeline()
