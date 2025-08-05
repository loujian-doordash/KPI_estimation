#!/usr/bin/env python3
"""
Test Snowflake connection and get current user info
"""

import snowflake.connector
import os

# Based on your blending reference files, these seem to be the connection params
# You may need to update these based on your actual credentials

def get_password():
    """Get password from environment variable or prompt user"""
    password = os.getenv('SNOWFLAKE_PASSWORD')
    if password:
        return password
    
    # If no env var, prompt user (more secure than hardcoding)
    import getpass
    password = getpass.getpass("Enter Snowflake password: ")
    return password

def test_connection():
    try:
        # Get credentials securely
        password = get_password()
        if not password:
            print("❌ No password provided")
            return
            
        # Connection parameters from environment variables
        conn = snowflake.connector.connect(
            account=os.getenv('SNOWFLAKE_ACCOUNT', 'DOORDASH'),
            user=os.getenv('SNOWFLAKE_USER', 'JIAN.LOU'),
            password=password,
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE', 'ADHOC_ETL'),
            database=os.getenv('SNOWFLAKE_DATABASE', 'PRODDB'),
            schema=os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC')
        )
        
        # Get current user info
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_USER(), CURRENT_ACCOUNT(), CURRENT_WAREHOUSE()")
        result = cursor.fetchone()
        
        print(f"✅ Connection successful!")
        print(f"Current User: {result[0]}")
        print(f"Current Account: {result[1]}")
        print(f"Current Warehouse: {result[2]}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        print("\nTry these common username formats:")
        print("- JIAN.LOU")
        print("- jian.lou") 
        print("- jian.lou@doordash.com")

def run_sql_file(sql_file_path):
    """Run SQL queries from a file"""
    try:
        password = get_password()
        if not password:
            print("❌ No password provided")
            return
            
        conn = snowflake.connector.connect(
            account=os.getenv('SNOWFLAKE_ACCOUNT', 'DOORDASH'),
            user=os.getenv('SNOWFLAKE_USER', 'JIAN.LOU'),
            password=password,
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE', 'ADHOC_ETL'),
            database=os.getenv('SNOWFLAKE_DATABASE', 'PRODDB'),
            schema=os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC')
        )
        
        # Read SQL file
        with open(sql_file_path, 'r') as file:
            sql_content = file.read()
        
        # Split queries by semicolon and execute
        queries = [q.strip() for q in sql_content.split(';') if q.strip() and not q.strip().startswith('--')]
        
        cursor = conn.cursor()
        for i, query in enumerate(queries, 1):
            if query:
                print(f"\n--- Query {i} ---")
                print(f"Executing: {query[:100]}...")
                try:
                    cursor.execute(query)
                    results = cursor.fetchall()
                    columns = [desc[0] for desc in cursor.description] if cursor.description else []
                    
                    if results:
                        print(f"✅ Success! {len(results)} rows returned")
                        if columns:
                            print(f"Columns: {', '.join(columns)}")
                        # Show first few results
                        for row in results[:5]:
                            print(row)
                    else:
                        print("✅ Query executed successfully (no results)")
                except Exception as e:
                    print(f"❌ Error: {e}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        # Run SQL file if provided as argument
        sql_file = sys.argv[1]
        print(f"🔍 Running SQL file: {sql_file}")
        run_sql_file(sql_file)
    else:
        # Default: test connection
        test_connection()