#!/usr/bin/env python3
"""
Direct Snowflake connection test - bypassing MCP
"""
import snowflake.connector
import os
import getpass

def test_snowflake_connection():
    try:
        # Get password from environment or prompt user
        password = os.getenv('SNOWFLAKE_PASSWORD')
        if not password:
            password = getpass.getpass("Enter Snowflake password: ")
        
        # Your configuration from the setup
        conn = snowflake.connector.connect(
            account=os.getenv('SNOWFLAKE_ACCOUNT', 'DOORDASH'),
            user=os.getenv('SNOWFLAKE_USER', 'JIAN.LOU'),
            password=password,
            warehouse='AD_ANALYTICS_SERVICE',
            database='PRODDB',
            schema='JIANLOU',
            role='JIANLOU'
        )
        
        print("✅ Connected to Snowflake successfully!")
        
        # Test basic query
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_USER(), CURRENT_ACCOUNT(), CURRENT_WAREHOUSE()")
        result = cursor.fetchone()
        
        print(f"Current User: {result[0]}")
        print(f"Current Account: {result[1]}")  
        print(f"Current Warehouse: {result[2]}")
        
        # Test the DESCRIBE query you wanted
        print("\n🔍 Testing bid_event_ice table schema...")
        cursor.execute("DESCRIBE TABLE iguazu.server_events_production.bid_event_ice")
        schema = cursor.fetchall()
        
        print("Schema of bid_event_ice table:")
        for row in schema[:10]:  # Show first 10 columns
            print(f"  {row[0]}: {row[1]} ({row[2]})")
        
        if len(schema) > 10:
            print(f"  ... and {len(schema) - 10} more columns")
            
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        print("\nTroubleshooting:")
        print("1. Check your password")
        print("2. Verify you have access to the warehouse")
        print("3. Confirm your role permissions")

if __name__ == "__main__":
    test_snowflake_connection()