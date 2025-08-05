-- Simple test query
SELECT 'Hello from Snowflake!' as message;

-- Check current session
SELECT CURRENT_USER(), CURRENT_ACCOUNT(), CURRENT_WAREHOUSE();