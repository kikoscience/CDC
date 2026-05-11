# MSSQL Change Data Capture (CDC) Command Center

A real-time monitoring dashboard for Microsoft SQL Server data mutations.

## Features
- **Real-time Streaming**: Captures INSERT, UPDATE, and DELETE operations using MSSQL CDC.
- **Command Center UI**: High-tech, dark-mode dashboard for visual data tracking.
- **Dockerized**: Easy setup with SQL Server 2022 and Node.js backend.

## Quick Start

1. **Start the environment**:
   ```bash
   docker-compose up --build
   ```

2. **Wait for initialization**:
   The SQL Server will automatically run `sql-init/init-cdc.sql` to enable CDC on the `CDC_Demo` database and `Inventory` table.

3. **Open the Dashboard**:
   Open `frontend/index.html` in your browser.

4. **Test the Capture**:
   Connect to the SQL Server (localhost:1434, sa/YourStrong!Passw0rd) and run some queries:
   ```sql
   USE CDC_Demo;
   INSERT INTO Inventory (ProductName, Quantity, Price) VALUES ('Tactical Monitor', 5, 299.99);
   UPDATE Inventory SET Quantity = 4 WHERE ProductName = 'Tactical Monitor';
   DELETE FROM Inventory WHERE ProductName = 'Tactical Monitor';
   ```
   Watch the dashboard update in real-time!

## Troubleshooting
- Ensure **SQL Server Agent** is running (the Docker image handles this by default for CDC).
- If no changes appear, verify CDC is enabled: 
  `SELECT name, is_cdc_enabled FROM sys.databases;`
