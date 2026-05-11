USE hospital;
GO

-- 1. Enable CDC on the database
IF (SELECT is_cdc_enabled FROM sys.databases WHERE name = 'hospital') = 0
BEGIN
    EXEC sys.sp_cdc_enable_db;
END
GO

-- 2. Enable CDC on ALL existing tables
DECLARE @TableName NVARCHAR(255);
DECLARE @SchemaName NVARCHAR(255) = 'dbo';

DECLARE table_cursor CURSOR FOR 
SELECT name FROM sys.tables WHERE is_ms_shipped = 0;

OPEN table_cursor;
FETCH NEXT FROM table_cursor INTO @TableName;

WHILE @@FETCH_STATUS = 0
BEGIN
    DECLARE @capture_instance NVARCHAR(255) = @SchemaName + '_' + @TableName;
    
    IF NOT EXISTS (SELECT * FROM cdc.change_tables WHERE capture_instance = @capture_instance)
    BEGIN
        PRINT 'Enabling CDC for: ' + @TableName;
        EXEC sys.sp_cdc_enable_table
            @source_schema = @SchemaName,
            @source_name   = @TableName,
            @role_name     = NULL,
            @supports_net_changes = 1;
    END
    FETCH NEXT FROM table_cursor INTO @TableName;
END

CLOSE table_cursor;
DEALLOCATE table_cursor;
GO
