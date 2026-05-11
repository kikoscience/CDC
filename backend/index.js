const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const sql = require('mssql');
const cors = require('cors');

require('dotenv').config();

const app = express();
app.use(cors());
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"] }
});

const config = {
  user: process.env.DB_USER || 'sa',
  password: process.env.DB_PASSWORD || 'Rfx14.14w.',
  server: process.env.DB_SERVER || process.env.DB_HOST || '10.0.1.90',
  port: parseInt(process.env.DB_PORT) || 1433,
  database: process.env.DB_DATABASE || 'hospital',
  options: {
    instanceName: process.env.DB_INSTANCE,
    encrypt: false,
    trustServerCertificate: true,
    connectTimeout: 30000
  }
};

console.log(`[CDC] Connecting to ${config.server}:${config.port}${config.options.instanceName ? '\\' + config.options.instanceName : ''}...`);

let lastLsn = null;
let eventCache = []; 

async function initializeDatabase() {
  try {
    const tempConfig = { ...config, database: 'master' };
    const pool = await sql.connect(tempConfig);
    
    console.log('[System] Checking database state...');
    
    // Create database if not exists
    await pool.request().query(`
      IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '${config.database}')
      CREATE DATABASE ${config.database};
    `);
    
    await sql.close();
    const dbPool = await sql.connect(config);
    
    // Enable CDC on DB
    await dbPool.request().query(`
      IF (SELECT is_cdc_enabled FROM sys.databases WHERE name = '${config.database}') = 0
      BEGIN
        EXEC sys.sp_cdc_enable_db;
        PRINT 'CDC Enabled on database';
      END
    `);

    // Enable CDC on ALL tables automatically
    const tablesResult = await dbPool.request().query(`
      SELECT name FROM sys.tables WHERE is_ms_shipped = 0
    `);
    
    for (const table of tablesResult.recordset) {
      const tableName = table.name;
      const captureInstance = `dbo_${tableName}`;
      
      await dbPool.request().query(`
        IF NOT EXISTS (SELECT * FROM cdc.change_tables WHERE capture_instance = '${captureInstance}')
        BEGIN
          PRINT 'Enabling CDC for table: ${tableName}';
          EXEC sys.sp_cdc_enable_table
            @source_schema = N'dbo',
            @source_name   = N'${tableName}',
            @role_name     = NULL,
            @supports_net_changes = 1;
        END
      `);
    }

    console.log('[System] Database and CDC initialized for all tables.');
  } catch (err) {
    console.error('[System] Initialization Warning:', err.message);
    // Continue anyway, maybe CDC is already enabled but we lack permissions to check
  }
}

async function pollChanges() {
  try {
    const pool = await sql.connect(config);
    
    // 1. Get current max LSN
    const maxResult = await pool.request().query('SELECT sys.fn_cdc_get_max_lsn() as max_lsn');
    const currentMaxLsn = maxResult.recordset[0].max_lsn;

    if (!currentMaxLsn) return;

    if (!lastLsn) {
      lastLsn = currentMaxLsn;
      console.log('[CDC] Dynamic Monitor Initialized at LSN:', lastLsn.toString('hex'));
      return;
    }

    if (currentMaxLsn.toString('hex') === lastLsn.toString('hex')) return;

    // 2. Identify all tables enabled for CDC
    const tablesResult = await pool.request().query('SELECT capture_instance, source_object_id FROM cdc.change_tables');
    const captureInstances = tablesResult.recordset;

    const fromLsnNext = (await pool.request()
      .input('lsn', sql.VarBinary(10), lastLsn)
      .query('SELECT sys.fn_cdc_increment_lsn(@lsn) as next_lsn')).recordset[0].next_lsn;

    for (const instance of captureInstances) {
      const instanceName = instance.capture_instance;
      
      // 3. Query changes for EACH table
      const query = `SELECT [__$operation] as op, * FROM cdc.fn_cdc_get_all_changes_${instanceName}(@from, @to, 'all')`;
      
      try {
        const result = await pool.request()
          .input('from', sql.VarBinary(10), fromLsnNext)
          .input('to', sql.VarBinary(10), currentMaxLsn)
          .query(query);

        if (result.recordset && result.recordset.length > 0) {
          console.log(`[CDC] Captured ${result.recordset.length} events from ${instanceName}`);
          
          const ops = { 1: 'DELETE', 2: 'INSERT', 3: 'UPDATE_BEFORE', 4: 'UPDATE_AFTER' };
          const newChanges = result.recordset.map(row => ({
            ...row,
            table: instanceName,
            operationType: ops[row.op] || 'UNKNOWN',
            timestamp: new Date().toISOString()
          }));
          
          eventCache = [...newChanges, ...eventCache].slice(0, 50);
          io.emit('cdc-change', newChanges);
        }
      } catch (tableErr) {
        // Silently skip if a table function fails (e.g. if it was just disabled)
      }
    }

    lastLsn = currentMaxLsn;
  } catch (err) {
    console.error('[CDC] Error:', err.message);
  } finally {
    setTimeout(pollChanges, 2000); 
  }
}

io.on('connection', (socket) => {
  console.log('[Socket] Client connected');
  socket.emit('status', { 
    connected: true, 
    lastLsn: lastLsn ? lastLsn.toString('hex') : null,
    history: eventCache 
  });
});

server.listen(3001, async () => {
  console.log(`[System] Dynamic CDC Monitor started on port 3001`);
  await initializeDatabase();
  pollChanges();
});
