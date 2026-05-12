const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const sql = require('mssql');
const cors = require('cors');

require('dotenv').config();

const app = express();
app.use(cors());

// Healthcheck route
app.get('/', (req, res) => res.status(200).send('CDC Monitor Active'));

const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"] }
});

const config = {
  user: process.env.DB_USER || 'sa',
  password: process.env.DB_PASSWORD || 'Rfx14w.14w.',
  server: process.env.DB_SERVER || process.env.DB_HOST || '10.0.1.90',
  port: parseInt(process.env.DB_PORT) || 1433,
  database: process.env.DB_DATABASE || 'hospital',
  options: {
    instanceName: process.env.DB_INSTANCE,
    encrypt: process.env.DB_ENCRYPT === 'true',
    trustServerCertificate: true,
    connectTimeout: 30000
  }
};

console.log(`[CDC] Target DB: ${config.database} on ${config.server}`);
console.log(`[CDC] Options: Encrypt=${config.options.encrypt}, Trust=${config.options.trustServerCertificate}`);

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
    
    // FIX: Set owner to sa to resolve Error 15517
    try {
      await dbPool.request().query(`ALTER AUTHORIZATION ON DATABASE::${config.database} TO sa;`);
      console.log('[System] Database ownership fixed.');
    } catch (e) {
      // Ignore if already owned or permission denied
    }

    // Enable CDC on DB
    await dbPool.request().query(`
      IF (SELECT is_cdc_enabled FROM sys.databases WHERE name = '${config.database}') = 0
      BEGIN
        EXEC sys.sp_cdc_enable_db;
      END
    `);
    console.log('[System] CDC check completed.');

    // Check if SQL Server Agent is running (required for CDC)
    try {
      const agentResult = await dbPool.request().query(`
        SELECT status_desc FROM sys.dm_server_services WHERE name LIKE 'SQL Server Agent%';
      `);
      if (agentResult.recordset.length > 0 && agentResult.recordset[0].status_desc !== 'Running') {
        console.warn(`[System] WARNING: SQL Server Agent is ${agentResult.recordset[0].status_desc}. CDC will not capture new changes until it is started.`);
      } else if (agentResult.recordset.length === 0) {
        console.warn('[System] WARNING: Could not determine SQL Server Agent status. Ensure it is running.');
      }
    } catch (e) {
      // sys.dm_server_services might not be accessible depending on permissions
    }

    // Enable CDC on ALL tables automatically
    const tablesResult = await dbPool.request().query(`
      SELECT s.name AS schema_name, t.name AS table_name 
      FROM sys.tables t
      JOIN sys.schemas s ON t.schema_id = s.schema_id
      WHERE t.is_ms_shipped = 0
    `);
    
    for (const table of tablesResult.recordset) {
      const { schema_name, table_name } = table;
      const captureInstance = `${schema_name}_${table_name}`;
      
      try {
        await dbPool.request().query(`
          IF NOT EXISTS (SELECT * FROM cdc.change_tables WHERE capture_instance = '${captureInstance}')
          BEGIN
            PRINT 'Enabling CDC for table: ${schema_name}.${table_name}';
            EXEC sys.sp_cdc_enable_table
              @source_schema = N'${schema_name}',
              @source_name   = N'${table_name}',
              @role_name     = NULL,
              @supports_net_changes = 1;
          END
        `);
      } catch (tableErr) {
        console.warn(`[System] Warning: Could not enable CDC for ${schema_name}.${table_name}:`, tableErr.message);
      }
    }

    console.log('[System] Database and CDC initialization phase complete.');
  } catch (err) {
    console.error('[System] CRITICAL Initialization Error:', err.message);
    if (err.message.includes('Login failed')) {
      console.error('[System] Recommendation: Check DB_USER and DB_PASSWORD.');
    } else if (err.message.includes('getaddrinfo')) {
      console.error('[System] Recommendation: Check DB_HOST. Is it reachable from inside the container?');
    }
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
      
      // 3. Query changes for EACH table using brackets for the function name
      const query = `SELECT [__$operation] as op, * FROM [cdc].[fn_cdc_get_all_changes_${instanceName}](@from, @to, 'all')`;
      
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
            table: instanceName.replace('_', '.'), // Convert dbo_Users to dbo.Users
            operationType: ops[row.op] || 'UNKNOWN',
            timestamp: new Date().toISOString()
          }));
          
          eventCache = [...newChanges, ...eventCache].slice(0, 50);
          io.emit('cdc-change', newChanges);
        }
      } catch (tableErr) {
        // Silently skip if a table function fails
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
