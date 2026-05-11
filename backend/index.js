const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const sql = require('mssql');
const cors = require('cors');

const app = express();
app.use(cors());
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"] }
});

const config = {
  user: 'sa',
  password: 'Rfx14w14w.',
  server: 'host.docker.internal',
  database: 'hospital',
  options: {
    encrypt: true,
    trustServerCertificate: true
  }
};

let lastLsn = null;
let eventCache = []; 

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

server.listen(3001, () => {
  console.log(`[System] Dynamic CDC Monitor started on port 3001`);
  pollChanges();
});
