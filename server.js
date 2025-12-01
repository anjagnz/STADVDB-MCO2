const express = require('express');
const mysql = require('mysql2/promise');
const cors = require('cors');
const path = require('path');

const app = express();
//const port = 3000;

app.use(express.json());
app.use(cors())

// initialize connection to all three nodes
const MasterNode = {
    host: 'ccscloud.dlsu.edu.ph', 
    user: 'remoteuser', 
    password: 'DenzelLuisAnjaNaysa2025!', 
    database: 'MCO2_Distributed_Database', 
    port: 60778,
    enableKeepAlive: true, 
    keepAliveInitialDelay: 0 
}

const OldSlave = {
    host: 'ccscloud.dlsu.edu.ph', 
    user: 'remoteuser', 
    password: 'DenzelLuisAnjaNaysa2025!', 
    database: 'MCO2_LowerShard', 
    port: 60780,
    enableKeepAlive: true, 
    keepAliveInitialDelay: 0 
}

const NewSlave = {
    host: 'ccscloud.dlsu.edu.ph', 
    user: 'remoteuser', 
    password: 'DenzelLuisAnjaNaysa2025!', 
    database: 'MCO2_UpperShard', 
    port: 60779  ,
    enableKeepAlive: true, 
    keepAliveInitialDelay: 0 
}

// dotenv used for node testing purposes
const dotenv = require('dotenv');
dotenv.config({path: process.argv[2]});

const currentNode = process.env.NODE_NAME;
const port = parseInt(process.env.NODE_PORT);

// because Denzel named the tables differently in each node
const tableSuffix = {
    Master : "",
    OldSlave : "_lower_half",
    NewSlave : "_upper_half"
}

// connect to all nodes to be able to propagate changes from primary node
const pools = {
    Master: mysql.createPool(MasterNode),
    OldSlave: mysql.createPool(OldSlave),
    NewSlave: mysql.createPool(NewSlave)
};

// create transaction logs in the db if none exist
async function createTransactionLogTable() {
    const query = `
        CREATE TABLE IF NOT EXISTS transaction_logs (
	    log_id SERIAL PRIMARY KEY,
   	    source_node VARCHAR(50) NOT NULL,
	    operation VARCHAR(20) NOT NULL CHECK (operation IN ('INSERT', 'UPDATE', 'UPDATE_MIGRATE')),
	    target_table VARCHAR(100) NOT NULL,
	    target_key BIGINT NOT NULL,
   	    old_values JSON,
	    new_values JSON,
	    nodes_replicated_to VARCHAR(120) DEFAULT '',
	    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	    status VARCHAR(50) NOT NULL
    );`

    console.log("Ensuring transaction_logs table exists...");
    try {
        await pools[currentNode].query(query);
        console.log("transaction_logs table is ready.");
    } catch (err) {
        console.error("Failed to create transaction_logs table:", err);
    }
}

createTransactionLogTable();

// function to log transactions
async function logTransaction(sourceNode, operation, targetTable, targetKey, oldValues, newValues, targetNodes) {
    const query = `
        INSERT INTO transaction_logs
        (source_node, operation, target_table, target_key, old_values, new_values, nodes_replicated_to, status)
        VALUES (?, ?, ?, ?, ?, ?, '', 'PENDING')
        `;

    const oldJson = JSON.stringify(oldValues);
    const newJson = JSON.stringify(newValues);

    const params = [sourceNode, operation, targetTable, targetKey, oldJson, newJson];
    try {
        const [result] = await pools[sourceNode].query(query, params);

        console.log(`Logged transaction with ID: ${result.insertId}`);
        return result.insertId;
    } catch (err) {
        console.error('Failed to log transaction:', err);
        return null;
    }
}

// function to update status of transaction log after attempting replication
async function updateTransactionLog(sourceNode, logId, nodesReplicatedTo, status) {
    const query = `
        UPDATE transaction_logs
        SET nodes_replicated_to = ?, status = ?
        WHERE log_id = ?
    `;

    const nodesReplicatedToString = nodesReplicatedTo.join(',');

    try {
        await pools[sourceNode].query(query, [nodesReplicatedToString, status, logId]);
        console.log(`Updated transaction log ID: ${logId} with status: ${status}`);
    } catch (err) {
        console.error('Failed to update transaction log:', err);
    }
}

// landing page
app.get('/', (req, res) => {
    res.sendFile(__dirname + '/index.html');
});

// initally load data into page
app.get('/data', (req, res) => {
    const filePath = path.join(__dirname, 'data.html');

    try {
        res.sendFile(filePath);
    } catch (err) {
        console.error('Failed to send data.html:', err);
        res.status(404).send('Data page not found');
    }
});

// this is for "LOAD MORE"
app.get('/api/data', async (req, res) => {
    let offset = parseInt(req.query.offset); // start loading from row 0
    let limit = parseInt(req.query.limit);   // amount of data to load at a time

    if (isNaN(offset) || offset < 0) offset = 0;
    if (isNaN(limit) || limit < 1 || limit > 100) limit = 20; // cap max limit

    console.log(`Fetching data: OFFSET=${offset}, LIMIT=${limit}`);

    const search = req.query.search;
    const hasSearch = search && search.trim().length > 0;

    let query, params;
    
    const initializeQueryParams = (node) => {
        if (hasSearch) {
            const term = `%${search.trim()}%`;
            return [
                `SELECT * FROM metadata${tableSuffix[node]}
                 WHERE primary_title LIKE ?
                 OR original_title LIKE ?
                 ORDER BY metadata_key
                 LIMIT ? OFFSET ?`,
                [term, term, limit, offset]
            ];
        } else {
            return [
                `SELECT * FROM metadata${tableSuffix[node]}
                 ORDER BY metadata_key
                 LIMIT ? OFFSET ?`,
                [limit, offset]
            ];
        }
    }

    try {
        [query, params] = initializeQueryParams('Master');
        const [results] = await pools['Master'].query(query, params);
        console.log(`Returned ${results.length} rows from Master`);
        res.json(results);
    } catch (masterErr) { // master is down
        console.error('Database error:', masterErr);
        console.log('Master unreachable. Reconstructing complete DB from OldSlave and NewSlave');

        try {
            // run the same query on BOTH slaves
            [query, params] = initializeQueryParams('OldSlave');
            const slave1Promise = pools['OldSlave'].query(query, params).catch(e => [[], []]);

            [query, params] = initializeQueryParams('NewSlave');
            const slave2Promise = pools['NewSlave'].query(query, params).catch(e => [[], []]);

            const [[res1], [res2]] = await Promise.all([slave1Promise, slave2Promise]);

            let combinedResults = [...res1, ...res2]; // put results from both slaves together

            // sort combinedResults by the metadata_key
            combinedResults.sort((a, b) => a.metadata_key - b.metadata_key);

            // re-apply limit on number of rows because it may have been exceeded when combining queries of two slaves
            const finalResult = combinedResults.slice(0, limit);

            res.json(finalResult);
        } catch (clusterErr) {
            console.error("Cluster failure:", clusterErr);
            res.status(500).json({ error: 'System unavailable.' });
        }

        //return res.status(500).json({ error: 'Failed to fetch metadata' });
    }
});

// create
app.post('/api/create', async (req, res) => {
    const {tconst, primary_title, original_title, title_type, is_adult, runtime_minutes, year} = req.body;

    const values = [tconst, primary_title, original_title, title_type, is_adult, runtime_minutes, year];
    const newValues = {tconst, primary_title, original_title, title_type, is_adult, runtime_minutes, year};

    const yearInt = parseInt(year); // needed to determine correct slave for replication

    const targetSlave = (yearInt < 2012) ? 'OldSlave' : 'NewSlave';

    // try master first to get the metadata_key, then the target slave
    const targetNodes = ['Master', targetSlave];

    const lockedConnections = new Map(); // acquire locks on target nodes

    try {
        // exclusive table locks on all target nodes
        for (const node of targetNodes) {
            const conn = await pools[node].getConnection();
            await conn.query('START TRANSACTION');
            
            // exclusive table lock here
            await conn.query(`LOCK TABLES metadata${tableSuffix[node]} WRITE`);
            
            lockedConnections.set(node, conn);
            console.log(`Acquired EXCLUSIVE TABLE lock on ${node}`);
        }
    } catch (lockErr) {
        console.error('Failed to acquire table locks:', lockErr);
        // release any acquired locks
        for (const [node, conn] of lockedConnections) {
            try {
                await conn.query('UNLOCK TABLES');
                await conn.query('ROLLBACK');
            } catch (e) {}
            conn.release();
        }
        return res.status(500).json({ error: 'Failed to acquire table locks' });
    }
    // REMOVE LATER: 
    await new Promise(resolve => setTimeout(resolve, 5000)); // 5 second delay for testing, to observe waiting for lock to release

    let successCount = 0;
    let generatedId = null;
    let logId = null;
    let successfulNodes = [];

    try{
        for (const target of targetNodes) {
            try {
                console.log(`Creating record in ${target}...`);
                let query, currentValues;

                const conn = lockedConnections.get(target);

                if (target === 'Master') {
                    query = `INSERT INTO metadata${tableSuffix[target]} (tconst, primary_title, original_title, title_type, is_adult, runtime_minutes, year) VALUES (?, ?, ?, ?, ?, ?, ?)`;
                    
                    const [result] = await conn.query(query, values);

                    // get the metadata_key generated by master
                    generatedId = result.insertId;
                    console.log(`Master generated ID: ${generatedId}`);

                    // log the transaction before insertion into slaves
                    logId = await logTransaction(currentNode, 'INSERT', `metadata`, generatedId, null, newValues, targetNodes);

                    successfulNodes.push(target);
                } else {
                    if (!generatedId) {
                        // the master failed, so query both slaves as a basis to create a unique metadata_key 
                        console.warn("Master is down! Generating ID for metadata_key from Slaves...");
                        
                        // get the maximum metadata_key and add 1 to generate a new metadata_key if master node is down
                        try {
                            const oldConn = lockedConnections.get('OldSlave') || await pools['OldSlave'].query(`SELECT MAX(metadata_key) as maxId FROM metadata${tableSuffix['OldSlave']}`);
                            const newConn = lockedConnections.get('NewSlave') || await pools['NewSlave'].query(`SELECT MAX(metadata_key) as maxId FROM metadata${tableSuffix['NewSlave']}`);
                            
                            const [oldRes] = await oldConn.query(`SELECT MAX(metadata_key) as maxId FROM metadata${tableSuffix['OldSlave']}`);
                            const [newRes] = await newConn.query(`SELECT MAX(metadata_key) as maxId FROM metadata${tableSuffix['NewSlave']}`);
                            
                            // release temporary connections if we created them
                            if (!lockedConnections.has('OldSlave')) {
                                oldConn.release();
                            }
                            if (!lockedConnections.has('NewSlave')) {
                                newConn.release();
                            }
                            
                            const maxOld = oldRes[0].maxId;
                            const maxNew = newRes[0].maxId;
                            
                            // create a new ID that is higher than both all metadata_keys in OldSlave and NewSlave to prevent duplicate metadata_key
                            generatedId = Math.max(maxOld, maxNew) + 1;
                            console.log(`Generated Fallback ID: ${generatedId}`);
                            
                            logId = await logTransaction(currentNode, 'INSERT', `metadata`, generatedId, null, newValues, targetNodes);
                        } catch (idErr) {
                            console.error("CRITICAL: One slave is also down! Could not query Slaves for Max ID.", idErr);
                            throw new Error("ID Generation failed. Cluster unavailable.");
                        }
                    }

                    query = `INSERT INTO metadata${tableSuffix[target]} 
                            (metadata_key, tconst, primary_title, original_title, title_type, is_adult, runtime_minutes, year) 
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)`; // Explicit ID insert
                    
                    // add the generated id from the master's metadata_key (or from taking the MAX(metadata_key) of both slaves)
                    currentValues = [generatedId, ...values];

                    await conn.query(query, currentValues);

                    successfulNodes.push(target);
                }

                successCount++;

            } catch (err) {
                console.error(`Database insert error on ${target}:`, err);
                throw err;
            }
        }
        
        // commit transaction & release locks
        for (const [node, conn] of lockedConnections) {
                await conn.query('COMMIT');
                await conn.query('UNLOCK TABLES'); // Explicit unlock
                conn.release();
                console.log(`Released table lock on ${node}`);
        } 
    
    } catch (err) {
        console.error('Create error:', err);
        for (const [node, conn] of lockedConnections) {
            try {
                await conn.query('ROLLBACK');
                await conn.query('UNLOCK TABLES');
            } catch (e) {}
            conn.release();
        }
        return res.status(500).json({ error: 'Create operation failed' });
    }
    
    // update transaction log with success/failure status
    if (logId) {
        const status = (successCount === targetNodes.length) ? 'SUCCESS' : 'FAILED';
        await updateTransactionLog(currentNode, logId, successfulNodes, status);
    }

    // as long as one node has successfully INSERTed the new row, we assume there is a log that means the INSERT can be replicated in the other node
    if (successCount > 0) {
        res.json({ success: true, message: `Records created in ${targetNodes.join(', ')}`});
    } else {
        return res.status(500).json({ error: `Failed to create metadata in ${targetNodes.join(', ')}` });
    }
});

// edit page
app.get('/api/edit', async (req, res) => {
    const metadata_key = req.query.id;

    let query;
    try {
        // current node read should never fail
        query = `SELECT * FROM metadata${tableSuffix[currentNode]} WHERE metadata_key = ?`;

        const [localResults] = await pools[currentNode].query(query, [metadata_key])
        if (localResults.length > 0) {
            return res.json(localResults[0]);
        } else if (currentNode === 'Master') {
            // Master is online but row is not in master
            return res.status(404).json({ error: 'Record not found in Master' });
        }

        try { // slave did not contain row, so check master first because it should contain all rows
            query = `SELECT * FROM metadata${tableSuffix['Master']} WHERE metadata_key = ?`;

            const [masterResults] = await pools['Master'].query(query, [metadata_key])
            if (masterResults.length > 0) {
                return res.json(masterResults[0]);
            } else {
                // Master is online but row is not in master
                return res.status(404).json({ error: 'Record not found in Master' });
            }

        } catch (masterErr) { // master might be down, so catch error
            console.error("Master unreachable. Attempting to read other slave: ", masterErr.message);

            try {
                const otherSlave = (currentNode === 'OldSlave') ? 'NewSlave' : 'OldSlave';
                query = `SELECT * FROM metadata${tableSuffix[otherSlave]} WHERE metadata_key = ?`;
                
                const [otherResults] = await pools[otherSlave].query(query, [metadata_key]);
                if (otherResults.length > 0) {
                    return res.json(otherResults[0]);
                }

            } catch (otherErr) {
                console.error("Other Slave also unreachable.");
            }     
        }

        return res.status(404).json({ error: 'Record not found in cluster' });

    } catch (err) { // current slave node does not contain row and all other nodes are down, thus row can not be read
        console.error('Critical database query error:', err);
        // send one response
        return res.status(500).json({ error: 'System unavailable' });
    }
});

// POST FOR EDIT
app.post('/api/update', async (req, res) => {
    console.log("Received update request:", req.body);

    const {
        metadata_key,
        primary_title,
        original_title,
        title_type,
        is_adult,
        runtime_minutes,
        year,
        tconst,
        oldYear
    } = req.body;

    // Validate
    if (!metadata_key) {
        return res.status(400).json({ error: 'metadata_key is required' });
    }

    const getShard = (y) => (parseInt(y) < 2012) ? 'OldSlave' : 'NewSlave';
    
    // determine currentYear and newYear provided by the user
    const currentYear = parseInt(oldYear);
    const newYear = parseInt(year);

    const sourceSlave = getShard(currentYear); // where row is currently stored
    const destSlave = getShard(newYear);  // where new values of the row should be reflected   
    
    const isMigration = (sourceSlave !== destSlave); // if true, the row is moving from one slave to another

    const sourceNodes = ['Master', sourceSlave]; // nodes that have the row
    const lockedConnections = new Map(); // acquire locks on source nodes
    try {
        for (const node of sourceNodes) {
            try {
                const conn = await pools[node].getConnection();
                await conn.query('START TRANSACTION');
                
                // row lock here
                const [rows] = await conn.query(
                    `SELECT metadata_key FROM metadata${tableSuffix[node]} WHERE metadata_key = ? FOR UPDATE`,
                    [metadata_key]
                );
                
                if (rows.length === 0) {
                    await conn.query('ROLLBACK');
                    conn.release();
                    throw new Error(`Row ${metadata_key} not found on ${node}`);
                }
                
                lockedConnections.set(node, conn);
                console.log(`Acquired EXCLUSIVE lock on ${node} for ROW ${metadata_key}`);
                // REMOVE LATER:
                await new Promise(resolve => setTimeout(resolve, 5000)); // 5 second delay for testing, to observe waiting for lock to release
            } catch (lockErr) {
                console.error(`Failed to lock ${node}:`, lockErr.message);
                // release locks
                for (const [lockedNode, conn] of lockedConnections) {
                    await conn.query('ROLLBACK');
                    conn.release();
                }
                return res.status(404).json({ error: `Row not found or locked: ${lockErr.message}` });
            }
        }
    } catch (err) {
        return res.status(500).json({ error: 'Lock acquisition failed' });
    }

    //always update Master, and always write to the Destination Slave
    let targetNodes = ['Master', destSlave]; 
    let logId = null;
    let successCount = 0;
    let successfulNodes = [];
    const newValues = {metadata_key, tconst, primary_title, original_title, title_type, is_adult, runtime_minutes, year};

    // log transaction before performing updates
    logId = await logTransaction(currentNode, isMigration ? 'UPDATE_MIGRATE' : 'UPDATE', `metadata`, metadata_key, null, newValues, targetNodes);

    // if row is moving from one slave to another, delete from the old slave before the update/insert loop
    if (isMigration) {
        console.log(`Migration detected: Moving from ${sourceSlave} to ${destSlave}`);
        targetNodes = ['Master', 'OldSlave', 'NewSlave'];
    }

    // params for simple update
    const updateParams = [primary_title, original_title, title_type, is_adult, runtime_minutes, year, metadata_key];

    // params in case a row needs to be moved from one slave to another
    const insertParams = [metadata_key, tconst, primary_title, original_title, title_type, is_adult, runtime_minutes, year];

    for (const target of targetNodes) {
        try {
            let query;
            let params;

            if (isMigration && target === destSlave) {
                // current target node is the slave where the row is migrating to, so perform INSERT
                console.log(`Migrating (Inserting) into ${target}...`);
                query = `INSERT INTO metadata${tableSuffix[target]} 
                        (metadata_key, tconst, primary_title, original_title, title_type, is_adult, runtime_minutes, year) 
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)`;
                params = insertParams;
            } else if (!isMigration || target === 'Master') { 
                // not the slave the row is migrating to, so UPDATE as normal
                console.log(`Updating ${target}...`);
                query = `UPDATE metadata${tableSuffix[target]} 
                            SET primary_title = ?, 
                            original_title = ?, 
                            title_type = ?, 
                            is_adult = ?, 
                            runtime_minutes = ?, 
                            year = ? 
                            WHERE metadata_key = ?`;
                params = updateParams;
            } else {
                // node whose row needs to be deleted in migration
                console.log(`Deleting from old node ${sourceSlave}...`);
                query = `DELETE FROM metadata${tableSuffix[sourceSlave]} WHERE metadata_key = ?`;
                params = [metadata_key];
            }

            if (lockedConnections.has(target)) {
                await lockedConnections.get(target).query(query, params);
            } else {
                await pools[target].query(query, params);
            }

            successfulNodes.push(target);
            successCount++;

        } catch (err) {
            console.error(`Update DB error on ${target}:`, err);
        }
    }

    try {
        for (const [node, conn] of lockedConnections) {
            await conn.query('COMMIT');
            conn.release();
            console.log(`Released lock on ${node}`);
        }
    } catch (commitErr) {
        console.error('Commit error:', commitErr);
        // try to release connections
        for (const conn of lockedConnections.values()) {
            conn.release();
        }
    }

    // update transaction log with success/failure status
    if (logId) {
        const status = (successCount === targetNodes.length) ? 'SUCCESS' : 'FAILED';
        await updateTransactionLog(currentNode, logId, successfulNodes, status);
    }

    // as long as one node has successfully UPDATEd the new row, we assume there is a log that means the UPDATE can be replicated in the other node
    if (successCount > 0) {
        res.json({ success: true, message: `Records updated in ${targetNodes.join(', ')}`});
    } else {
        return res.status(500).json({ error: `Failed to update records in ${targetNodes.join(', ')}` });
    }
});

// recovery endpoint to reapply failed/pending transactions
app.post('/api/recover', async (req, res) => {
    try {
        console.log("Starting recovery process...");

        const [failedLogs] = await pools[currentNode].query(`
            SELECT * FROM transaction_logs
            WHERE status IN ('PENDING', 'FAILED')
            ORDER BY timestamp ASC
        `);

        console.log(`Found ${failedLogs.length} transactions to recover.`);

        const recovered = [];
        const failed = [];

        for(const log of failedLogs) {
            try {
                const newValues = log.new_values ? log.new_values : null;

                console.log(`Recovering log ID: ${log.log_id}, with operation: ${log.operation}`);

                let targetNodes = [];

                // determine target nodes based on the year
                if (log.operation === 'UPDATE_MIGRATE') {
                    targetNodes = ['Master', 'OldSlave', 'NewSlave'];
                } else if (newValues && newValues.year) {
                    const yearInt = parseInt(newValues.year);
                    const targetSlave = (yearInt < 2012) ? 'OldSlave' : 'NewSlave';
                    targetNodes = ['Master', targetSlave];
                }

                let successCount = 0;
                let successfulNodes = [];

                const insertParams = [log.target_key, newValues.tconst, newValues.primary_title, newValues.original_title, newValues.title_type, newValues.is_adult, newValues.runtime_minutes, newValues.year];
                const updateParams = [newValues.primary_title, newValues.original_title, newValues.title_type, newValues.is_adult, newValues.runtime_minutes, newValues.year, log.target_key];

                // attempt to reapply the logged operations
                for(const target of targetNodes) {
                    try {
                        if (log.operation === 'INSERT') {
                            const query = `INSERT INTO metadata${tableSuffix[target]}
                                (metadata_key, tconst, primary_title, original_title, title_type, is_adult, runtime_minutes, year)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                                ON DUPLICATE KEY UPDATE
                                tconst = VALUES(tconst),
                                primary_title = VALUES(primary_title),
                                original_title = VALUES(original_title),
                                title_type = VALUES(title_type),
                                is_adult = VALUES(is_adult),
                                runtime_minutes = VALUES(runtime_minutes),
                                year = VALUES(year)
                            `;

                            await pools[target].query(query, insertParams);

                            successfulNodes.push(target);
                            successCount++;
                        } else if (log.operation === 'UPDATE') {
                            const query = `UPDATE metadata${tableSuffix[target]}
                                SET primary_title = ?,
                                original_title = ?,
                                title_type = ?,
                                is_adult = ?,
                                runtime_minutes = ?,
                                year = ?
                                WHERE metadata_key = ?
                            `;

                            await pools[target].query(query, updateParams);

                            successfulNodes.push(target);
                            successCount++;
                        } else if (log.operation === 'UPDATE_MIGRATE') {
                            const newYear = newValues ? parseInt(newValues.year) : null;

                            // determine source and destination slaves
                            const dstSlave = (newYear < 2012) ? 'OldSlave' : 'NewSlave';
                            const srcSlave = (dstSlave === 'OldSlave') ? 'NewSlave' : 'OldSlave';
                            
                            if (target === 'Master') {
                                const query = `UPDATE metadata${tableSuffix[target]}
                                    SET primary_title = ?,
                                    original_title = ?,
                                    title_type = ?,
                                    is_adult = ?,
                                    runtime_minutes = ?,
                                    year = ?
                                    WHERE metadata_key = ?
                                `;
                                
                                await pools[target].query(query, updateParams);
                            } else if (target === dstSlave) {
                                // attempt to insert to destination slave and delete from old slave
                                const query = `INSERT INTO metadata${tableSuffix[target]}
                                    (metadata_key, tconst, primary_title, original_title, title_type, is_adult, runtime_minutes, year)
                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                                    ON DUPLICATE KEY UPDATE
                                    tconst = VALUES(tconst),
                                    primary_title = VALUES(primary_title),
                                    original_title = VALUES(original_title),
                                    title_type = VALUES(title_type),
                                    is_adult = VALUES(is_adult),
                                    runtime_minutes = VALUES(runtime_minutes),
                                    year = VALUES(year)
                                `;

                                await pools[target].query(query, insertParams);
                            } else if (target === srcSlave) {
                                const delQuery = `DELETE FROM metadata${tableSuffix[srcSlave]} WHERE metadata_key = ?`;
                                await pools[srcSlave].query(delQuery, [log.target_key]);
                            }

                            successfulNodes.push(target);
                            successCount++;
                        }

                    } catch (err) {
                        console.error(`Recovery DB error on ${target} for log ID ${log.log_id}:`, err);
                    }
                }

                const status = (successCount === targetNodes.length) ? 'SUCCESS' : 'FAILED';
                await updateTransactionLog(currentNode, log.log_id, successfulNodes, status);

                if (status === 'SUCCESS') {
                    recovered.push(log.log_id);
                } else {
                    failed.push(log.log_id);
                }
            } catch (err) {
                console.error(`Failed to recover transaction with log ID ${log.log_id}:`, err);
                failed.push(log.log_id);
            }
        }

        res.json({success: true, message: 'Recovery process completed', recovered: recovered.length, failed: failed.length, recoveredIds: recovered, failedIds: failed});
    } catch (err) {
        console.error('Recovery process failed:', err);
        res.status(500).json({ error: 'Recovery process failed' });
    }
});

// view transaction logs
app.get('/api/logs', async (req, res) => {
    try {
        const limit = parseInt(req.query.limit) || 100; // default the limit to 50 logs
        const [logs] = await pools[currentNode].query(`
            SELECT * FROM transaction_logs
            ORDER BY timestamp DESC LIMIT ?`,
            [limit]
        );
        res.json(logs);
    } catch (err) {
        console.error('Failed to fetch transaction logs:', err);
        res.status(500).json({ error: 'Failed to fetch transaction logs' });
    }
});

app.use(express.static('.')); // serves static files (HTML, CSS, JS) from current directory

app.listen(port, () => {
    console.log(`Server listening at http://localhost:${port}`);
});