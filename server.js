const express = require('express');
const mysql = require('mysql2/promise');
const cors = require('cors');
const path = require('path');

const app = express();
const port = 3000;

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

const currentNode = "OldSlave"; // can be NewSlave or OldSlave too

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

    const yearInt = parseInt(year); // needed to determine correct slave for replication

    const targetSlave = (yearInt < 2012) ? 'OldSlave' : 'NewSlave';

    // try master first to get the metadata_key, then the target slave
    const targetNodes = ['Master', targetSlave];

    let successCount = 0;
    let generatedId = null;

    for (const target of targetNodes) {
        try {
            console.log(`Creating record in ${target}...`);
            let query, currentValues;

            if (target === 'Master') {
                query = `INSERT INTO metadata${tableSuffix[target]} (tconst, primary_title, original_title, title_type, is_adult, runtime_minutes, year) VALUES (?, ?, ?, ?, ?, ?, ?)`;
                
                const [result] = await pools['Master'].query(query, values);

                // get the metadata_key generated by master
                generatedId = result.insertId;
                console.log(`Master generated ID: ${generatedId}`);
            } else {
                if (!generatedId) {
                    // the master failed, so query both slaves as a basis to create a unique metadata_key 
                    console.warn("Master is down! Generating ID for metdata_key from Slaves...");
                    
                    // get the maximum metadata_key and add 1 to generate a new metadata_key if master node is down
                    try {
                        const [oldRes] = await pools['OldSlave'].query(`SELECT MAX(metadata_key) as maxId FROM metadata${tableSuffix['OldSlave']}`);
                        const [newRes] = await pools['NewSlave'].query(`SELECT MAX(metadata_key) as maxId FROM metadata${tableSuffix['NewSlave']}`);
                        
                        const maxOld = oldRes[0].maxId;
                        const maxNew = newRes[0].maxId;
                        
                        // create a new ID that is higher than both all metadata_keys in OldSlave and NewSlave to prevent duplicate metadata_key
                        generatedId = Math.max(maxOld, maxNew) + 1;
                        console.log(`Generated Fallback ID: ${generatedId}`);
                        
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

                await pools[target].query(query, currentValues);
            }

            successCount++;

        } catch (err) {
            console.error(`Database insert error on ${target}:`, err);
            /* 
                TO DO: Logs for recovery in case an insert fails during current iteration
            */
        }
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

    //always update Master, and always write to the Destination Slave
    const targetNodes = ['Master', destSlave]; 

    // if row is moving from one slave to another, delete from the old slave before the update/insert loop
    if (isMigration) {
        console.log(`Migration detected: Moving from ${sourceSlave} to ${destSlave}`);
        try {
            console.log(`Deleting from old node ${sourceSlave}...`);
            const delQuery = `DELETE FROM metadata${tableSuffix[sourceSlave]} WHERE metadata_key = ?`;
            await pools[sourceSlave].query(delQuery, [metadata_key]);
        } catch (err) {
            console.error(`Deletion failed on ${sourceSlave}:`, err);
            // Log for recovery
        }
    }

    let successCount = 0;

    // params for simple update
    const updateParams = [primary_title, original_title, title_type, is_adult, runtime_minutes, year, metadata_key];

    // params in case a row needs to be moved from one slave to another
    const insertParams = [metadata_key, tconst, primary_title, original_title, title_type, is_adult, runtime_minutes, year];

    for (const target of targetNodes) {
        try {
            let query;
            let params;

            if (isMigration && target !== 'Master') {
                // current target node is the slave where the row is migrating to, so perform INSERT
                console.log(`Migrating (Inserting) into ${target}...`);
                query = `INSERT INTO metadata${tableSuffix[target]} 
                        (metadata_key, tconst, primary_title, original_title, title_type, is_adult, runtime_minutes, year) 
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)`;
                params = insertParams;
            } else { 
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
            }
            await pools[target].query(query, params);
            successCount++;

        } catch (err) {
            console.error(`Update DB error on ${target}:`, err);
            /* 
                TO DO: Logs for recovery in case an update fails during current iteration
            */
        }
    }

    // as long as one node has successfully UPDATEd the new row, we assume there is a log that means the UPDATE can be replicated in the other node
    if (successCount > 0) {
        res.json({ success: true, message: `Records updated in ${targetNodes.join(', ')}`});
    } else {
        return res.status(500).json({ error: `Failed to update records in ${targetNodes.join(', ')}` });
    }
});

app.use(express.static('.')); // serves static files (HTML, CSS, JS) from current directory

app.listen(port, () => {
    console.log(`Server listening at http://localhost:${port}`);
});