const express = require('express');
const mysql = require('mysql2');
const cors = require('cors');

const app = express();
const port = 3000;

app.use(express.json());
app.use(cors())


// database connection
const db = mysql.createConnection({
    host: 'ccscloud.dlsu.edu.ph',
    user: 'remoteuser',
    password: 'DenzelLuisAnjaNaysa2025!',
    database: 'MCO2_Distributed_Database',
    port: 60778
});

db.connect(err => {
    if (err) throw err;
    console.log('Connected to MySQL database!');
});

// landing page
app.get('/', (req, res) => {
    res.sendFile(__dirname + '/index.html');
});

// initally load data into page
app.get('/data', (req, res) => {
    const filePath = path.join(__dirname, 'data.html');
    res.sendFile(filePath, (err) => {
        if (err) {
            console.error('Failed to send data.html:', err);
            res.status(404).send('Data page not found');
        }
    });
});

// this is for "LOAD MORE"
app.get('/api/data', (req, res) => {
    let offset = parseInt(req.query.offset); // start loading from row 0
    let limit = parseInt(req.query.limit);   // amount of data to load at a time

    if (isNaN(offset) || offset < 0) offset = 0;
    if (isNaN(limit) || limit < 1 || limit > 100) limit = 20; // cap max limit

    console.log(`Fetching data: OFFSET=${offset}, LIMIT=${limit}`);

    const search = req.query.search;
    const hasSearch = search && search.trim().length > 0;

    let query, params;

    if (hasSearch) {
        // with search
        query = `
            SELECT * FROM metadata
            WHERE primary_title LIKE ?
               OR original_title LIKE ?
            ORDER BY metadata_key
            LIMIT ? OFFSET ?
        `;
        // for sql to allow partial matching
        const term = `%${search.trim()}%`;
        params = [term, term, limit, offset];
    } else {
        // without search (full table)
        query = `
            SELECT * FROM metadata
            ORDER BY metadata_key
            LIMIT ? OFFSET ?
        `;
        params = [limit, offset];
    }

    db.query(query, params, (err, results) => {
        if (err) {
            console.error('Database error:', err);
            return res.status(500).json({ error: 'Failed to fetch metadata' });
        }
        console.log(`Returned ${results.length} rows`);
        res.json(results);
    });
});

// edit page
app.get('/api/edit', (req, res) => {
    const metadata_key = req.query.id;

    const query = 'SELECT * FROM metadata WHERE metadata_key = ?';
    db.query(query,[metadata_key], (err, results) => {
        if (err) {
            console.error('Database query error:', err);
            // send one response
            return res.status(500).json({ error: 'Failed to fetch metadata' });
        }
        res.json(results[0]); // send single object
    });
});

// POST FOR EDIT
app.post('/api/update', (req, res) => {
    console.log("Received update request:", req.body);

    const {
        metadata_key,
        primary_title,
        original_title,
        title_type,
        is_adult,
        runtime_minutes,
        year
    } = req.body;

    // Validate
    if (!metadata_key) {
        return res.status(400).json({ error: 'metadata_key is required' });
    }

    const query = `
        UPDATE metadata SET
            primary_title = ?,
            original_title = ?,
            title_type = ?,
            is_adult = ?,
            runtime_minutes = ?,
            year = ?
        WHERE metadata_key = ?
    `;

    db.query(
        query,
        [primary_title, original_title, title_type, is_adult, runtime_minutes, year, metadata_key],
        (err, result) => {
            if (err) {
                console.error('Update DB error:', err);
                return res.status(500).json({ error: 'Database update failed' });
            }
            if (result.affectedRows === 0) {
                return res.status(404).json({ error: 'No record found to update' });
            }
            res.json({ success: true, message: 'Record updated' });
        }
    );
});

app.use(express.static('.')); // serves static files (HTML, CSS, JS) from current directory

app.listen(port, () => {
    console.log(`Server listening at http://localhost:${port}`);
});