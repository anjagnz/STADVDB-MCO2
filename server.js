// server.js
const express = require('express');
const mysql = require('mysql2');
const cors = require('cors');

const app = express();
const port = 3000;

app.use(express.json()); // Enable parsing of JSON request bodies
app.use(cors())
app.use(express.static('.')); // Serves static files (HTML, CSS, JS) from current directory

// Database connection configuration
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

// API endpoint to get users
app.get('/', (req, res) => {
    //res.redirect('/data');
    res.sendFile(__dirname + '/data.html');
});

// load data into page
app.get('/data', (req, res) => {
    db.query('SELECT * FROM metadata LIMIT 10;', (err, results) => {
        if (err) {
            console.error('Database query error:', err);
            // Ensure we only send one response
            if (!res.headersSent) {
                res.status(500).json({ error: 'Failed to fetch metadata' });
            }
            return; // Crucial: exit early
        }

        // Only send success response if headers not already sent
        if (!res.headersSent) {
            res.json(results);
        }
    });
});

// edit page
app.get('/api/edit', (req, res) => {
    const metadata_key = req.query.id;

    const query = 'SELECT * FROM metadata WHERE metadata_key = ?';
    db.query(query,[metadata_key], (err, results) => {
        if (err) {
            console.error('Database query error:', err);
            // Ensure we only send one response
            return res.status(500).json({ error: 'Failed to fetch metadata' }); // Crucial: exit early
        }
        res.json(results[0]); // Send single object
    });
});

// POST FOR EDIT
app.post('/api/update', (req, res) => {
    console.log("🔄 Received update request:", req.body);

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

app.listen(port, () => {
    console.log(`Server listening at http://localhost:${port}`);
});