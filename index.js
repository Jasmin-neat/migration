/**
 * Module dependencies.
 */

var express = require('express');
var mysql = require('mysql');
const { exec } = require('child_process');

var app = express();
app.use(express.json());

var { insert, update, remove } = require('./sync/sync');
var { migrateAll } = require('./migrate');

//Routes
app.post('/sync', function (req, res) {
    // var mgConnection = mysql.createConnection({
    //     host: 'medigle-db-dev-main-mariadb.c3kt6elpaaoa.ap-northeast-1.rds.amazonaws.com',
    //     user: 'admin',
    //     password: 'g.(?gcv*m7AT8]Jf-loolEV)rH9Q',
    //     database: 'newMedigle'
    // });

    var srcConnection = mysql.createConnection({
        host: 'localhost',
        user: 'root',
        password: '',
        database: 'medigle'
    });
    
    var dstConnection = mysql.createConnection({
        host: 'localhost',
        user: 'root',
        password: '',
        database: 'newMedigle'
    });

    var mgConnection = mysql.createConnection({
        host: 'localhost',
        user: 'root',
        password: '',
        database: 'newMedigle'
    });

    const { action, table, data } = req.body;
    switch (action) {
        case "INSERT":
            insert(res, mgConnection, table, data);
            break;
        case "UPDATE":
            update(res, srcConnection, dstConnection, mgConnection, data);
            break;
        case "DELETE":
            remove(res, mgConnection, table, data);
            break;
        case "ALL":
            migrateAll();
            break;
        case "NONE":
            return res.status(400).send({ result: "NONE", message: "No action specified" });
        default:
            return res.status(400).send({ result: "Error", message: "Invalid action" });
    }
    res.status(200).send({ result: "Success" });
});

//delete an employee

app.listen(process.env.PORT || 3000, () => {
    console.log(`process running on port ${process.env.PORT || 3000}`);
});