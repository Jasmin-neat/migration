const { migrateConnection } = require('./connection');
exports.migrateFacility = function(srcConnection, dstConnection, mgConnection) {
    const queryStart = Date.now();

    dstConnection.query("DROP TABLE IF EXISTS facilities", function (error, result, fields) {
        if (error) {
            throw error;
        }
    });
    dstConnection.query("CREATE TABLE facilities (id int(11) NOT NULL AUTO_INCREMENT, facility_name varchar(200) DEFAULT NULL, facility_name_kana varchar(200) DEFAULT NULL, facility_zip varchar(200) DEFAULT NULL, facility_prefecture int(10) DEFAULT NULL, facility_address varchar(200) DEFAULT NULL, facility_other_address varchar(200) DEFAULT NULL, facility_tel varchar(200) DEFAULT NULL, facility_fax varchar(200) DEFAULT NULL, facility_website_url varchar(200) DEFAULT NULL, updated_by int(11) DEFAULT NULL, created_at date DEFAULT NULL, updated_at datetime DEFAULT NULL, PRIMARY KEY (`id`) USING BTREE) ENGINE=InnoDB AUTO_INCREMENT=249453 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci ROW_FORMAT=DYNAMIC"
        ,function (error, result, fields) {
            if (error) {
                throw error;
            }
        }
    );

    var query = srcConnection.query('SELECT * FROM epark_facility');

    var progress = 0;
    query.on('result', function(row) {
        srcConnection.pause();

        var newRow = {
            id: row.id,
            facility_name: row.facility_name,
            facility_name_kana: row.facility_name_kana,
            facility_zip: row.zip,
            facility_prefecture: row.epark_pref_id,
            facility_address: row.address,
            facility_other_address: row.building,
            facility_tel: row.tel,
            facility_fax: row.fax,
            facility_website_url: row.facility_url,
            updated_by: row.update_user,
            created_at: row.reg_date,
            updated_at: row.modified,
        };

        dstConnection.query("INSERT INTO facilities SET ?", newRow, function(error, result, fields) {
            progress = progress + 1;
            migrateConnection(mgConnection, 'epark_facility', 'facilities', row.id, row.id);
            srcConnection.resume();
        });
    });
    query.on('end', function() {
        const queryEnd = Date.now();
        const queryExecutionTime = queryEnd - queryStart;
        console.log("Facility Total count:",progress);
        console.log(`Query execution time: ${(queryExecutionTime / 1000).toFixed(2)} seconds.`);
    });
}

exports.migrateSingleFacility = function(srcConnection, dstConnection, mgConnection, id) {
    const queryStart = Date.now();

    // Query to select a single row from the source database
    const selectQuery = 'SELECT * FROM epark_facility WHERE id = ?';
    srcConnection.query(selectQuery, [id], function(error, results) {
        if (error) {
            console.error('Error selecting facility:', error);
            return;
        }

        if (results.length === 0) {
            console.log(`No facility found with id ${id}`);
            return;
        }

        const row = results[0];

        const newRow = {
            id: row.id,
            facility_name: row.facility_name,
            facility_name_kana: row.facility_name_kana,
            facility_zip: row.zip,
            facility_prefecture: row.epark_pref_id,
            facility_address: row.address,
            facility_other_address: row.building,
            facility_tel: row.tel,
            facility_fax: row.fax,
            facility_website_url: row.facility_url,
            updated_by: row.update_user,
            created_at: row.reg_date,
            updated_at: row.modified,
        };

        // Insert or update the row in the destination database
        const upsertQuery = `
            INSERT INTO facilities SET ?
            ON DUPLICATE KEY UPDATE
            facility_name = VALUES(facility_name),
            facility_name_kana = VALUES(facility_name_kana),
            facility_zip = VALUES(facility_zip),
            facility_prefecture = VALUES(facility_prefecture),
            facility_address = VALUES(facility_address),
            facility_other_address = VALUES(facility_other_address),
            facility_tel = VALUES(facility_tel),
            facility_fax = VALUES(facility_fax),
            facility_website_url = VALUES(facility_website_url),
            updated_by = VALUES(updated_by),
            created_at = VALUES(created_at),
            updated_at = VALUES(updated_at)
        `;

        dstConnection.query(upsertQuery, newRow, function(error, result) {
            if (error) {
                console.error('Error upserting facility:', error);
                return;
            }

            migrateConnection(mgConnection, 'epark_facility', 'facilities', row.id, row.id);

            const queryEnd = Date.now();
            const queryExecutionTime = queryEnd - queryStart;
            console.log(`Facility with id ${id} migrated successfully.`);
            console.log(`Query execution time: ${(queryExecutionTime / 1000).toFixed(2)} seconds.`);
        });
    });
}