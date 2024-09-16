const { migrateConnection } = require('./connection');
exports.migrateContracts = function(srcConnection, dstConnection, mgConnection) {
    const queryStart = Date.now();

    dstConnection.query("DROP TABLE IF EXISTS contracts", function (error, result, fields) {
        if (error) {
            throw error;
        }
    });
    let tableCreateQuery = "CREATE TABLE contracts (";
    tableCreateQuery += "id int(11) NOT NULL AUTO_INCREMENT";
    tableCreateQuery += ", facility_id int(11)";
    tableCreateQuery += ", epark_id int(11)";
    tableCreateQuery += ", contract_plan int(1)";
    tableCreateQuery += ", contract_status tinyint(1)";
    tableCreateQuery += ", contract_start_date datetime DEFAULT NULL";
    tableCreateQuery += ", contract_end_date datetime DEFAULT NULL";
    tableCreateQuery += ", contract_contact_name varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL";
    tableCreateQuery += ", contract_contact_department varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL";
    tableCreateQuery += ", contract_contact_tel varchar(15)";
    tableCreateQuery += ", contract_contact_fax varchar(15)";
    tableCreateQuery += ", update_by int(11)";
    tableCreateQuery += ", created_at date";
    tableCreateQuery += ", updated_at datetime";
    tableCreateQuery += ", PRIMARY KEY (`id`) USING BTREE"
    tableCreateQuery += ") ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci ROW_FORMAT=DYNAMIC";
    dstConnection.query(tableCreateQuery, function (error, result, fields) {
        if (error) {
            throw error;
        }
    });

    var query = srcConnection.query('SELECT medigle_facility.*, epark_facility.* FROM epark_facility INNER JOIN medigle_facility ON epark_facility.id = medigle_facility.epark_id');

    var progress = 0;
    query.on('result', function(row) {
        srcConnection.pause();

        var newRow = {
            facility_id: row.id,
            epark_id: row.epark_id,
            contract_plan: row.plan,
            contract_status: row.status,
            contract_start_date: row.start_date,
            contract_end_date: row.end_date,
            contract_contact_name: row.change_name,
            contract_contact_department: row.change_department,
            contract_contact_tel: row.tel,
            contract_contact_fax: row.fax,
            update_by: row.update_user,
            created_at: row.reg_date,
            updated_at: row.modified
        };

        dstConnection.query("INSERT INTO contracts SET ?", newRow, function(error, result, fields) {
            if (error) throw error;
            migrateConnection(mgConnection, 'medigle_facility', 'contracts', row.id, result.insertId);
            progress = progress + 1;
            srcConnection.resume();
        });
    });
    query.on('end', function() {
        const queryEnd = Date.now();
        const queryExecutionTime = queryEnd - queryStart;
        console.log("Contract Total count:",progress);
        console.log(`Query execution time: ${(queryExecutionTime / 1000).toFixed(2)} seconds.`);
    });
}

exports.migrateSingleContract = function(srcConnection, dstConnection, mgConnection, epark_id) {
    const queryStart = Date.now();

    // Query to select a single row from the source database
    const selectQuery = `
        SELECT medigle_facility.*, epark_facility.* 
        FROM epark_facility 
        INNER JOIN medigle_facility ON epark_facility.id = medigle_facility.epark_id
        WHERE epark_facility.id = ?
    `;

    srcConnection.query(selectQuery, [epark_id], function(error, results) {
        if (error) {
            console.error('Error selecting contract:', error);
            return;
        }

        if (results.length === 0) {
            console.log(`No contract found with epark_id ${epark_id}`);
            return;
        }

        const row = results[0];

        const updatedData = {
            facility_id: row.id,
            contract_plan: row.plan,
            contract_status: row.status,
            contract_start_date: row.start_date,
            contract_end_date: row.end_date,
            contract_contact_name: row.change_name,
            contract_contact_department: row.change_department,
            contract_contact_tel: row.tel,
            contract_contact_fax: row.fax,
            update_by: row.update_user,
            updated_at: new Date()
        };

        // Update the existing row in the destination database
        const updateQuery = `
            UPDATE contracts 
            SET ? 
            WHERE epark_id = ?
        `;

        dstConnection.query(updateQuery, [updatedData, epark_id], function(error, result) {
            if (error) {
                console.error('Error updating contract:', error);
                return;
            }

            if (result.affectedRows === 0) {
                console.log(`No contract found with epark_id ${epark_id} in the destination database`);
                return;
            }

            migrateConnection(mgConnection, 'medigle_facility', 'contracts', row.id, row.id);

            const queryEnd = Date.now();
            const queryExecutionTime = queryEnd - queryStart;
            console.log(`Contract with epark_id ${epark_id} updated successfully.`);
            console.log(`Query execution time: ${(queryExecutionTime / 1000).toFixed(2)} seconds.`);
        });
    });
}

exports.getMedigleFacilityId = function(srcConnection, epark_id) {

    return new Promise((resolve, reject) => {
        const selectQuery = `
            SELECT medigle_facility.id AS medigle_id
            FROM medigle_facility
            WHERE medigle_facility.epark_id = ?
        `;

        srcConnection.query(selectQuery, [epark_id], function(error, results) {
            if (error) {
                console.error('Error selecting medigle_facility id:', error);
                reject(error);
                return;
            }

            if (results.length === 0) {
                console.log(`No medigle_facility found with epark_id ${epark_id}`);
                resolve(null);
                return;
            }

            const medigle_id = results[0].medigle_id;
            console.log(`Medigle facility id for epark_id ${epark_id} is: ${medigle_id}`);
            resolve(medigle_id);
        });
    });
}