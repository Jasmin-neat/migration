const { migrateConnection } = require('./connection');
const { getMedigleFacilityId } = require('./contract');
exports.migrateUsers = function(srcConnection, dstConnection, mgConnection) {
    const queryStart = Date.now();
    
    dstConnection.query("DROP TABLE IF EXISTS users", function (error, result, fields) {
        if (error) {
            throw error;
        }
    });
    let tableCreateQuery = "CREATE TABLE users (";
    tableCreateQuery += "id int(11) NOT NULL AUTO_INCREMENT";
    tableCreateQuery += ", facility_id int(11)";
    tableCreateQuery += ", user_id varchar(255)";
    tableCreateQuery += ", user_password_hash varchar(255)";
    tableCreateQuery += ", user_name varchar(255)";
    tableCreateQuery += ", user_role tinyint(4) DEFAULT 0";
    tableCreateQuery += ", is_active int(1) DEFAULT 0";
    tableCreateQuery += ", is_locked int(11) DEFAULT 0";
    tableCreateQuery += ", updated_by int(11) DEFAULT NULL";
    tableCreateQuery += ", created_at datetime";
    tableCreateQuery += ", updated_at datetime";
    tableCreateQuery += ", deleted_at datetime";
    tableCreateQuery += ", PRIMARY KEY (`id`) USING BTREE"
    tableCreateQuery += ") ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci ROW_FORMAT=DYNAMIC";
    dstConnection.query(tableCreateQuery, function (error, result, fields) {
        if (error) {
            throw error;
        }
    });

    var query = srcConnection.query('SELECT * from user');

    var progress = 0;
    query.on('result', function(row) {
        srcConnection.pause();

        // Check if the user has already been migrated
        dstConnection.query("SELECT id FROM users WHERE facility_id = ? AND user_id = ?", [row.medigle_id, row.user_name], function(error, results) {
            if (error) {
                console.error('Error checking existing user:', error);
                srcConnection.resume();
                return;
            }

            if (results.length > 0) {
                // User already exists, skip migration
                srcConnection.resume();
                return;
            }

            var newRow = {
                facility_id: row.medigle_id,
                user_id: row.user_name,
                user_password_hash: row.password,
                user_name: row.name,
                user_role: row.manage,
                is_active: row.use,
                is_locked: row.failed_count,
                updated_by: row.update_user,
                created_at: row.created,
                updated_at: row.modified
            };

            dstConnection.query("INSERT INTO users SET ?", newRow, function(error, result, fields) {
                if (error) {
                    console.error('Error inserting user:', error);
                } else {
                    migrateConnection(mgConnection, 'user', 'users', row.id, result.insertId);
                    progress++;
                }
                srcConnection.resume();
            });
        });
    });
    query.on('end', function() {
        const queryEnd = Date.now();
        const queryExecutionTime = queryEnd - queryStart;
        console.log("Users Total count:", progress);
        console.log(`Query execution time: ${(queryExecutionTime / 1000).toFixed(2)} seconds.`);
    });
}

exports.updateUsersByFacilityId = async function(srcConnection, dstConnection, mgConnection, epark_id) {
    const queryStart = Date.now();

    try {
        const medigle_id = await getMedigleFacilityId(srcConnection, epark_id);
        
        if (!medigle_id) {
            console.log(`No facility found with epark_id ${epark_id}`);
            return;
        }

        // First, delete all existing users for this facility
        const deleteQuery = 'DELETE FROM users WHERE facility_id = ?';
        await new Promise((resolve, reject) => {
            dstConnection.query(deleteQuery, [medigle_id], (error, result) => {
                if (error) reject(error);
                else resolve(result);
            });
        });

        console.log(`Deleted existing users for facility_id ${medigle_id}`);

        // Now, select all users from the source database for this facility
        const selectQuery = 'SELECT * FROM user WHERE medigle_id = ?';
        const users = await new Promise((resolve, reject) => {
            srcConnection.query(selectQuery, [medigle_id], (error, results) => {
                if (error) reject(error);
                else resolve(results);
            });
        });

        if (users.length === 0) {
            console.log(`No users found with medigle_id ${medigle_id}`);
            return;
        }

        // Insert all users into the destination database
        const insertQuery = `INSERT INTO users 
            (facility_id, user_id, user_password_hash, user_name, user_role, is_active, is_locked, updated_by, created_at, updated_at) 
            VALUES ?`;

        const values = users.map(row => [
            medigle_id,
            row.user_name,
            row.password,
            row.name,
            row.manage,
            row.use,
            row.failed_count,
            row.update_user,
            row.created,
            row.modified
        ]);

        await new Promise((resolve, reject) => {
            dstConnection.query(insertQuery, [values], (error, result) => {
                if (error) reject(error);
                else resolve(result);
            });
        });

        // Update migration status for each user
        for (const user of users) {
            await migrateConnection(mgConnection, 'user', 'users', user.id, user.id);
        }

        const queryEnd = Date.now();
        const queryExecutionTime = queryEnd - queryStart;
        console.log(`Inserted ${users.length} users for facility_id ${medigle_id}.`);
        console.log(`Query execution time: ${(queryExecutionTime / 1000).toFixed(2)} seconds.`);

    } catch (error) {
        console.error('Error in updateUsersByFacilityId:', error);
    }
}