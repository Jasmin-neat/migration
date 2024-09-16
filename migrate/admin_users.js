const { migrateConnection } = require('./connection');
exports.migrateAdminUsers = function(srcConnection, dstConnection, mgConnection) {
    const queryStart = Date.now();

    dstConnection.query("DROP TABLE IF EXISTS admin_users", function (error, result, fields) {
        if (error) {
            throw error;
        }
    });
    let tableCreateQuery = "CREATE TABLE admin_users (";
    tableCreateQuery += "id int(11) NOT NULL AUTO_INCREMENT";
    tableCreateQuery += ", admin_user_id varchar(255)";
    tableCreateQuery += ", admin_user_password_hash varchar(255)";
    tableCreateQuery += ", admin_user_name varchar(100)";
    tableCreateQuery += ", is_active int(1)";
    tableCreateQuery += ", is_locked int(11)";
    tableCreateQuery += ", updated_by int(11)";
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

    var query = srcConnection.query('SELECT * FROM admin_user');

    var progress = 0;
    query.on('result', function(row) {
        srcConnection.pause();

        var newRow = {
            admin_user_id: row.login_id,
            admin_user_password_hash: row.password,
            admin_user_name: row.name,
            is_active: row.use,
            is_locked: row.failed_count,
            updated_by: row.update_user,
            created_at: row.created,
            updated_at: row.modified
        };

        dstConnection.query("INSERT INTO admin_users SET ?", newRow, function(error, result, fields) {
            if (error) throw error;
            migrateConnection(mgConnection, 'admin_user', 'admin_users', row.id, result.insertId);
            progress = progress + 1;
            srcConnection.resume();
        });
    });
    query.on('end', function() {
        const queryEnd = Date.now();
        const queryExecutionTime = queryEnd - queryStart;
        console.log("Admin_users Total count:",progress);
        console.log(`Query execution time: ${(queryExecutionTime / 1000).toFixed(2)} seconds.`);
    });
}