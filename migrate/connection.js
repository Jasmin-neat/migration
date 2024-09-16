exports.migrateConnection = function (connection, oldDB, newDB, old_id, new_id) {
    const newRow = {
        oldDB,
        newDB,
        old_id,
        new_id
    };

    connection.query("INSERT INTO db_connection SET ?", newRow, function(error, result, fields) {
        if (error) throw error;
    });
}