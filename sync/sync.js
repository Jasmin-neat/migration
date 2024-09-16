const { migrateConnection } = require('../migrate/connection');
const { migrateSingleContract, getMedigleFacilityId } = require('../migrate/contract');
const { migrateSingleFacility } = require('../migrate/facility');
const { updateUsersByFacilityId } = require('../migrate/users');
exports.insert = function (res, connection, table, row) {
    let newTable;
    switch (table) {
        case 'medigle_facility':
            newTable = "contracts";
            newData = {
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
            break;
        case 'admin_user':
            newTable = 'admin_users';
            newRow = {
                admin_user_id: row.login_id,
                admin_user_password_hash: row.password,
                admin_user_name: row.name,
                is_active: row.use,
                is_locked: row.failed_count,
                updated_by: row.update_user,
                created_at: row.created,
                updated_at: row.modified
            };
            break;
        case 'user':
            newTable = 'users';
            newRow = {
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
            break;
        case 'epark_facility':
            newTable = 'facilities';
            newRow = {
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
            break;
    }
    connection.query(`INSERT INTO ${newTable} SET ?`, newRow, function (error, result, fields) {
        console.log(result);
        migrateConnection(connection, table, newTable, row.id, result.insertId);
        console.log('inserted successfully');
    });
}

exports.update = function (res, srcConnection, dstConnection, mgConnection, row) {
    migrateSingleFacility(srcConnection, dstConnection, mgConnection, row.id);
    migrateSingleContract(srcConnection, dstConnection, mgConnection, row.id);
    updateUsersByFacilityId(srcConnection, dstConnection, mgConnection, row.id);
}

exports.remove = function (res, connection, table, row) {
    let newTable;
    switch (table) {
        case 'medigle_facility':
            newTable = "contracts";
            newData = {
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
            break;
        case 'admin_user':
            newTable = 'admin_users';
            newRow = {
                admin_user_id: row.login_id,
                admin_user_password_hash: row.password,
                admin_user_name: row.name,
                is_active: row.use,
                is_locked: row.failed_count,
                updated_by: row.update_user,
                created_at: row.created,
                updated_at: row.modified
            };
            break;
        case 'user':
            newTable = 'users';
            newRow = {
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
            break;
        case 'epark_facility':
            newTable = 'facilities';
            newRow = {
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
            break;
    }
    var newId;
    var query = connection.query(`SELECT * from db_connection WHERE oldDB='${table}' AND newDB='${newTable}' AND old_id='${row.id}'`);
    query.on('result', function (row) {
        newId = row.new_id;
        newRow.id = newId;
        console.log(newId);
        connection.query(`DELETE FROM ${newTable} WHERE id='${newId}'`, function (error, result, fields) {
            console.log('deleted successfully');
        });
    });
}