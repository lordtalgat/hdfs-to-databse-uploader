package kz.talgat.extensions.databases;

import java.util.Properties;

public class DbConnections {

    //generation of connection string
    public static String getConnectionString(String dataBaseType, String host, String port, String dbName, String tableName) {
        String _connectionString = "%s" + host + ":" + port + "%s";
        switch (dataBaseType) {
            case "MYSQL":
                _connectionString = String.format(_connectionString, "jdbc:mysql://", dbName + "?characterEncoding=utf8");
                break;
            case "POSTGRESQL":
                _connectionString = String.format(_connectionString, "jdbc:postgresql://", dbName);
                break;
            case "ORACLE":
                _connectionString = String.format(_connectionString, "jdbc:oracle:thin:@", dbName);
                break;
            case "CLICKHOUSE":
                _connectionString = String.format(_connectionString, "jdbc:clickhouse://", dbName);
                break;
            case "MSSQL":
                _connectionString = String.format(_connectionString, "jdbc:sqlserver://", ";databaseName=" + dbName + ";useBulkCopyForBatchInsert=true;applicationName=" + tableName + ";");
                break;
        }
        return _connectionString;
    }

    //generation of connection properties -> user,password
    public static Properties getProreties(String dataBaseType, String user, String password) {
        switch (dataBaseType) {
            case "MSSQL":
                return getProreties(user, password, false);
            default:
                return getProreties(user, password, false);
        }
    }

    //generation of connection properties kerberos -> user,password
    private static Properties getProreties(String user, String password, Boolean integratedSecurity) {
        Properties properties = new Properties();
        properties.setProperty("user", user);
        properties.setProperty("password", password);
        if ((integratedSecurity)) {
            properties.setProperty("integratedSecurity", "true");
            properties.setProperty("authenticationScheme", "JavaKerberos");
        }
        return properties;
    }
}
