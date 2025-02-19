package kz.talgat.extensions.databases;

import java.sql.*;
import java.util.Properties;

//class for db-> db
public class AdapterDB {
    private static String whereDb = "";
    private static String userDB = "";
    private static String passwordDB = "";
    private static String hostDB = "";
    private static String portDB = "";
    private static String schemaDB = "";
    private static String tableDB = "";
    private static String selectDB = "*";
    private static String databaseType;
    private static String[] sourcedb;


    public AdapterDB(String[] sourcedb) {
        this.sourcedb = sourcedb;
    }

    // get array String from -> String separated by ;
    //get result set of DB
    public static ResultSet getResultDB() throws SQLException {
        for (String s : sourcedb) {
            String[] k = s.split("=");
            switch (k[0]) {
                case "user":
                    userDB = k[1];
                    break;
                case "password":
                    passwordDB = k[1];
                    break;
                case "host":
                    hostDB = k[1];
                    break;
                case "port":
                    portDB = k[1];
                    break;
                case "schema":
                    schemaDB = k[1];
                    break;
                case "table":
                    tableDB = k[1];
                    break;
                case "select":
                    selectDB = k[1];
                    break;
                case "databasetype":
                    databaseType = k[1];
                    break;
            }
        }

        String strConnection = DbConnections.getConnectionString(databaseType, hostDB, portDB, schemaDB, tableDB);
        Properties properties = DbConnections.getProreties(databaseType, userDB, passwordDB);
        Connection connection = DriverManager.getConnection(strConnection, properties);
        Statement statement = connection.createStatement();
        return statement.executeQuery("Select " + selectDB + " from %s ".replace("%s", tableDB) + whereDb);
    }
}
