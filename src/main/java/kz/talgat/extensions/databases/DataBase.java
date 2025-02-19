package kz.talgat.extensions.databases;

import kz.dmc.packages.console.DMCConsoleColors;
import kz.dmc.packages.controllers.DMCController;
import kz.dmc.packages.enums.DMCStatuses;
import kz.dmc.packages.error.DMCError;
import kz.dmc.packages.threads.pools.DMCThreadsPool;
import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarStyle;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileStatus;


import javax.mail.NoSuchProviderException;
import java.io.IOException;
import java.sql.*;
import java.text.DecimalFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class DataBase {
    private static String fileFilter = "";
    private static int batchSize = 10000;
    private static int submitThreadCount = 8;
    private static String dataBaseType = "NONE";
    private static String host = "";
    private static String port = "";
    private static String dbName = "";
    private static String connectionString = "";
    private static String tableName = "";
    private static Properties properties = new Properties();
    private static String user = "";
    private static String password = "";
    private static String caseFields = "exact";
    private static LocalDate date = null;
    private static LocalDateTime date2 = null;
    private static HashMap<String, String> fieldsHashMap = null;
    private static String customColumn = "";
    private static Schema schema = null;
    private static boolean readAllSchema = false;
    private static int schemaAll = 0;
    private static int withLog = 0;
    private static String sqlQuery;
    private static int modeType = 0;  //0 - automatic chouse of program,1- aoutomatic bulk for mssql, 2- row method choose, 3- group choose, 4 - row bulk, 5- group bulk
    private static String mapDataFields = "";
    private static String dataType = "PARQUET";
    private static String[] sourceDb;
    private static String[] filedsOrder;
    private static HashMap<Integer, String> mapData = new HashMap();
    private static HashMap<String, String> mapCustomColums = new HashMap(); //"custom-column": "ReportDate=?date[-1day]"  [-1month] [-1hour] [-1minute]
    protected static ProgressBar progressBar = new ProgressBar("Completed:", 0, 2000, System.out, ProgressBarStyle.UNICODE_BLOCK, "", 1, true, new DecimalFormat("#"));

    static {
        try {
            if (DMCController.get().isExistAppParam("file-filter")) {
                fileFilter = DMCController.get().getAppParam("file-filter").getAsString();
            }
        } catch (Exception e) {
        }

        try {
            if (DMCController.get().isExistAppParam("insert-batch-size")) {
                batchSize = DMCController.get().getAppParam("insert-batch-size").getAsInteger();
            }
        } catch (Exception e) {
        }

        try {
            if (DMCController.get().isExistAppParam("submit-thread-count")) {
                submitThreadCount = DMCController.get().getAppParam("submit-thread-count").getAsInteger();
            }
        } catch (Exception e) {
        }

        try {
            if (DMCController.get().isExistAppParam("date")) {
                date = DMCController.get().getAppParam("date").getAsLocalDate();
            }
        } catch (Exception e) {
        }

        try {
            if (DMCController.get().isExistAppParam("date")) {
                date2 = DMCController.get().getAppParam("date").getAsLocalDateTime();
            }
        } catch (Exception e) {
        }

        try {
            dataBaseType = DMCController.get().getAppParam("database-type").getAsString();
        } catch (Exception e) {
        }

        try {
            host = DMCController.get().getAppParam("host").getAsString();
        } catch (Exception e) {
        }

        try {
            port = DMCController.get().getAppParam("port").getAsString();
        } catch (Exception e) {
        }

        try {
            dbName = DMCController.get().getAppParam("database-name").getAsString();
        } catch (Exception e) {
        }

        try {
            tableName = DMCController.get().getAppParam("destination-table").getAsString();
        } catch (Exception e) {
        }

        try {
            user = DMCController.get().getAppParam("user").getAsString();
        } catch (Exception e) {
        }

        try {
            password = DMCController.get().getAppParam("password").getAsString();
        } catch (Exception e) {
        }

        try {
            if (DMCController.get().isExistAppParam("fields-case")) {
                caseFields = DMCController.get().getAppParam("fields-case").getAsString();
            }
        } catch (Exception e) {
        }

        try {
            if (DMCController.get().isExistAppParam("mode-type")) {
                modeType = DMCController.get().getAppParam("mode-type").getAsInteger();
            }
        } catch (Exception e) {
        }

        try {
            if (DMCController.get().isExistAppParam("custom-column")) {
                customColumn = DMCController.get().getAppParam("custom-column").getAsString();
            }
        } catch (Exception e) {
        }

        try {
            if (DMCController.get().isExistAppParam("map-data-fields")) {
                mapDataFields = DMCController.get().getAppParam("map-data-fields").getAsString();
            }
        } catch (Exception e) {
        }

        try {
            if (DMCController.get().isExistAppParam("with-log")) {
                withLog = DMCController.get().getAppParam("with-log").getAsInteger();
            }
        } catch (Exception e) {
        }

        try {
            if (DMCController.get().isExistAppParam("data-type")) {
                dataType = DMCController.get().getAppParam("data-type").getAsString();
            }
        } catch (Exception e) {
        }

        try {
            if (DMCController.get().isExistAppParam("source-db-config")) {
                sourceDb = DMCController.get().getAppParam("source-db-config").getAsString().split(";");
            }
        } catch (Exception e) {
        }

        try {
            if (DMCController.get().isExistAppParam("all-schema")) {
                schemaAll = DMCController.get().getAppParam("all-schema").getAsInteger();
            }
        } catch (Exception e) {
            schemaAll = 0;
        }


        DMCThreadsPool.get().setPoolSize(submitThreadCount);
        connectionString = DbConnections.getConnectionString(dataBaseType, host, port, dbName, tableName);
        properties = DbConnections.getProreties(dataBaseType, user, password);

    }

    //main worker method
    public static void runJob() throws Exception {
        switch (dataType) {
            case "DB":
                DataBaseSubmit dataBaseSubmit = new DataBaseSubmit();
                runScriptBefore();
                dataBaseSubmit.generateScriptFromSourceDB(new AdapterDB(sourceDb).getResultDB(), getHashMap(), sourceDb, tableName, connectionString, properties);
                runScriptAfter();
                break;
            default:
                setCustomColumnMap();
                FileStatus[] final_fileStatuses = getFileListsParquet();
                getRowCount(final_fileStatuses, submitThreadCount);
                if (StaticVariables.getRowsCount() > 0) {
                    DMCController.get().log(DMCStatuses.COMPUTING_INFO, "count rows=" + StaticVariables.getRowsCount());
                    runScriptBefore();
                    try {
                        readFilesForSubmit(final_fileStatuses);
                    } catch (Exception e) {
                        System.out.println(DMCError.get().getFullErrorText(e));
                        if (dataBaseType.equals("MSSQL") && tableName.contains("OSA_Aux")) {
                            try {
                                runScript("exec [test].[spOSAOnError] ?");
                            } catch (Exception e1) {
                                e1.printStackTrace();
                            }
                        }
                        if (dataBaseType.equals("MSSQL") && tableName.contains("CHA_Aux")) {
                            try {
                                runScript("exec [test].[spCHAOnError] ?");
                            } catch (Exception e1) {
                                e1.printStackTrace();
                            }
                        }
                        throw new RuntimeException(e);
                    } finally {
                        runScriptAfter();
                    }

                }
        }
    }

    //get list files of HDFS
    protected static FileStatus[] getFileListsParquet() throws Exception {
        HadoopFileSystem _hadoopFileSystem = new HadoopFileSystem();
        FileStatus[] final_fileStatuses = null;
        String _fileName = BaseSystem.get().getFileName(DMCController.get().getListDataSources()[0]);
        FileStatus[] _fileStatuses = null;
        if (!fileFilter.equals("")) {
            _fileStatuses = _hadoopFileSystem.listFilesStatuses(_fileName, fileFilter.split(","));
        } else {
            _fileStatuses = _hadoopFileSystem.listFilesStatuses(_fileName);
        }
        if (withLog > 0) {
            _fileStatuses = getLogFiles(_fileStatuses, _fileName);
        }
        final_fileStatuses = _fileStatuses;
        return final_fileStatuses;
    }

    //get Log of files
    protected static FileStatus[] getLogFiles(FileStatus[] _fileStatuses, String _fileName) throws Exception {
        FileStatus[] fileStatuses = null;
        FileLogs.get().setLogFile(_fileName);
        if (FileLogs.get().logFileExist()) {
            FileLogs.get().init();
            fileStatuses = FileLogs.get().getListEarlyCalledTimeFileStatuses(FileLogs.get().getListNotCompletedFileStatuses(_fileStatuses), withLog, date2);
        } else {
            if (withLog > 0) {
                fileStatuses = FileLogs.get().getListEarlyCalledTimeFileStatuses(_fileStatuses, withLog, date2);
            }
        }
        return fileStatuses;
    }

    //count of rows in folder/file/files
    protected static void getRowCount(FileStatus[] fileStatuses, int submitThreadCount) throws InterruptedException {
        Long countFiles = Long.valueOf(fileStatuses.length);
        AtomicInteger currFile = new AtomicInteger();
        ProgressBar pb = new ProgressBar("Completed:", 0, 1000, System.out, ProgressBarStyle.UNICODE_BLOCK, "", 1, true, new DecimalFormat("#"));
        pb.maxHint(countFiles);
        DMCThreadsPool readRowsCountPool = (DMCThreadsPool) new DMCThreadsPool().setPoolSize(submitThreadCount);
        Supplier<Stream<FileStatus>> fileStatusStream = () -> Arrays.stream(fileStatuses);

        fileStatusStream.get().forEach(hdfsFile -> {
            currFile.getAndIncrement();
            try {
                readRowsCountPool.addToPool(new DMCThreadReadRowsCount(pb, countFiles, currFile.get(), hdfsFile));
            } catch (Exception e) {
                DMCConsoleColors.colorRedText(DMCError.get().getFullErrorText(e));
            }
        });
        readRowsCountPool.shutDownAndWait();
        pb.close();
    }

    //generate threeds for parralel reding
    protected static void readFilesForSubmit(FileStatus[] fileStatuses) throws InterruptedException, IOException, SQLException, NoSuchProviderException, ClassNotFoundException {
        progressBar.maxHint(StaticVariables.getRowsCount());
        getSchema(fileStatuses[0].getPath().toString());
        setSqlQuery();
        getMapDataFromParams();
        DMCThreadsPool readRowsPool = (DMCThreadsPool) new DMCThreadsPool().setPoolSize(submitThreadCount);
        System.out.println(sqlQuery);
        Supplier<Stream<FileStatus>> fileStatusStream = () -> Arrays.stream(fileStatuses);
        if ((fileStatuses.length < submitThreadCount && StaticVariables.groupsCount > fileStatuses.length && !readAllSchema && modeType == 0) || (modeType == 3 && !readAllSchema) || (modeType == 5 && !readAllSchema)) {
            System.out.println("\n------------     With Group Parquet  " + StaticVariables.groupsCount + "     -------------");
            fileStatusStream.get().forEach(hdfsFile -> {
                try {
                    ReadInfoParquet readInfoParquet = new ReadInfoParquet();
                    readInfoParquet.init(hdfsFile.getPath().toString());
                    int groupCount = readInfoParquet.getGroupsCount();
                    for (int j = 1; j <= groupCount; j++) {
                        readRowsPool.addToPool(new ParquetReadRows(withLog, progressBar, hdfsFile, schema, batchSize, connectionString, properties, sqlQuery, mapData, modeType, mapCustomColums, j));
                        FileLogs.get().setGroupsInFile(hdfsFile.getPath().toString(), withLog);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(DMCError.get().getFullErrorText(e));
                }
            });
        } else if (readAllSchema || (modeType == 3 && readAllSchema) || (modeType == 5 && readAllSchema)) {
            System.out.println("\n------------     With Group All Schema Parquet  " + StaticVariables.groupsCount + "     -------------");
            fileStatusStream.get().forEach(hdfsFile -> {
                try {
                    ReadInfoParquet readInfoParquet = new ReadInfoParquet();
                    readInfoParquet.init(hdfsFile.getPath().toString());
                    int groupCount = readInfoParquet.getGroupsCount();
                    for (int j = 1; j <= groupCount; j++) {
                        readRowsPool.addToPool(new ParquetReadRows(withLog, progressBar, hdfsFile, batchSize, connectionString, properties, sqlQuery, mapData, modeType, mapCustomColums, j));
                        FileLogs.get().setGroupsInFile(hdfsFile.getPath().toString(), withLog);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(DMCError.get().getFullErrorText(e));
                }
            });
        } else if (modeType == 2 || modeType == 4) {
            System.out.println("\n------------     With Row Parquet       -------------");
            fileStatusStream.get().forEach(hdfsFile -> {
                try {
                    readRowsPool.addToPool(new ParquetReadRows(withLog, progressBar, hdfsFile, schema, batchSize, connectionString, properties, sqlQuery, mapData, modeType, mapCustomColums));
                    FileLogs.get().setGroupsInFile(hdfsFile.getPath().toString(), withLog);
                } catch (Exception e) {
                    throw new RuntimeException(DMCError.get().getFullErrorText(e));
                }
            });
        } else {
            System.out.println("\n------------     With Row Parquet       -------------");
            fileStatusStream.get().forEach(hdfsFile -> {
                try {
                    readRowsPool.addToPool(new ParquetReadRows(withLog, progressBar, hdfsFile, schema, batchSize, connectionString, properties, sqlQuery, mapData, modeType, mapCustomColums));
                    FileLogs.get().setGroupsInFile(hdfsFile.getPath().toString(), withLog);
                } catch (Exception e) {
                    throw new RuntimeException(DMCError.get().getFullErrorText(e));
                }
            });
        }
        readRowsPool.shutDownAndWait();
        DMCThreadsPool.get().shutDownAndWait();
        FileLogs.get().writeTextFile(withLog);
        try {
            DMCController.get().log(DMCStatuses.COMPUTING_INFO, "row inserted=" + progressBar.getCurrent());
        } catch (Exception e) {
        }
        progressBar.close();
    }

    // set Schema from DB -> Parquet
    protected static void getSchema(String fileName) throws ClassNotFoundException, SQLException, NoSuchProviderException, IOException {
        ReadInfoParquet parquet = new ReadInfoParquet();
        parquet.init(fileName);
        parquet.getSchema().getColumns().forEach(st -> {
            try {
                if (st.getPrimitiveType().getPrimitiveTypeName().toString().contains("INT96")) readAllSchema = true;
            } catch (Exception e) {
            }
        });
        if (schemaAll > 0) readAllSchema = true;
        fieldsHashMap = getHashMap();
        schema = (modeType == 1 || modeType == 4) ? parquet.getSchemaByMap(fieldsHashMap, filedsOrder) : parquet.getSchemaByMap(fieldsHashMap);
    }

    //generate hashMap of data types of fields
    protected static void getMapDataFromParams() {
        mapData = new HashMap<Integer, String>();
        if (mapDataFields.length() > 0) {
            String arrayMap[] = mapDataFields.split(",");
            String position[] = sqlQuery.split("\\) VALUES")[0].split("\\(")[1].split(",");
            for (int i = 0; i < arrayMap.length; i++) {
                String nName[] = arrayMap[i].split("=");
                mapData.put(getPositionOfColumn(position, nName[0]), nName[1]);
            }
        }
    }

    //get position of fields in Insert into Query
    private static int getPositionOfColumn(String position[], String value) {
        for (int i = 0; i < position.length; i++) {
            if (position[i].toUpperCase().equals(value.toUpperCase())) return i;
        }
        return -1;
    }

    // set all data and Map from destination table
    private static HashMap<String, String> getHashMap() throws SQLException {
        Connection connection = DriverManager.getConnection(connectionString, properties);
        HashMap<String, String> _result = new HashMap<>();
        Statement _statement = connection.createStatement();
        String sqlStr = "SELECT * FROM " + tableName + " WHERE 1=0";
        if (dataBaseType.equals("MSSQL")) {
            sqlStr = "SELECT * FROM " + tableName + " with(NoLock) WHERE 1=0";
        }
        ResultSet _rs = _statement.executeQuery(sqlStr);
        ResultSetMetaData _rsMetaData = _rs.getMetaData();

        int _countFields = _rsMetaData.getColumnCount();
        filedsOrder = new String[_countFields];
        for (int i = 1; i <= _countFields; i++) {
            if (_rsMetaData.getColumnTypeName(i).contains("(")) {
                String a[] = _rsMetaData.getColumnTypeName(i).split("\\(");
                _result.put(rightNameColumn(_rsMetaData.getColumnName(i)), a[0]);
            } else {
                _result.put(rightNameColumn(_rsMetaData.getColumnName(i)), _rsMetaData.getColumnTypeName(i));
            }
            filedsOrder[i - 1] = _rsMetaData.getColumnName(i);
        }
        connection.close();
        return _result;
    }

    //"fields-case" case sensetive
    // "lower"-> to lower; "upper" -> to upper; "exact" -> "exact",default;
    protected static String rightNameColumn(String value) {
        switch (dataBaseType) {
            case "ORACLE":
                if (Character.isDigit(value.charAt(0))) {
                    value = "\"" + value + "\"";
                }
        }
        return value;

    }

    //Run destination database scripts;
    //? inserting date parametr
    private static void runScript(String sqlScript) throws SQLException {
        Connection connection = DriverManager.getConnection(connectionString, properties);
        connection.setAutoCommit(false);
        String[] scriptLines = sqlScript.split(";");
        Statement _statement = connection.createStatement();
        for (int i = 0; i < scriptLines.length; i++) {
            if (!scriptLines[i].equals("")) {
                if (scriptLines[i].contains("?")) {
                    if (dataBaseType.equals("CLICKHOUSE")) {
                        boolean _rs = _statement.execute(scriptLines[i].replace("?", "'" + date.toString() + "'"));
                        String _status = _rs ? DMCConsoleColors.colorRedText("Error") : DMCConsoleColors.colorGreenText("Success");
                        System.out.println(scriptLines[i].replace("?", "'" + date.toString() + "'") + " -> " + _status);
                    } else {
                        CallableStatement cstmt = connection.prepareCall(scriptLines[i]);
                        cstmt.setDate(1, Date.valueOf(date));
                        boolean _rs = cstmt.execute();
                        String _status = _rs ? DMCConsoleColors.colorRedText("Error") : DMCConsoleColors.colorGreenText("Success");
                        System.out.println(scriptLines[i].replace("?", "'" + date.toString() + "'") + " -> " + _status);
                    }
                    connection.commit();
                } else {
                    boolean _rs = false;
                    if (scriptLines[i].contains("select")) {
                        _rs = _statement.executeQuery(scriptLines[i]).isFirst();
                    } else {
                        _rs = _statement.execute(scriptLines[i]);
                    }
                    String _status = _rs ? DMCConsoleColors.colorRedText("Error") : DMCConsoleColors.colorGreenText("Success");
                    System.out.println(scriptLines[i] + " -> " + _status);
                    connection.commit();
                }
            }
        }
        connection.close();
    }

    //run before inserting script
    private static void runScriptBefore() {
        try {
            if (DMCController.get().isExistAppParam("script-before")) {
                runScript(DMCController.get().getAppParam("script-before").getAsString());
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(DMCError.get().getFullErrorText(e));
        }
    }

    //run after inserting script
    private static void runScriptAfter() {
        try {
            if (DMCController.get().isExistAppParam("script-after")) {
                runScript(DMCController.get().getAppParam("script-after").getAsString());
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(DMCError.get().getFullErrorText(e));
        }
    }

    //set Insert into by schema
    private static void setSqlQuery() {
        StringBuilder query = new StringBuilder();
        if (dataBaseType.equals("ORACLE")) {
            query.append("INSERT /*+ APPEND NOLOGGING */ INTO " + tableName + " (");
        } else {
            query.append("INSERT INTO " + tableName + " (");
        }

        schema.getFields().forEach(field -> {
            if (field.pos() == schema.getFields().size() - 1) {
                fieldsHashMap.keySet().stream().filter(key -> (key.toLowerCase().equals(field.name().toLowerCase()))).forEach(key -> query.append(rightNameColumn(key)));
            } else {
                fieldsHashMap.keySet().stream().filter(key -> (key.toLowerCase().equals(field.name().toLowerCase()))).forEach(key -> query.append(rightNameColumn(key) + ","));
            }
        });
        if (mapCustomColums.size() > 0) {
            mapCustomColums.keySet().forEach(key -> {
                query.append("," + key.toString());
            });
        }
        query.append(")");
        query.append(" VALUES (");
        schema.getFields().forEach(field -> {
            if (field.pos() == schema.getFields().size() - 1) {
                query.append("?");
            } else {
                query.append("?,");
            }
        });

        if (mapCustomColums.size() > 0) {
            mapCustomColums.keySet().forEach(key -> {
                query.append(",?");
            });
        }

        query.append(")");
        sqlQuery = query.toString();
    }

    //set custom columns not in schema
    // ReportDate=?date[-1day]
    private static void setCustomColumnMap() throws Exception {
        if (customColumn.length() > 1) {
            mapCustomColums = BaseSystem.get().setCustomColumnMap(customColumn);
        }
    }
}
