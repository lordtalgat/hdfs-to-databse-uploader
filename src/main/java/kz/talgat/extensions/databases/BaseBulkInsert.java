package kz.talgat.extensions.databases;

import com.microsoft.sqlserver.jdbc.SQLServerBulkCSVFileRecord;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions;
import kz.dmc.packages.error.DMCError;
import me.tongfei.progressbar.ProgressBar;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.MessageType;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;



public class BaseBulkInsert implements Runnable {
    private ProgressBar progressBar;
    private LinkedList<GenericRecord> rows = null;
    private LinkedList<Group> groupRows = null;
    private String connectionString;
    private Properties properties;
    private String sqlQuery;
    private HashMap<Integer, String> mapData;
    private HashMap<String, String> mapCustomColums;
    private MessageType parquetSchema = null;
    private Connection connection;
    private String[] filedsOrder;

    //constructor from rows parquet
    public BaseBulkInsert(String sqlQuery, String connectionString, Properties properties, ProgressBar progressBar, HashMap<Integer, String> mapData, HashMap<String, String> mapCustomColumns, LinkedList<GenericRecord> rows) {
        this.sqlQuery = sqlQuery;
        this.connectionString = connectionString;
        this.properties = properties;
        this.progressBar = progressBar;
        this.mapData = mapData;
        this.mapCustomColums = mapCustomColumns;
        this.rows = rows;
    }

    //constructor from group parquet
    public BaseBulkInsert(String sqlQuery, String connectionString, Properties properties, ProgressBar progressBar, HashMap<Integer, String> mapData, HashMap<String, String> mapCustomColumns, LinkedList<Group> groupRows, MessageType parquetSchema) {
        this.sqlQuery = sqlQuery;
        this.connectionString = connectionString;
        this.properties = properties;
        this.progressBar = progressBar;
        this.mapData = mapData;
        this.mapCustomColums = mapCustomColumns;
        this.groupRows = groupRows;
        this.parquetSchema = parquetSchema;
    }

    @Override
    public void run() {
        fillBulkFile();
    }

    //bulk insert from csv file
    private void fillBulkFile() {
        StringBuilder csvBuilder = new StringBuilder();
        csvBuilder.append(getFromData());
        byte[] bytes = csvBuilder.toString().replace("'", "").getBytes(StandardCharsets.UTF_8);
        InputStream inputStream = new ByteArrayInputStream(bytes);
        try {
            connection = DriverManager.getConnection(connectionString, properties);
            SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(connection);
            bulkCopy.setDestinationTableName(sqlQuery.split(" \\(")[0].replace("INSERT INTO ", ""));
            SQLServerBulkCopyOptions copyOptions = new SQLServerBulkCopyOptions();
            copyOptions.setBulkCopyTimeout(10000000);
            copyOptions.setKeepNulls(true);
            bulkCopy.setBulkCopyOptions(copyOptions);
            SQLServerBulkCSVFileRecord fileRecord = new SQLServerBulkCSVFileRecord(inputStream, StandardCharsets.UTF_8.name(), "\t", false);
            int i = 1;
            for (String str : sqlQuery.split("\\) VALUES")[0].split("\\(")[1].split(",")) {
                fileRecord.addColumnMetadata(i, str, Types.NVARCHAR, 2000, 0);
                i = i + 1;
            }

            bulkCopy.writeToServer(fileRecord);
            connection.commit();
            bulkCopy.close();
            if (groupRows != null) {
                progressBar.stepBy(groupRows.size());
                groupRows.clear();
            }
            if (rows != null) {
                progressBar.stepBy(rows.size());
                rows.clear();
            }
            connection.close();
        } catch (SQLException e) {
            if (connection!=null) try {
                connection.close();
            }catch (Exception e1){}
            System.out.println(e.getMessage());
            new RuntimeException(e);
        }
    }

    //for one record ready
    private void fillBulkFile(LinkedList<GenericRecord> rowsR,LinkedList<Group> groupRowsR) {
        StringBuilder csvBuilder = new StringBuilder();
        if (rowsR==null){
            csvBuilder.append(getFromDataGroup(groupRowsR));
        }else{
            csvBuilder.append(getFromDataRow(rowsR));
        }
        byte[] bytes = csvBuilder.toString().replace("'", "").getBytes(StandardCharsets.UTF_8);
        InputStream inputStream = new ByteArrayInputStream(bytes);
        try {
            connection = DriverManager.getConnection(connectionString, properties);
            SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(connection);
            bulkCopy.setDestinationTableName(sqlQuery.split(" \\(")[0].replace("INSERT INTO ", ""));
            SQLServerBulkCopyOptions copyOptions = new SQLServerBulkCopyOptions();
            copyOptions.setBulkCopyTimeout(10000000);
            copyOptions.setKeepNulls(true);
            bulkCopy.setBulkCopyOptions(copyOptions);
            SQLServerBulkCSVFileRecord fileRecord = new SQLServerBulkCSVFileRecord(inputStream, StandardCharsets.UTF_8.name(), "\t", false);
            int i = 1;
            for (String str : sqlQuery.split("\\) VALUES")[0].split("\\(")[1].split(",")) {
                fileRecord.addColumnMetadata(i, str, Types.NVARCHAR, 2000, 0);
                i = i + 1;
            }

            bulkCopy.writeToServer(fileRecord);
            connection.commit();
            bulkCopy.close();
            if (groupRowsR != null) {
                progressBar.stepBy(groupRowsR.size());
                groupRowsR.clear();
            }
            if (rowsR != null) {
                progressBar.stepBy(rowsR.size());
                rowsR.clear();
            }
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private  ArrayList<String> getResultArray(String connectionString, Properties properties,String tableName) throws SQLException {
        Connection connection = DriverManager.getConnection(connectionString, properties);
        Statement _statement = connection.createStatement();
        String sqlStr ="SELECT * FROM " + tableName + " with(NoLock) WHERE 1=0";
        ResultSet _rs = _statement.executeQuery(sqlStr);
        ArrayList<String> rows = new ArrayList<>();
        for(int i=1;i<=_rs.getMetaData().getColumnCount();i++){
            rows.add(_rs.getMetaData().getColumnName(i));
        }
        connection.close();
        return rows;
    }

    //choose source for csv file
    private StringBuilder getFromData() {
        if (groupRows == null) {
            return getFromDataRow();
        } else {
            return getFromDataGroup();
        }
    }

    //generate csv file from group parquet
    private StringBuilder getFromDataGroup() {
        ArrayList<String> columnsOrder = new ArrayList<>();
        parquetSchema.getColumns().forEach(column -> {
            columnsOrder.add(column.getPath()[0]);
        });
        ArrayList<String> rs= new ArrayList<String>();
        try {
            rs = getResultArray(connectionString, properties,sqlQuery.split(" \\(")[0].replace("INSERT INTO ", ""));
        } catch (SQLException e) {
            e.printStackTrace();
            new RuntimeException(DMCError.get().getFullErrorText(e));
        }
        filedsOrder = BaseSystem.getFieldNameOrderBulk(columnsOrder, rs);
        int max = parquetSchema.getColumns().size();
        StringBuilder csvBuilder = new StringBuilder();
        groupRows.forEach(row -> {
            for (int i = 0; i < max; i++) {
                try {
                    csvBuilder.append(prepareData(getDataFromParquetOrRow(row, i), i));
                } catch (Exception e) {
                    if (e.getMessage().contains("not found")) {
                        csvBuilder.append("\t");
                    }
                }
            }
            for (Map.Entry<String, String> entry : mapCustomColums.entrySet()) {
                csvBuilder.append(entry.getValue() + "\t");
            }

            csvBuilder.append("\r\n");
        });
        return csvBuilder;
    }

    //for one row after bulk problem
    private StringBuilder getFromDataGroup(LinkedList<Group> groupRows) {
        int max = parquetSchema.getColumns().size();
        StringBuilder csvBuilder = new StringBuilder();
        groupRows.forEach(row -> {
            for (int i = 0; i < max; i++) {
                try {
                    csvBuilder.append(prepareData(getDataFromParquetOrRow(row, i), i));
                } catch (Exception e) {
                    if (e.getMessage().contains("not found")) {
                        csvBuilder.append("\t");
                    }
                }
            }
            for (Map.Entry<String, String> entry : mapCustomColums.entrySet()) {
                csvBuilder.append(entry.getValue() + "\t");
            }

            csvBuilder.append("\r\n");
        });
        return csvBuilder;
    }

    //generate csv file from row parquet
    private StringBuilder getFromDataRow() {
        StringBuilder csvBuilder = new StringBuilder();
        int max = rows.getFirst().getSchema().getFields().size();
        rows.forEach(row -> {
            for (int i = 0; i < max; i++) {
                csvBuilder.append(prepareData(getDataFromParquetOrRow(row, i), i));
            }
            for (Map.Entry<String, String> entry : mapCustomColums.entrySet()) {
                csvBuilder.append(entry.getValue() + "\t");
            }
            csvBuilder.append("\r\n");
        });
        return csvBuilder;
    }

    //for one row after bulk problem
    private StringBuilder getFromDataRow(LinkedList<GenericRecord> rows) {
        StringBuilder csvBuilder = new StringBuilder();
        int max = rows.getFirst().getSchema().getFields().size();
        rows.forEach(row -> {
            for (int i = 0; i < max; i++) {
                csvBuilder.append(prepareData(getDataFromParquetOrRow(row, i), i));
            }
            for (Map.Entry<String, String> entry : mapCustomColums.entrySet()) {
                csvBuilder.append(entry.getValue() + "\t");
            }
            csvBuilder.append("\r\n");
        });
        return csvBuilder;
    }

    private String prepareData(Object value, int index) {
        if (mapData.containsKey(index) && value != null) {
            if (mapData.get(index).contains("date")) {
                return DataBaseSubmit.getMappedDate(value, mapData.get(index)) + "\t";
            }
        } else if (value == null || value.toString().toLowerCase().equals("null")) {
            return "\t";
        } else {
            return String.valueOf(value) + "\t";
        }
        return String.valueOf(value) + "\t";
    }

    //getDataFromParquet row or group
    private Object getDataFromParquetOrRow(GenericRecord row, int index) {
        return row.get(index);
    }

    //getDataFromParquet row or group
    private Object getDataFromParquetOrRow(Group row, int index) {
        try {
            return row.getValueToString(row.getType().getFieldIndex(filedsOrder[index]), 0);
        } catch (Exception e) {
            return null;
        }
    }

}