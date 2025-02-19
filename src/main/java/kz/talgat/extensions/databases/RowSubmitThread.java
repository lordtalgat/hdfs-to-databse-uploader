package kz.talgat.extensions.databases;

import me.tongfei.progressbar.ProgressBar;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.MessageType;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Properties;

public class RowSubmitThread implements Runnable {

    private ProgressBar progressBar;
    private LinkedList<GenericRecord> rows = null;
    private LinkedList<Group> groupRows = null;
    private String connectionString;
    private Properties properties;
    private String sqlQuery;
    private HashMap<Integer, String> mapData;
    private HashMap<String, String> mapCustomColums;
    private int modeType;
    private MessageType parquetSchema = null;

    public RowSubmitThread(ProgressBar progressBar, LinkedList<GenericRecord> rows, String connectionString, Properties properties, String sqlQuery, HashMap<Integer, String> mapData, int modeType, HashMap<String, String> mapCustomColums) {
        this.progressBar = progressBar;
        this.rows = rows;
        this.connectionString = connectionString;
        this.properties = properties;
        this.sqlQuery = sqlQuery;
        this.mapData = mapData;
        this.modeType = modeType;
        this.mapCustomColums = mapCustomColums;
    }

    public RowSubmitThread(ProgressBar progressBar, LinkedList<Group> groupRows, String connectionString, Properties properties, String sqlQuery, HashMap<Integer, String> mapData, int modeType, HashMap<String, String> mapCustomColums, MessageType parquetSchema) {
        this.progressBar = progressBar;
        this.groupRows = groupRows;
        this.connectionString = connectionString;
        this.properties = properties;
        this.sqlQuery = sqlQuery;
        this.mapData = mapData;
        this.modeType = modeType;
        this.mapCustomColums = mapCustomColums;
        this.parquetSchema = parquetSchema;
    }

    @Override
    public void run() {
        DataBaseSubmit dataBaseSubmit = new DataBaseSubmit();
        if (parquetSchema != null) {
            dataBaseSubmit.submitRowGroupCustomColumns(groupRows, connectionString, properties, sqlQuery, mapData, mapCustomColums, parquetSchema);
        } else {
            dataBaseSubmit.submitRowCustomColumns(rows, connectionString, properties, sqlQuery, mapData, mapCustomColums);
        }
        if (groupRows != null) {
            progressBar.stepBy(groupRows.size());
            groupRows.clear();
        }
        if (rows != null) {
            progressBar.stepBy(rows.size());
            rows.clear();
        }

    }
}
