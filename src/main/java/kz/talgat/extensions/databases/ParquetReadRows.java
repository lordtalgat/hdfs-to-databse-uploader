package kz.talgat.extensions.databases;

import kz.dmc.packages.console.DMCConsoleColors;
import kz.dmc.packages.error.DMCError;
import kz.dmc.packages.threads.pools.DMCThreadsPool;
import me.tongfei.progressbar.ProgressBar;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileStatus;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Properties;

public class ParquetReadRows implements Runnable {
    private ProgressBar progressBar = null;
    private ReadInfoParquet readInfoParquet = null;
    private FileStatus file = null;
    private Schema schema = null;
    private int batchSize = 100000;
    private String connectionString;
    private Properties properties;
    private String sqlQuery;
    private PageReadStore group = null;
    private HashMap<Integer, String> mapData;
    private HashMap<String, String> mapCustomColums;
    private int withLog=0;
    private int modeType;
    private int index = 0;

    public ParquetReadRows(int withLog,ProgressBar progressBar, FileStatus file, Schema schema, int batchSize, String connectionString, Properties properties, String sqlQuery, HashMap<Integer, String> mapData, int modeType, HashMap<String, String> mapCustomColums) {
        this.progressBar = progressBar;
        this.file = file;
        this.schema = schema;
        this.batchSize = batchSize;
        this.connectionString = connectionString;
        this.properties = properties;
        this.sqlQuery = sqlQuery;
        this.mapData = mapData;
        this.modeType = modeType;
        this.mapCustomColums = mapCustomColums;
        this.withLog=withLog;
    }
//read group with parquet schema
    public ParquetReadRows(int withLog,ProgressBar progressBar, FileStatus file, Schema schema, int batchSize, String connectionString, Properties properties, String sqlQuery, HashMap<Integer, String> mapData, int modeType, HashMap<String, String> mapCustomColums, int index) {
        this.progressBar = progressBar;
        this.file = file;
        this.schema = schema;
        this.batchSize = batchSize;
        this.connectionString = connectionString;
        this.properties = properties;
        this.sqlQuery = sqlQuery;
        this.mapData = mapData;
        this.modeType = modeType;
        this.mapCustomColums = mapCustomColums;
        this.index = index;
        this.withLog=withLog;
    }
    //read group withOut parquet schema
    public ParquetReadRows(int withLog,ProgressBar progressBar, FileStatus file, int batchSize, String connectionString, Properties properties, String sqlQuery, HashMap<Integer, String> mapData, int modeType, HashMap<String, String> mapCustomColums, int index) {
        this.progressBar = progressBar;
        this.file = file;
        this.batchSize = batchSize;
        this.connectionString = connectionString;
        this.properties = properties;
        this.sqlQuery = sqlQuery;
        this.mapData = mapData;
        this.modeType = modeType;
        this.mapCustomColums = mapCustomColums;
        this.index = index;
        this.withLog=withLog;
    }


    @Override
    public void run() {
        if (index == 0) {
            readRowFile();
        } else {
            readRowGropus(index);
        }
    }
    //prepare Row from file
    private void readRowFile() {
        try {
            readInfoParquet = new ReadInfoParquet();
            readInfoParquet.initSimple(file.getPath().toString());
            ParquetReader<GenericRecord> _records = readInfoParquet.readParquetFileWithSchema(schema);
            GenericRecord _record = null;
            LinkedList<GenericRecord> _arrayRows = new LinkedList<>();
            while ((_record = _records.read()) != null) {
                _arrayRows.addFirst(_record);
                if (_arrayRows.size() >= batchSize) {
                    while (DMCThreadsPool.get().getQueueTaskCount() > 4) {
                        Thread.sleep(100);
                    }
                    DMCThreadsPool.get().addToPool(getTheadClass(_arrayRows));
                    _arrayRows = new LinkedList<>();
                }
            }

            readInfoParquet.closeFile();
            if (_arrayRows.size() > 0) {
                DMCThreadsPool.get().addToPool(getTheadClass(_arrayRows));
            }
            FileLogs.get().setGroupsProgressHashMap(file.getPath().toString(),withLog);
        } catch (Exception e) {
            DMCConsoleColors.colorRedText(DMCError.get().getFullErrorText(e));
            new RuntimeException(e);
        }
    }
   //getClass by Rows
    private Runnable getTheadClass(LinkedList<GenericRecord> arrayRows) {
        switch (modeType) {
            case 1:
            case 4:
            case 5:
                return new BaseBulkInsert(sqlQuery, connectionString, properties, progressBar, mapData, mapCustomColums, arrayRows);
            default:
                return new RowSubmitThread(progressBar, arrayRows, connectionString, properties, sqlQuery, mapData, modeType, mapCustomColums);
        }
    }
    //getClass by Group
    private Runnable getTheadClass(LinkedList<Group> arrayGroups, MessageType parquetSchema) {
        switch (modeType) {
            case 1:
            case 4:
            case 5:
                return new BaseBulkInsert(sqlQuery, connectionString, properties, progressBar, mapData, mapCustomColums, arrayGroups, parquetSchema);
            default:
                return new RowSubmitThread(progressBar, arrayGroups, connectionString, properties, sqlQuery, mapData, modeType, mapCustomColums, parquetSchema);
        }
    }
    //prepare Group from file
    private void readRowGropus(int index) {
        try {
            readInfoParquet = new ReadInfoParquet();
            readInfoParquet.init(file.getPath().toString());
            ParquetFileReader groups = readInfoParquet.readParquetGroup();
            final MessageType parquetSchema =setSchemaForGroup(); //new AvroSchemaConverter().convert(schema);
            groups.setRequestedSchema(parquetSchema);
            group = getGroupByIndex(groups, index);
            final MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(parquetSchema);
            final RecordReader<Group> recordReader = columnIO.getRecordReader(group, new GroupRecordConverter(parquetSchema));
            LinkedList<Group> _arrayRows = new LinkedList<>();

            for (long i = 0; i < group.getRowCount(); i++) {
                Group recordGroup = getRecord(recordReader);
                _arrayRows.addFirst(recordGroup);
                if (_arrayRows.size() >= batchSize) {
                    while (DMCThreadsPool.get().getQueueTaskCount() > 4) {
                        Thread.sleep(100);
                    }
                    DMCThreadsPool.get().addToPool(getTheadClass(_arrayRows, parquetSchema));
                    _arrayRows = new LinkedList<>();
                }
            }
            if (_arrayRows.size() > 0) {
                DMCThreadsPool.get().addToPool(getTheadClass(_arrayRows, parquetSchema));
            }
            FileLogs.get().setGroupsProgressHashMap(file.getPath().toString(),withLog);
        } catch (Exception e) {
            DMCConsoleColors.colorRedText(DMCError.get().getFullErrorText(e));
            new RuntimeException(e);
        }
    }

    //Chteni konkretnoi grouppy po @index
    private PageReadStore getGroupByIndex(ParquetFileReader groups, int index) throws IOException {
        PageReadStore _page = null;
        for (int i = 1; i <= index; i++) {
            _page = groups.readNextRowGroup();
        }
        return _page;
    }

    //poluchenie group iz file
    private Group getRecord(RecordReader<Group> recordReader) {
        return recordReader.read();

    }

    //setSchema
    private MessageType setSchemaForGroup(){
        if (schema==null){
            return readInfoParquet.getSchema();
        }else{
            return new AvroSchemaConverter().convert(schema);
        }
    }
}
