package kz.talgat.extensions.databases;


import com.google.gson.*;
import kz.dmc.packages.controllers.DMCController;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.schema.*;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class ReadInfoParquet {
    private Configuration configuration = new Configuration();
    private ParquetFileReader reader = null;
    private MessageType schema = null;
    private InputFile file = null;
    private List<ColumnDescriptor> columns = null;
    private int groupsCount = 0;
    private int rowsCount = 0;
    private ParquetReader<GenericRecord> parquetFile = null;

    public ReadInfoParquet() throws SQLException, IOException, ClassNotFoundException, javax.mail.NoSuchProviderException {
        configuration = DMCController.get().getHadoopConf();
        configuration.set("dfs.socket.timeout", "1000");
        configuration.set("dfs.client.socket-timeout", "1000");
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public ReadInfoParquet init(String fileName) throws IOException {
        file = HadoopInputFile.fromPath(new Path(fileName), configuration);
        ParquetFileReader parquetFileReader = new ParquetFileReader(file, ParquetReadOptions.builder().build());
        schema = parquetFileReader.getFooter().getFileMetaData().getSchema();
        groupsCount = parquetFileReader.getFooter().getBlocks().size();
        rowsCount = 0;

        for (BlockMetaData block : parquetFileReader.getFooter().getBlocks()) {
            rowsCount += block.getRowCount();
        }

        parquetFileReader.close();
        return this;
    }

    public void initSimple(String fileName) throws IOException {
        file = HadoopInputFile.fromPath(new Path(fileName), configuration);
    }

    public Schema getSchemaByMap(HashMap<String, String> hashMap) {
        return getSchemaByMap(hashMap, null);
    }

    public Schema getSchemaByMap(HashMap<String, String> hashMap, String[] filedsOrder) {
        MessageType messageType = schema;
        AvroSchemaConverter b = new AvroSchemaConverter();
        JsonObject jobj = null;
        try {
            Schema schema2 = b.convert(messageType);
            jobj = new Gson().fromJson(schema2.toString(), JsonObject.class);
        } catch (Exception e) {
            jobj = schmemaConvert(schema.toString());
        }
        JsonArray jsonArray = jobj.getAsJsonArray("fields");
        JsonArray jsonArray_new = new JsonArray();
        if (filedsOrder == null) { //for default find of schema
            for (int i = 0; i < jsonArray.size(); i++) {
                int fI = i;
                if (hashMap.keySet().stream().filter(key -> (key.toLowerCase().equals(jsonArray.get(fI).getAsJsonObject().get("name").getAsString().toLowerCase()))).count() > 0) {
                    jsonArray_new.add(jsonArray.get(fI));
                }
            }
        } else { //for bulk insert mssql
            HashMap<String, String> hashSchemaFiledsRightNames = new HashMap<>();
            Iterator ir = jsonArray.iterator();
            while (ir.hasNext()) {
                JsonElement jsonElement = (JsonElement) ir.next();
                hashSchemaFiledsRightNames.put(jsonElement.getAsJsonObject().get("name").getAsString().toLowerCase(), jsonElement.getAsJsonObject().get("name").getAsString());
            }
            for (int i = 0; i < filedsOrder.length; i++) {
                String fieldName = hashSchemaFiledsRightNames.get(filedsOrder[i].toLowerCase());
                for (int j = 0; j < jsonArray.size(); j++) {
                    if (jsonArray.get(j).getAsJsonObject().get("name").getAsString().equals(fieldName)) {
                        jsonArray_new.add(jsonArray.get(j));
                        break;
                    }
                }
            }
        }
        jobj.remove("fields");
        jobj.add("fields", jsonArray_new);
        return new Schema.Parser().parse(jobj.toString());
    }

    private static JsonObject schmemaConvert(String str) {
        String[] splited = str.split("\n");
        String namespace = "";
        try {
            namespace = splited[0].replace("message ", "").replace(" {", "").split("\\.")[0];
        } catch (Exception e) {
            namespace = "";
        }
        ;
        String name = "spark_schema";
        try {
            name = splited[0].replace("message ", "").replace(" {", "").split("\\.")[1];
        } catch (Exception e) {
        }
        ;
        JsonObject jobj = new JsonObject();
        jobj.addProperty("type", "record");
        jobj.addProperty("name", name);
        jobj.addProperty("namespace", namespace);
        JsonArray jsonArray = new JsonArray();
        for (int i = 1; i < splited.length - 1; i++) {
            String[] value = splited[i].substring(2, splited[i].length() - 1).split("\\ ");
            JsonObject job = new JsonObject();
            job.addProperty("name", value[2]);
            JsonArray jsonArrayInternal = getElementFromSchema(value);
            job.add("type", jsonArrayInternal);
            job.add("default", JsonNull.INSTANCE);
            jsonArray.add(job);
        }
        jobj.add("fields", jsonArray);
        return jobj;
    }

    private static JsonArray getElementFromSchema(String[] value) {
        JsonArray jsonArray = new JsonArray();
        jsonArray.add(new JsonPrimitive("null"));
        if (value[1].equals("binary")) {
            jsonArray.add(new JsonPrimitive("string"));
        } else if (value[1].equals("int32")) {
            if (value.length > 3) {
                JsonObject jsonObject = new JsonObject();
                jsonObject.addProperty("type", "int");
                jsonObject.addProperty("logicalType", value[3].toLowerCase().replace(")", "").replace("(", ""));
                jsonArray.add(jsonObject);
            } else {
                jsonArray.add(new JsonPrimitive("int"));
            }
        } else if (value[1].equals("double")) {
            jsonArray.add(new JsonPrimitive("double"));
        } else if (value[1].equals("int64")) {
            jsonArray.add(new JsonPrimitive("long"));
        } else if (value[1].equals("int96")) {
            jsonArray.add(new JsonPrimitive("string"));
        }
        return jsonArray;
    }

    public void closeFile() {
        try {
            parquetFile.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public ParquetReader<GenericRecord> readParquetFile() throws IOException {
        parquetFile = AvroParquetReader.<GenericRecord>builder(file).build();
        return parquetFile;
    }

    public ParquetReader<GenericRecord> readParquetFileWithSchema(Schema schema) throws IOException {
        AvroReadSupport.setRequestedProjection(configuration, schema);
        AvroReadSupport.setAvroReadSchema(configuration, schema);
        parquetFile = AvroParquetReader.<GenericRecord>builder(file).withConf(configuration).build();
        return parquetFile;
    }

    public MessageType getSchema() {
        return schema;
    }

    public List<ColumnDescriptor> getColumns() {
        return schema.getColumns();
    }

    public int getRowsCount() {
        return rowsCount;
    }

    public int getGroupsCount() {
        return groupsCount;
    }

    public ParquetFileReader readParquetGroup() throws IOException {
        ParquetFileReader _parquetFileReader = new ParquetFileReader(file, ParquetReadOptions.builder().build());
        return _parquetFileReader;
    }

    private String getRigthFieldName(String filedName, List<Type> typesList) {
        for (int i = 0; i < typesList.size(); i++) {
            if (filedName.toLowerCase().equals(typesList.get(i).getName().toLowerCase())) {
                return typesList.get(i).getName();
            }
        }
        return filedName;
    }

    public String getStringValue(Group recordGroup, String fieldName) {
        if (!recordGroup.getType().containsField(fieldName)) {
            fieldName = getRigthFieldName(fieldName, recordGroup.getType().getFields());
        }
        try {
            switch (recordGroup.getType().getType(fieldName).asPrimitiveType().getPrimitiveTypeName()) {
                case FLOAT:
                    return String.valueOf(recordGroup.getFloat(fieldName, 0));
                case DOUBLE:
                    return String.valueOf(recordGroup.getDouble(fieldName, 0));
                case INT32:
                    if (recordGroup.getType().getType(fieldName).getLogicalTypeAnnotation() == LogicalTypeAnnotation.dateType()) {
                        return new java.sql.Date(0).toLocalDate().plusDays(recordGroup.getInteger(fieldName, 0)).toString();
                    } else {
                        return String.valueOf(recordGroup.getInteger(fieldName, 0));
                    }
                case INT64:
                    return String.valueOf(recordGroup.getLong(fieldName, 0));
                case INT96:
                    return String.valueOf(recordGroup.getInt96(fieldName, 0));
                case BOOLEAN:
                    return String.valueOf(recordGroup.getBoolean(fieldName, 0));
                case BINARY:
                    String _value = recordGroup.getBinary(fieldName, 0).toStringUsingUTF8();
                    if (_value.equals("") || _value.equals("null")) {
                        return null;
                    } else {
                        return _value;
                    }
            }
        } catch (Exception e) {
            System.out.println(e.getLocalizedMessage());
        }

        return null;
    }

    public String getStringValueMySql(Group recordGroup, String fieldName) {
        try {
            switch (recordGroup.getType().getType(fieldName).asPrimitiveType().getPrimitiveTypeName()) {
                case FLOAT:
                    return String.valueOf(recordGroup.getFloat(fieldName, 0));
                case DOUBLE:
                    return String.valueOf(recordGroup.getDouble(fieldName, 0));
                case INT32:
                    return String.valueOf(recordGroup.getInteger(fieldName, 0));
                case INT64:
                    return String.valueOf(recordGroup.getLong(fieldName, 0));
                case INT96:
                    return String.valueOf(recordGroup.getInt96(fieldName, 0));
                case BOOLEAN:
                    return String.valueOf(recordGroup.getBoolean(fieldName, 0));
                case BINARY:
                    String _value = recordGroup.getBinary(fieldName, 0).toStringUsingUTF8();
                    if (_value.equals("null")) {
                        return null;
                    } else {
                        return _value;
                    }
            }
        } catch (Exception e) {
            System.out.println(e.getLocalizedMessage());
        }
        return null;
    }

    public String getStringValue(GenericRecord record, String fieldName) {
        AtomicReference<String> rightName = new AtomicReference<>();
        if (record.get(fieldName) == null) {
            record.getSchema().getFields().forEach(field -> {
                if (field.name().toUpperCase().equals(fieldName.toUpperCase())) {
                    rightName.set(field.name());
                }
            });
        } else {
            rightName.set(fieldName);
        }

        String _val = String.valueOf(record.get(rightName.get()));

        if (_val != null) {
            if (_val.equals("null")) {
                return null;
            } else {
                return _val;
            }
        } else {
            return null;
        }
    }

}
