package kz.talgat.extensions.databases;

import kz.dmc.packages.console.DMCConsoleColors;
import kz.dmc.packages.error.DMCError;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.MessageType;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.JulianFields;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class DataBaseSubmit {
    private Connection connection;
    private PreparedStatement preparedStatement;
    private HashMap<String, String> dbHash = null;
    private String[] filedsOrder;
    private String[] keys = {"date", "timestamp", "numeric", "bigint", "datum", "binary", "bit"};
    private String[] values = {"date", "timestamp", "numeric", "bigint", "datum", "binary", "bit"};

    //Submit Parquet from group of Reading
    public void submitRowGroupCustomColumns(LinkedList<Group> rows, String connectionString, Properties properties, String sqlQuery, HashMap<Integer, String> mapData, HashMap<String, String> mapCustomColumns, MessageType parquetSchema) {
        try {
            ArrayList<String> columnsOrder = new ArrayList<>();
            parquetSchema.getColumns().forEach(column -> {
                columnsOrder.add(column.getPath()[0]);
            });
            filedsOrder = BaseSystem.getFieldNameOrder(columnsOrder, sqlQuery);

            connection = DriverManager.getConnection(connectionString, properties);
            connection.setAutoCommit(false);
            preparedStatement = connection.prepareStatement(sqlQuery);

            int max = filedsOrder.length;
            rows.forEach(row -> {
                for (int i = 0; i < max; i++) {
                    try {
                        prepareData(mapData, getDataFromParquetOrRow(row, i), i, connectionString);
                    } catch (Exception e) {
                        if (e.getMessage().contains("not found")) {
                            try {
                                if (connectionString.contains("sqlserver")) {
                                    preparedStatement.setString(i + 1, "");
                                } else {
                                    preparedStatement.setNull(i + 1, 0);
                                }

                            } catch (SQLException e1) {
                            }
                        } else {
                            e.printStackTrace();
                        }
                    }
                }
                int j = 1;
                for (Map.Entry<String, String> entry : mapCustomColumns.entrySet()) {
                    try {
                        preparedStatement.setString(max + j, entry.getValue());
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                    j = j + 1;
                }

                try {
                    preparedStatement.addBatch();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            });
            preparedStatement.executeBatch();
            connection.commit();
            connection.close();
        } catch (BatchUpdateException e) {
            if (connectionString.contains("postgresql")) {
                throw new RuntimeException(e.getNextException());
            } else if (connectionString.contains("sqlserver") && !e.getMessage().contains("duplicate key")) {
                StringBuilder stringBuilder = new StringBuilder();
                for (int er = 0; er < e.getUpdateCounts().length; er++) {
                    if (e.getUpdateCounts()[er] < 1) {
                        Group group = rows.get(er).asGroup();
                        for (int g = 0; g < filedsOrder.length; g++) {
                            try {
                                stringBuilder.append("\n" + filedsOrder[g] + "=" + getDataFromParquetOrRow(group, g));
                            } catch (Exception e1) {
                                stringBuilder.append("\n" + e1.getMessage());
                            }
                        }
                        stringBuilder.append("\n-------------------------------------------");
                    }
                }
                System.out.println(stringBuilder.toString() + e.getMessage());
                throw new RuntimeException(e);
            } else {
                throw new RuntimeException(e);
            }
        } catch (SQLException e) {
            if (connection != null) try {
                connection.close();
            } catch (Exception e1) {
            }
            DMCConsoleColors.colorRedText(DMCError.get().getFullErrorText(e));
            System.out.println(DMCError.get().getFullErrorText(e));
            new RuntimeException(e);
        }
    }

    //Submit Parquet from row of Reading
    public void submitRowCustomColumns(LinkedList<GenericRecord> rows, String connectionString, Properties properties, String sqlQuery, HashMap<Integer, String> mapData, HashMap<String, String> mapCustomColumns) {
        try {

            connection = DriverManager.getConnection(connectionString, properties);
            connection.setAutoCommit(false);
            preparedStatement = connection.prepareStatement(sqlQuery);
            int max = rows.getFirst().getSchema().getFields().size();
            rows.forEach(row -> {
                for (int i = 0; i < max; i++) {
                    try {
                        prepareData(mapData, getDataFromParquetOrRow(row, i), i, connectionString);
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
                int j = 1;
                for (Map.Entry<String, String> entry : mapCustomColumns.entrySet()) {
                    try {
                        preparedStatement.setString(max + j, entry.getValue());
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                    j = j + 1;
                }

                try {
                    preparedStatement.addBatch();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            });
            preparedStatement.executeBatch();
            connection.commit();
            connection.close();

        } catch (BatchUpdateException e) {
            if (connectionString.contains("postgresql")) {
                throw new RuntimeException(e.getNextException());
            } else if (connectionString.contains("sqlserver") && !e.getMessage().contains("duplicate key")) {
                StringBuilder stringBuilder = new StringBuilder();
                for (int er = 0; er < e.getUpdateCounts().length; er++) {
                    if (e.getUpdateCounts()[er] < 1) {
                        int finalEr = er;
                        rows.get(er).getSchema().getFields().forEach(field -> {
                            stringBuilder.append("\n" + field.name() + "=" + rows.get(finalEr).get(field.name()));
                        });
                        stringBuilder.append("\n--------------------------------------");
                    }
                }
                System.out.println(stringBuilder.toString() + e.getMessage());
                throw new RuntimeException(e);
            } else {
                throw new RuntimeException(e);
            }
        } catch (SQLException e) {
            if (connection != null) try {
                connection.close();
            } catch (Exception e1) {
            }
            DMCConsoleColors.colorRedText(DMCError.get().getFullErrorText(e));
            new RuntimeException(e);
        }
    }

    //insert from db->db
    public void generateScriptFromSourceDB(ResultSet resultSet, HashMap hm, String[] sourceDb, String tableName, String connectionString, Properties properties) {
        String sqlQuery = "";
        try {
            sqlQuery = BaseSystem.get().getInsertIntoQuery(sourceDb, hm).replace("%t", tableName);
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            AtomicInteger j = new AtomicInteger();
            connection = DriverManager.getConnection(connectionString, properties);
            connection.setAutoCommit(false);
            preparedStatement = connection.prepareStatement(sqlQuery);

            while (resultSet.next()) {
                j.set(1);
                hm.forEach((k, v) -> {
                    try {
                        setPreparedStatment(j.get(), resultSet.getString(k.toString()), v.toString());
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                    j.getAndIncrement();
                });
                preparedStatement.addBatch();
            }
            System.out.println("J=" + j);
            preparedStatement.executeBatch();
            connection.commit();
        } catch (BatchUpdateException e) {
            throw new RuntimeException(e.getNextException());
        } catch (SQLException e) {
            if (connection != null) try {
                connection.close();
            } catch (Exception e1) {
            }
            DMCConsoleColors.colorRedText(DMCError.get().getFullErrorText(e));
            new RuntimeException(e);
        }
    }

    //get mapped data from date-> String
    public static String getMappedDate(Object value, String type) {
        String v = value.toString();
        try {
            switch (type) {
                case "date1":
                    return v.substring(6, 10) + "-" + v.substring(0, 2) + "-" + v.substring(3, 5);
                case "date2":
                    return "to_date('" + v.substring(0, v.length() - 2) + "','yyyy-mm-dd hh24:mi:ss')";
                case "date3":
                    return v.substring(0, 19);
                case "date4": //int96 from datetime
                    return getTimeStampFromIn96(v).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                case "date5": //int32 from date
                    return new SimpleDateFormat("yyyy-MM-dd").format(getDateInt(v));
                case "date6": //from linux long
                    return Timestamp.from(Instant.ofEpochSecond(Long.parseLong(v.substring(0, 10)))).toLocalDateTime().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                case "date7":
                    if (v.contains("'")) {
                        v = v.replace("'", "");
                    }
                    v = v.replaceAll("\u0000", "");
                    return v;
                case "date8":
                    try {
                        return setFromByteChar(v);
                    } catch (Exception e) {
                        return "0";
                    }
                default:
                    return v;
            }
        } catch (Exception e) {
            return v;
        }
    }

    //set mapped data from numeric,timestamp -> PreparedStatement
    public void setMappedData(Object value, String type, int position) {
        String v;
        try {
            v = value.toString();
        } catch (Exception e) {
            v = null;
        }

        String match = "default";
        for (int i = 0; i < keys.length; i++) {
            if (type.contains(keys[i])) {
                match = values[i];
                break;
            }
        }

        try {
            switch (match) {
                case "numeric":
                    try {
                        preparedStatement.setBigDecimal(position, BigDecimal.valueOf(Double.parseDouble(v)));
                    } catch (Exception e) {
                        preparedStatement.setNull(position, Types.DECIMAL);
                    }
                    break;
                case "timestamp":
                    try {
                        preparedStatement.setTimestamp(position, Timestamp.valueOf(v.substring(0, 19)));
                    } catch (Exception e) {
                        preparedStatement.setNull(position, Types.TIMESTAMP);
                    }
                    break;
                case "date":
                    try {
                        preparedStatement.setString(position, getMappedDate(value, type));
                    } catch (Exception e) {
                        preparedStatement.setNull(position, Types.DATE);
                    }
                    break;
                case "bigint":
                    try {
                        preparedStatement.setLong(position, Long.valueOf(v));
                    } catch (Exception e) {
                        preparedStatement.setNull(position, Types.BIGINT);
                    }
                    break;
                case "datum":
                    try {
                        switch (type) {
                            case "datum1":
                                try {
                                    preparedStatement.setDate(position, getDate(v));
                                } catch (Exception e) {
                                    preparedStatement.setNull(position, Types.DATE);
                                }
                                break;
                            case "datum2":
                                try {
                                    preparedStatement.setDate(position, getDateInt(v));
                                } catch (Exception e) {
                                    preparedStatement.setNull(position, Types.DATE);
                                }
                                break;
                            case "datum3":
                                try {
                                    preparedStatement.setTimestamp(position, Timestamp.valueOf(getTimeStampFromIn96(v)));
                                } catch (Exception e) {
                                    preparedStatement.setNull(position, Types.TIMESTAMP);
                                }
                                break;
                            case "datum4":
                                try {
                                    preparedStatement.setTimestamp(position, Timestamp.from(Instant.ofEpochSecond(Long.parseLong(v.substring(0, 10)))));
                                } catch (Exception e) {
                                    preparedStatement.setNull(position, Types.TIMESTAMP);
                                }
                                break;
                            default:
                                preparedStatement.setDate(position, getDate(v));
                        }
                    } catch (Exception e) {
                        preparedStatement.setNull(position, Types.DATE);
                    }
                    break;
                case "binary":
                    try {
                        preparedStatement.setBytes(position, longToBytes(v));
                    } catch (Exception e) {
                        preparedStatement.setNull(position, Types.VARBINARY);
                    }
                    break;
                case "bit":
                    try {
                        preparedStatement.setByte(position, setByteFromChar(v));
                    } catch (Exception e) {
                        preparedStatement.setNull(position, Types.BIT);
                    }
                    break;
                default:
                    preparedStatement.setString(position, v);
            }
        } catch (Exception e) {
            try {
                preparedStatement.setNull(position, 0);
            } catch (SQLException e1) {
                DMCConsoleColors.colorRedText(DMCError.get().getFullErrorText(e));
                System.out.println(DMCError.get().getFullErrorText(e));
                new RuntimeException(e);
            }
        }
    }

    //set DB->DB -> PreparedStatement
    private void setPreparedStatment(int index, String value, String type) throws SQLException {
        try {
            switch (type.toUpperCase()) {
                case "BIT":
                case "TINYINT":
                    preparedStatement.setByte(index, Byte.parseByte(value));
                case "INT8":
                    preparedStatement.setLong(index, setLongValue(value));
                    break;
                case "INT2":
                case "INT4":
                case "INT32":
                case "REGPROC":
                case "OID":
                case "FLOAT4":
                case "FLOAT8":
                case "MONEY":
                case "NUMERIC":
                case "REGPROCEDURE":
                case "REGOPER":
                case "REGOPERATOR":
                case "REGCLASS":
                case "REGTYPE":
                case "REGROLE":
                case "REGNAMESPACE":
                case "REGCONFIG":
                case "REGDICTIONARY":
                case "CARDINAL_NUMBER":
                    preparedStatement.setBigDecimal(index, BigDecimal.valueOf(Double.parseDouble(value)));
                    break;
                case "STRING":
                case "VARCHAR":
                case "GEOMETRY":
                    preparedStatement.setString(index, value);
                    break;
                case "FIXEDSTRING":
                    preparedStatement.setNString(index, value);
                    break;
                case "DATETIME":
                case "TIMESTAMP":
                    preparedStatement.setTimestamp(index, Timestamp.valueOf(value.substring(0, 19)));
                    break;
                default:
                    preparedStatement.setString(index, value);
                    break;
            }
        } catch (Exception e) {
            preparedStatement.setNull(index, 0);
        }

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

    //setData commons for 2 methods
    private void prepareData(HashMap<Integer, String> mapData, Object value, int index, String connectionString) throws SQLException {
        if (mapData.containsKey(index) && value != null) {
            if (mapData.get(index).contains("date")) {
                preparedStatement.setString(index + 1, getMappedDate(value, mapData.get(index)));
            } else {
                setMappedData(value, mapData.get(index), index + 1);
            }
        } else if (value == null || value.toString().toLowerCase().equals("null")) {
            if (connectionString.contains("sqlserver") && mapData.containsKey(index)) {
                setMappedData(value, mapData.get(index), index + 1);
            } else {
                preparedStatement.setNull(index + 1, 0);
            }
        } else {
            preparedStatement.setString(index + 1, String.valueOf(value));
        }
    }

    //set LONG value from String, 0 if exaption
    private long setLongValue(String str) {
        long value = 0L;
        try {
            value = Long.parseLong(str.trim());
        } catch (Exception e) {
            value = 0L;
        }
        return value;
    }

    private java.sql.Date getDate(String value) {
        java.sql.Date ts = null;
        try {
            ts = java.sql.Date.valueOf(value);
        } catch (Exception e) {

        }
        return ts;
    }

    private static java.sql.Date getDateInt(String value) {
        java.sql.Date ts = null;
        java.sql.Date ts1 = java.sql.Date.valueOf("1970-01-01");
        try {
            ts = java.sql.Date.valueOf(ts1.toLocalDate().plusDays(Long.valueOf(value)));
        } catch (Exception e) {

        }
        return ts;
    }

    private static LocalDateTime getTimeStampFromIn96(String bytesString) {
        String[] strBytes = bytesString.replace("Int96Value{Binary{12 constant bytes, [", "").replace("]}}", "").replace(" ", "").split(",");
        byte[] int96Bytes = new byte[strBytes.length];

        for (int i = 0; i < int96Bytes.length; ++i) {
            int96Bytes[i] = Byte.parseByte(strBytes[i]);
        }

        // Find Julian day
        int julianDay = 0;
        int index = int96Bytes.length;
        while (index > 8) {
            index--;
            julianDay <<= 8;
            julianDay += int96Bytes[index] & 0xFF;
        }

        // Find nanos since midday (since Julian days start at midday)
        long nanos = 0;
        // Continue from the index we got to
        while (index > 0) {
            index--;
            nanos <<= 8;
            nanos += int96Bytes[index] & 0xFF;
        }

        LocalDateTime timestamp = LocalDate.MIN
                .with(JulianFields.JULIAN_DAY, julianDay)
                .atTime(LocalTime.NOON)
                .plusNanos(nanos);

        return timestamp;
    }

    public byte[] longToBytes(String value) {
        Long x = Long.parseLong(value);
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(x);
        byte[] byteArray = buffer.array();
        byte[] byteConvert = new byte[4];
        int j = 0;
        if (byteArray.length > 4) {
            for (int i = 4; i < byteArray.length; i++) {
                byteConvert[j] = byteArray[i];
                j++;
            }
            return byteConvert;
        }
        return byteArray;
    }

    //setByteFromChars()
    private Byte setByteFromChar(String value) {
        Byte b = 0;
        if (value.toUpperCase().contains("N")) {
            b = 0;
        } else if (value.toUpperCase().contains("Y")) {
            b = 1;
        } else {
            b = Byte.parseByte(value);
        }
        return b;
    }

    //setByteFromChars()
    private static String setFromByteChar(String value) {
        if (value.toUpperCase().contains("N")) {
            return "0";
        } else if (value.toUpperCase().contains("Y")) {
            return "1";
        } else {
            return value;
        }
    }

}

