package kz.talgat.extensions.databases;

import kz.dmc.packages.controllers.DMCController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class BaseSystem {
    private static BaseSystem ourInstance = null;

    public static BaseSystem get() throws Exception {
        if (ourInstance == null) {
            synchronized (BaseSystem.class) {
                ourInstance = new BaseSystem();
            }
        }
        return ourInstance;
    }

    public String getFileName(String rootID) throws Exception {
        AtomicReference<LocalDateTime> _localDateTime = new AtomicReference<>();
        AtomicReference<String> _fileName = new AtomicReference<>();
        if (DMCController.get().isExistAppParam("date")) {
            _localDateTime.set(DMCController.get().getAppParam("date").getAsLocalDateTime());
            _fileName.set(DateTimeStringFormatter.get().format(DMCController.get().getRootDataSourcePath(rootID) + DMCController.get().getAppParam("source-file-expression").getAsString(), _localDateTime.get()));
        } else {
            _fileName.set(DMCController.get().getRootDataSourcePath(rootID) + DMCController.get().getAppParam("source-file-expression").getAsString());
        }
        return _fileName.get();
    }

    // Array of columns separator ,
    // Name and Value separator =
    // Static setting of computing value ? date,datetime,addDays
    // dynamic setting of computing value $
    public HashMap<String, String> setCustomColumnMap(String colums) {
        HashMap<String, String> hp = new HashMap<>();
        if (colums.contains(",")) {
            String splitColumns[] = colums.split(",");
            for (int i = 0; i < splitColumns.length; i++) {
                hp.putAll(getHashItemFromString(splitColumns[i]));
            }
        } else {
            hp.putAll(getHashItemFromString(colums));
        }
        return hp;
    }

    private HashMap<String, String> getHashItemFromString(String value) {
        HashMap<String, String> hp = new HashMap<>();
        String splitValue[] = value.split("=");
        String name = splitValue[0];
        String settingValue = "";
        if (splitValue[1].contains("?")) {
            try {
                settingValue = splitValue[1].replace("?date", DMCController.get().getAppParam("date").getAsLocalDate().format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (settingValue.contains("[")) {
                try {
                    settingValue = solveDataChange(settingValue);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
        } else {
            settingValue = splitValue[1];
        }
        hp.put(name, settingValue);
        return hp;
    }

    //solve setting data [-1day],[+1day]
    //solve [-1month],[+2month]
    //solve [-1hour],[-1minute]
    private String solveDataChange(String settingValue) throws ParseException {
        String[] splitter = settingValue.split("\\[");
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Calendar cal = Calendar.getInstance();
        cal.setTime(sdf.parse(splitter[0]));
        for (int i = 1; i < splitter.length; i++) {
            if (splitter[i].contains("day")) {
                cal.add(Calendar.DAY_OF_MONTH,Integer.parseInt(splitter[i].replace("day","").replace("]","")));
            } else if (splitter[i].contains("month")) {
                cal.add(Calendar.MONTH,Integer.parseInt(splitter[i].replace("month","").replace("]","")));
            } else if (splitter[i].contains("hour")) {
                cal.add(Calendar.HOUR,Integer.parseInt(splitter[i].replace("hour","").replace("]","")));
            } else if (splitter[i].contains("minute")) {
                cal.add(Calendar.MINUTE,Integer.parseInt(splitter[i].replace("minute","").replace("]","")));
            }
        }
        return sdf.format(cal.getTime());
    }

    //getFileds order by sqlQuery -> Array<String>
    public static String[] getFieldNameOrder(ArrayList<String> arrayList, String sqlQuery) {
        String[] splitColumns = sqlQuery.split("\\) VALUES")[0].split("\\(")[1].split(",");
        ArrayList<String> rightFieldsOrder = new ArrayList<>();
        for (int i = 0; i < splitColumns.length; i++) {
            int fi = i;
            arrayList.stream().filter(list -> (list.toLowerCase().equals(splitColumns[fi].toLowerCase()))).forEach(list -> rightFieldsOrder.add(list));
        }
        return rightFieldsOrder.stream().toArray(String[]::new);
    }

    //getFileds order by sqlQuery -> Array<String>
    public static String[] getFieldNameOrderBulk(ArrayList<String> arrayList, ArrayList<String> splitColumns) {
        ArrayList<String> rightFieldsOrder = new ArrayList<>();
        splitColumns.stream().forEach(list->{
            arrayList.stream().filter(f->(f.toLowerCase().equals(list.toLowerCase()))).forEach(b -> rightFieldsOrder.add(b));
        });
        return rightFieldsOrder.stream().toArray(String[]::new);
    }

    private boolean onlyContainsNumbers(String text) {
        try {
            Long.parseLong(text);
            return true;
        } catch (NumberFormatException ex) {
            return false;
        }
    }

    private String typeFrom10Digits(String simbol, String value) {
        String finalType;
        if (onlyContainsNumbers(value.replace(simbol, ""))) {
            String a[] = value.split(simbol);
            if (a[2].length() > 3 && Integer.valueOf(a[0]) < 13 && Integer.valueOf(a[1]) > 12) {
                return "date1";
            }
        }
        return "date0";
    }

    public String getInsertIntoQuery(String[] sourceDb, HashMap<String, String> hashMap) {
        StringBuilder sb = new StringBuilder("INSERT INTO %t(");
        AtomicInteger i = new AtomicInteger();
        String select[] = new String[0];
        for (String s : sourceDb) {
            String[] k = s.split("=");
            switch (k[0]) {
                case "select":
                    select = k[1].split(",");
            }
        }
        String[] finalSelect = select;
        Set keys = hashMap.keySet().stream().filter(key -> (Arrays.stream(finalSelect).filter(field -> (field.toLowerCase().equals(key.toLowerCase())))).count() > 0).collect(Collectors.toSet());
        keys.forEach(key -> {
            if (i.get() == 0) {
                sb.append(key);
            } else {
                sb.append("," + key);
            }
            i.set(i.get() + 1);
        });
        sb.append(") VALUES(");
        i.set(0);
        keys.forEach(key -> {
            if (i.get() == 0) {
                sb.append("?");
            } else {
                sb.append(",?");
            }
            i.set(i.get() + 1);
        });
        sb.append(")");
        return sb.toString();
    }
}
