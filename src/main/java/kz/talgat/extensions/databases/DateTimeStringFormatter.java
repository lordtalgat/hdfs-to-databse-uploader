package kz.talgat.extensions.databases;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;

public class DateTimeStringFormatter {
    private static DateTimeStringFormatter ourInstance = null;

    public static DateTimeStringFormatter get() throws Exception {
        if (ourInstance == null) {
            synchronized (DateTimeStringFormatter.class) {
                ourInstance = new DateTimeStringFormatter();
            }
        }
        return ourInstance;
    }

    //get format of date from file sources
    public String format(String str, LocalDateTime dateTime) {
        if (str.indexOf("[") > 0) {
            String[] _dates = str.substring(str.indexOf("[") + 1, str.lastIndexOf("]")).split(",");
            final String[] result = {str.substring(0, str.indexOf(","))};
            Arrays.stream(_dates).forEach(i -> {
                result[0] = result[0].replaceFirst("@format", "%s");
                DateTimeFormatter _dateTimeFormatter = DateTimeFormatter.ofPattern(i);
                result[0] = String.format(result[0], dateTime.format(_dateTimeFormatter));
            });

            return result[0];
        }

        return str;
    }


}
