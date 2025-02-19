package kz.talgat.extensions.databases;

import kz.dmc.packages.controllers.DMCController;
import org.apache.commons.codec.DecoderException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.mail.NoSuchProviderException;
import java.io.*;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;


public class FileLogs {
    private static FileLogs instantLog = null;
    private static Configuration configuration = new Configuration();
    private static Path pathFile = null;
    private static FileSystem fs = null;
    private HashMap<String, String> fileCompleteHashMap = new HashMap<>();
    private HashMap<String, Integer> groupsInFileHashMap = new HashMap();
    private HashMap<String, Integer> groupsProgressHashMap = new HashMap();

    public void setLogFile(String filePath) throws ClassNotFoundException, SQLException, NoSuchProviderException, IOException {
        configuration = DMCController.get().getHadoopConf();
        this.pathFile = new Path("/db-submit-log/" + filePath + "log.txt");
        fs = FileSystem.get(configuration);
    }

    public static FileLogs get() {
        if (instantLog == null) {
            synchronized (FileLogs.class) {
                if (instantLog == null) {
                    instantLog = new FileLogs();
                }
            }
        }
        return instantLog;
    }

    private void putRecordCompletedHashMap(String key, String value) {
        synchronized (FileLogs.class) {
            fileCompleteHashMap.put(key, value);
        }
    }

    public boolean logFileExist() throws IOException {
        return fs.exists(pathFile);
    }

    public FileStatus[] getListNotCompletedFileStatuses(FileStatus[] _fileStatuses) {
        return Arrays.stream(_fileStatuses).filter(hdfsFile -> !checkFileForComplete(hdfsFile.getPath().toString())).toArray(FileStatus[]::new);
    }

    public FileStatus[] getListEarlyCalledTimeFileStatuses(FileStatus[] _fileStatuses, int withLog, LocalDateTime date) throws ClassNotFoundException, SQLException, NoSuchProviderException, IOException, DecoderException {
        if (date != null) {
            long currentMilliseconds = Timestamp.valueOf(date).getTime();
            int dateInt = Integer.parseInt(String.format("%02d", date.getHour()) + String.format("%02d", date.getMinute()) + String.format("%02d", date.getSecond()));
            if (withLog == 2) {  //all files in folder
                return _fileStatuses;
            }
            if (withLog == 3) { //all files by name
                return Arrays.stream(_fileStatuses).filter(hdfsFile -> (checkLogByFileName(hdfsFile, dateInt))).toArray(FileStatus[]::new);
            }
            //files between dates
            return Arrays.stream(_fileStatuses).filter(hdfsFile -> (hdfsFile.getModificationTime() < currentMilliseconds)).toArray(FileStatus[]::new);
        } else {
            return _fileStatuses;
        }
    }

    private boolean checkFileForComplete(String Key) {
        if (fileCompleteHashMap.containsKey(Key)) {
            String value = fileCompleteHashMap.get(Key);
            String[] values = value.split(";");
            if (values[0].equals("complete")) {
                return true;
            }
        }
        return false;
    }

    public void init() throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pathFile)));
        try {
            String line;
            line = br.readLine();
            while (line != null) {
                String[] lines = line.split("\t");
                fileCompleteHashMap.put(lines[0], lines[1]);
                line = br.readLine();
            }
        } finally {
            br.close();
        }
    }

    public void writeTextFile(int withLog) throws IOException {
        if (withLog > 0) {
            renameIfExist();
            OutputStream os = fs.create(pathFile);
            BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
            String keys[] = groupsInFileHashMap.keySet().stream().filter(key -> (groupsProgressHashMap.get(key) != null)).toArray(String[]::new);
            Arrays.stream(keys).forEach(key -> putRecordCompletedHashMap(key, "complete;" + getTimeStamp()));
            br.write(setStringsHashMap(fileCompleteHashMap));
            br.close();
            deleteOldIfExist();
        }
    }

    private void renameIfExist() throws IOException {
        if (logFileExist()) {
            fs.rename(pathFile, new Path(pathFile.toUri().getPath().toString().replace("log.txt", "log_old.txt")));
        }
    }

    private void deleteOldIfExist() throws IOException {
        if (fs.exists(new Path(pathFile.toUri().getPath().toString().replace("log.txt", "log_old.txt")))) {
            fs.delete(new Path(pathFile.toUri().getPath().toString().replace("log.txt", "log_old.txt")), false);
        }
    }

    private String setStringsHashMap(HashMap<String, String> hm) {
        StringBuilder sb = new StringBuilder();
        hm.forEach((key, value) -> {
            sb.append(key + "\t" + value + "\n");
        });
        return sb.toString();
    }

    public void setGroupsInFile(String fileName, int groupCount) throws ClassNotFoundException, SQLException, NoSuchProviderException, IOException, DecoderException {
        groupsInFileHashMap.put(fileName, groupCount);
    }

    public void setGroupsProgressHashMap(String fileName, int withLog) throws ClassNotFoundException, SQLException, NoSuchProviderException, IOException, DecoderException {
        if (withLog > 0) {
            synchronized (FileLogs.class) {
                if (groupsProgressHashMap.containsKey(fileName)) {
                    int groupCount = groupsProgressHashMap.get(fileName);
                    groupsProgressHashMap.replace(fileName, groupCount + 1);
                } else {
                    groupsProgressHashMap.put(fileName, 1);
                }
            }
        }
    }

    private String getTimeStamp() {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = new Date(System.currentTimeMillis());
        return formatter.format(date);
    }

    private boolean checkLogByFileName(FileStatus fileStatus, int date) {
        String fileParts[] = fileStatus.getPath().getName().split("_");
        String[] str = Arrays.stream(fileParts).filter(string -> (string.matches("[0-9]+") && string.length() == 6)).filter(dg -> (Integer.parseInt(dg) <= date)).toArray(String[]::new);
        if (str.length > 0) {
            return true;
        } else {
            return false;
        }
    }
}
