package kz.talgat.extensions.databases;


import kz.dmc.packages.controllers.DMCController;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;


public class HadoopFileSystem {
    private Configuration configuration = null;
    private FileSystem hadoopFileSystem = null;
    private String defaultFS = null;

    public HadoopFileSystem() throws IOException, SQLException, ClassNotFoundException, javax.mail.NoSuchProviderException {
        hadoopFileSystem = DistributedFileSystem.get(DMCController.get().getHadoopConf());
    }

    public HadoopFileSystem(Configuration configuration) throws IOException {
        hadoopFileSystem = DistributedFileSystem.get(configuration);
    }

    public FileStatus[] listFilesStatuses(String rootDir) throws IOException {
        return Arrays.stream(hadoopFileSystem.listStatus(new Path(rootDir))).filter(f -> (!f.getPath().getName().contains("_SUCCESS"))).toArray(FileStatus[]::new);
    }

    public FileStatus[] listFilesStatuses(String rootDir, String[] filters) throws IOException {
        List<FileStatus> res = new ArrayList<FileStatus>();
        FileStatus[] notFiltered = hadoopFileSystem.listStatus(new Path(rootDir));
        for (FileStatus file : notFiltered) {
            if (Arrays.stream(filters).filter(f -> (file.getPath().getName().contains(f))).count() > 0 && !file.getPath().getName().contains("_SUCCESS")) {
                res.add(file);
            }
        }
        return res.toArray(new FileStatus[0]);
    }

    public Configuration getConfiguration() {
        return configuration;
    }


    public boolean isExists(String fileOrDir) throws IOException {
        return hadoopFileSystem.exists(new Path(fileOrDir));
    }


    public FSDataOutputStream createFile(String fileName) throws IOException {
        return hadoopFileSystem.create(new Path(fileName));
    }

    public OutputStream createOutputStream(String fileName) throws IOException {
        return hadoopFileSystem.create(new Path(fileName));
    }

    public InputStream createInputStream(String fileName) throws IOException {
        return hadoopFileSystem.open(new Path(fileName));
    }

    public String[] findFiles(String pathDir, String ext) throws IOException {
        List<String> _files = new ArrayList<>();
        FileStatus[] _filesStatus = hadoopFileSystem.listStatus(new Path(pathDir));
        Set<String> acceptableNames = Arrays.stream(ext.split(",")).collect(Collectors.toSet());

        if (ext.equals("*")) {
            List<String> final_files = _files;
            Arrays.stream(_filesStatus).filter(i -> i.isFile()).forEach(i -> {
                final_files.add(i.getPath().toString().replace(defaultFS, ""));
            });

        } else {

            _files = Arrays.stream(_filesStatus)
                    .filter(i -> i.isFile())
                    .filter(i -> acceptableNames.contains(getExt(i)))
                    .map(i -> i.getPath().toString().replace(defaultFS, ""))
                    .collect(Collectors.toList());

        }

        return _files.toArray(new String[_files.size()]);
    }

    private String getExt(FileStatus fs) {
        String _fs = fs.getPath().toString();
        return _fs.substring(_fs.lastIndexOf("."));
    }

    public FileSystem getHadoopFileSystem() {
        return hadoopFileSystem;
    }

    public boolean deleteFile(String fileName) throws IOException {
        return hadoopFileSystem.delete(new Path(fileName), false);
    }

    public boolean deleteDirectory(String directoryName) throws IOException {
        hadoopFileSystem.delete(new Path(directoryName), true);
        return false;
    }

    public boolean renameFile(String fileNameFrom, String fileNameTo) throws IOException {
        Path _fNameFrom = new Path(fileNameFrom);

        if (hadoopFileSystem.isFile(_fNameFrom)) {
            Path _fNameTo = new Path(fileNameTo);
            return hadoopFileSystem.rename(_fNameFrom, _fNameTo);
        } else {
            return false;
        }
    }

    public boolean createDirs(String dirName) throws IOException {
        return hadoopFileSystem.mkdirs(new Path(dirName));
    }
}
