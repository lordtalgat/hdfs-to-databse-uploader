package kz.talgat.extensions.databases;

import kz.dmc.packages.console.DMCConsoleColors;
import kz.dmc.packages.error.DMCError;
import me.tongfei.progressbar.ProgressBar;
import org.apache.hadoop.fs.FileStatus;

public class DMCThreadReadRowsCount implements Runnable {
    private ProgressBar progressBar = null;
    private ReadInfoParquet readInfoParquet = null;
    private FileStatus file = null;
    private Integer numFile = 0;
    private Long allFilesCount = 0L;

    public DMCThreadReadRowsCount(ProgressBar progressBar, Long allFilesCount, Integer numFile, FileStatus file) {
        this.progressBar = progressBar;
        this.allFilesCount = allFilesCount;
        this.numFile = numFile;
        this.file = file;
    }


    @Override
    public void run() {
        try {
            readInfoParquet = new ReadInfoParquet();
            readInfoParquet.init(file.getPath().toString());
            StaticVariables.incRowsCount(Long.valueOf(readInfoParquet.getRowsCount()));
            StaticVariables.incGroupCount(readInfoParquet.getGroupsCount());
            progressBar.step();
            //System.out.println("Файл: " + numFile + " / " + allFilesCount + " / общее количество строк: " + DMCStaticVariables.getRowsCount());
        } catch (Exception e) {
            DMCConsoleColors.colorRedText(DMCError.get().getFullErrorText(e));
        }
    }
}
