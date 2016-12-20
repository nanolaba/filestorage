package com.nanolaba.filestorage.util;

import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipParameters;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.Deflater;

public class PlainToGzipConverter {

    private int bufferSize = 1024 * 8;

    private String rootFolder;
    private String sourceFileExtention = "dt";
    private String destFileExtention = "gz";

    public String getRootFolder() {
        return rootFolder;
    }

    public void setRootFolder(String rootFolder) {
        this.rootFolder = rootFolder;
    }

    public String getSourceFileExtention() {
        return sourceFileExtention;
    }

    public void setSourceFileExtention(String sourceFileExtention) {
        this.sourceFileExtention = sourceFileExtention;
    }

    public String getDestFileExtention() {
        return destFileExtention;
    }

    public void setDestFileExtention(String destFileExtention) {
        this.destFileExtention = destFileExtention;
    }

    public void startConvertion() throws IOException {
        convertFolder(new File(rootFolder));
    }

    protected void convertFolder(File folder) throws IOException {
        File[] files = folder.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    convertFolder(file);
                } else {
                    String name = file.getAbsolutePath();
                    if (name.endsWith('.' + sourceFileExtention)) {
                        convertFile(file);
                    }
                }
            }
        }
    }

    protected void convertFile(File file) throws IOException {
        try (FileInputStream in = new FileInputStream(file)) {
            File newFile = createNewFile(file);
            try (FileOutputStream out = new FileOutputStream(newFile)) {
                try (GzipCompressorOutputStream gout = new GzipCompressorOutputStream(out, getGzipParameters())) {
                    int i;
                    byte[] buff = new byte[bufferSize];
                    while ((i = in.read(buff)) != -1) {
                        gout.write(buff, 0, i);
                    }
                }
            }
        }
        deleteOldFile(file);
    }

    protected boolean deleteOldFile(File file) throws IOException {
        int attempts = 0;
        while (!file.delete()) {
            attempts++;
            if (attempts > 1000) {
                return false;
            }

            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        return true;
    }

    protected File createNewFile(File file) {
        String fileName = file.getAbsolutePath();
        return new File(fileName.substring(0, fileName.length() - sourceFileExtention.length()) + destFileExtention);
    }

    protected GzipParameters getGzipParameters() {
        GzipParameters parameters = new GzipParameters();
        parameters.setCompressionLevel(Deflater.BEST_COMPRESSION);
        return parameters;
    }
}
