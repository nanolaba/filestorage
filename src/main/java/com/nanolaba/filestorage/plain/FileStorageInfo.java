package com.nanolaba.filestorage.plain;

import com.nanolaba.filestorage.IStorageInfo;
import com.nanolaba.filestorage.StorageException;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class FileStorageInfo implements IStorageInfo {

    public static final String PROPERTY_FILE_COUNT = "size";
    public static final String PROPERTY_TOTAL_SIZE = "totalsize";
    public static final String PROPERTY_LAST_UPDATE = "lastUpdate";

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss");

    private final File root;
    private final File propertiesFile;
    private final Properties properties = new Properties();

    public FileStorageInfo(String rootDirectory) throws StorageException {
        root = new File(rootDirectory);
        propertiesFile = new File(new File(rootDirectory + "/info.properties").getAbsolutePath());

        try {

            if (!propertiesFile.exists()) {
                propertiesFile.getParentFile().mkdirs();
                propertiesFile.createNewFile();
            }

            try (FileInputStream stream = new FileInputStream(propertiesFile)) {
                properties.load(stream);
            }
        } catch (IOException e) {
            throw new StorageException(e, null);
        }
    }

    @Override
    public long getFileCount() throws StorageException {
        return Long.valueOf(properties.getProperty(PROPERTY_FILE_COUNT, "0"));
    }

    @Override
    public long getTotalSize() throws StorageException {
        return Long.valueOf(properties.getProperty(PROPERTY_TOTAL_SIZE, "0"));
    }

    public void increaseStorageSize(long size) throws StorageException {
        properties.setProperty(PROPERTY_FILE_COUNT, String.valueOf(getFileCount() + 1));
        properties.setProperty(PROPERTY_TOTAL_SIZE, String.valueOf(getTotalSize() + size));
        saveFile();
    }

    public void decreaseStorageSize(long size) throws StorageException {
        properties.setProperty(PROPERTY_FILE_COUNT, String.valueOf(getFileCount() - 1));
        properties.setProperty(PROPERTY_TOTAL_SIZE, String.valueOf(getTotalSize() - size));
        saveFile();
    }

    private void saveFile() throws StorageException {
        try {
            properties.setProperty(PROPERTY_LAST_UPDATE, DATE_FORMAT.format(new Date()));

            try (FileOutputStream outputStream = new FileOutputStream(propertiesFile)) {
                properties.store(outputStream, "");
            }
        } catch (IOException e) {
            throw new StorageException(e, null);
        }
    }

    public void clearCount() {
        properties.setProperty(PROPERTY_FILE_COUNT, String.valueOf(0));
        properties.setProperty(PROPERTY_TOTAL_SIZE, String.valueOf(0));
    }

    public File getRoot() {
        return root;
    }

    public File getPropertiesFile() {
        return propertiesFile;
    }

    public Properties getProperties() {
        return properties;
    }
}
