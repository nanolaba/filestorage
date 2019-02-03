package com.nanolaba.filestorage.gzip;

import com.nanolaba.filestorage.AbstractStorageTest;
import com.nanolaba.filestorage.StorageException;
import com.nanolaba.filestorage.plain.FileStorage;
import org.apache.commons.io.FileUtils;
import org.junit.After;

import java.io.File;
import java.io.IOException;

public class GzipProxyFileStorageTest extends AbstractStorageTest<GzipProxyStorage> {

    private FileStorage fileStorage;

    @Override
    protected GzipProxyStorage createStorage() throws StorageException, IOException {
        String rootDirectory = "filestorage";
        File directory = new File(rootDirectory);
        if (directory.exists()) {
            FileUtils.deleteDirectory(directory);
        }

        fileStorage = new FileStorage();
        fileStorage.setRootDirectory(rootDirectory);
        fileStorage.init();


        GzipProxyStorage gzipStorage = new GzipProxyStorage();
        gzipStorage.setOriginalStorage(fileStorage);
        return gzipStorage;
    }

    @After
    public void tearDown() throws Exception {
        FileUtils.deleteDirectory(new File(fileStorage.getRootDirectory()));
    }

}
