package com.nanolaba.filestorage.gzip;

import com.nanolaba.filestorage.AbstractStorageTest;
import com.nanolaba.filestorage.StorageException;
import com.nanolaba.filestorage.plain.FileStorage;
import org.apache.commons.io.FileUtils;
import org.junit.After;

import java.io.File;
import java.io.IOException;

public class GzipProxyFileStorageWithFilesizeStorageTest extends AbstractStorageTest<GzipProxyStorage> {

    private FileStorage fileStorage;
    private FileStorage filesizeStorage;

    @Override
    protected GzipProxyStorage createStorage() throws StorageException, IOException {
        fileStorage = createFilestorage("filestorage");
        filesizeStorage = createFilestorage("fileSizeStorage");


        GzipProxyStorage gzipStorage = new GzipProxyStorage();
        gzipStorage.setOriginalStorage(fileStorage);
        return gzipStorage;
    }

    private static FileStorage createFilestorage(String rootDirectory) throws IOException, StorageException {
        File directory = new File(rootDirectory);
        if (directory.exists()) {
            FileUtils.deleteDirectory(directory);
        }

        FileStorage storage = new FileStorage();
        storage.setRootDirectory(rootDirectory);
        storage.init();
        return storage;
    }

    @After
    public void tearDown() throws Exception {
        FileUtils.deleteDirectory(new File(fileStorage.getRootDirectory()));
        FileUtils.deleteDirectory(new File(filesizeStorage.getRootDirectory()));
    }

}
