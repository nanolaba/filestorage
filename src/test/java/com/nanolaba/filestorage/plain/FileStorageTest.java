package com.nanolaba.filestorage.plain;

import com.nanolaba.filestorage.AbstractStorageTest;
import org.apache.commons.io.FileUtils;
import org.junit.After;

import java.io.File;

public class FileStorageTest extends AbstractStorageTest<FileStorage> {

    @After
    public void tearDown() throws Exception {
        FileUtils.deleteDirectory(new File(getStorage().getRootDirectory()));
    }

    @Override
    protected FileStorage createStorage() throws Exception {


        String rootDirectory = "filestorage";
        File directory = new File(rootDirectory);

        if (directory.exists()) {
            FileUtils.deleteDirectory(directory);
        }

        FileStorage fileStorage = new FileStorage();
        fileStorage.setRootDirectory(rootDirectory);
        fileStorage.init();

        return fileStorage;
    }
}
