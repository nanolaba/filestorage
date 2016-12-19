package com.nanolaba.filestorage.plain;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;

public class FileStorageTest {

    private FileStorage fileStorage;

    @Before
    public void setUp() throws Exception {

        String rootDirectory = "/opt/test/filestorage";
        File directory = new File(rootDirectory);
        if (directory.exists()) {
            FileUtils.deleteDirectory(directory);
        }

        fileStorage = new FileStorage();
        fileStorage.setRootDirectory(rootDirectory);
        fileStorage.init();
    }

    @After
    public void tearDown() throws Exception {
        FileUtils.deleteDirectory(new File(fileStorage.getRootDirectory()));
    }

    @Test
    public void saveTest() throws Exception {
        String data = "123";

        Assert.assertFalse(fileStorage.isExists(1L));
        fileStorage.save(1L, new ByteArrayInputStream(data.getBytes()));

        Assert.assertTrue(fileStorage.isExists(1L));
        try (InputStream input = fileStorage.readAsStream(1L)) {
            Assert.assertEquals(data, IOUtils.toString(input));
        }

        fileStorage.delete(1L);
        Assert.assertFalse(fileStorage.isExists(1L));
    }
}
