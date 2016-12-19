package com.nanolaba.filestorage.util;

import com.nanolaba.filestorage.IStorage;
import com.nanolaba.filestorage.StorageException;
import com.nanolaba.filestorage.gzip.GzipProxyStorage;
import com.nanolaba.filestorage.plain.FileStorage;
import junit.framework.Assert;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class PlainToGzipConverterTest {

    static final int NUMBER_OF_FILES = 2000;

    @Test
    public void testConvertion() throws Exception {

        List<String> values = generateValues();

        String rootDirectory = "/opt/test/filestorage";
        File directory = new File(rootDirectory);
        if (directory.exists()) {
            FileUtils.deleteDirectory(directory);
        }

        FileStorage oldStorage = new FileStorage();
        oldStorage.setRootDirectory(rootDirectory);
        oldStorage.init();

        for (int i = 0; i < NUMBER_OF_FILES; ++i) {
            oldStorage.save((long) i, new ByteArrayInputStream(values.get(i).getBytes("UTF-8")));
            Assert.assertEquals(values.get(i), valueToString(oldStorage, i));
        }


        PlainToGzipConverter converter = new TestPlainToGzipConverter();
        converter.setRootFolder(rootDirectory);
        converter.startConvertion();


        for (int i = 0; i < NUMBER_OF_FILES; ++i) {
            Assert.assertFalse(oldStorage.isExists((long) i));
        }

        FileStorage newStorage = new FileStorage();
        newStorage.setRootDirectory(rootDirectory);
        newStorage.setDatafileExtension("gz");
        newStorage.init();
        newStorage.rebuild();

        GzipProxyStorage gzipStorage = new GzipProxyStorage();
        gzipStorage.setOriginalStorage(newStorage);


        for (int i = 0; i < NUMBER_OF_FILES; ++i) {
            Assert.assertEquals(values.get(i), valueToString(gzipStorage, i));
        }

    }

    private String valueToString(IStorage storage, long i) throws IOException, StorageException {
        try (InputStream input = storage.readAsStream(i)) {
            return IOUtils.toString(input, "UTF-8");
        }
    }

    private List<String> generateValues() {
        List<String> values = new ArrayList<>(NUMBER_OF_FILES);
        for (int i = 0; i < NUMBER_OF_FILES; ++i) {
            values.add(RandomStringUtils.random(10000));
        }
        return values;
    }

    private static class TestPlainToGzipConverter extends PlainToGzipConverter {
        @Override
        protected void convertFolder(File folder) throws IOException {
            super.convertFolder(folder);
            System.out.println("Folder converted: " + folder.getAbsolutePath());
        }

        @Override
        protected void convertFile(File file) throws IOException {
            super.convertFile(file);
            System.out.println("File converted: " + file.getAbsolutePath());
        }

        @Override
        protected void deleteOldFile(File file) throws IOException {
            super.deleteOldFile(file);
            System.out.println("File deleted: " + file.getAbsolutePath());
        }

        @Override
        protected File createNewFile(File file) {
            File newFile = super.createNewFile(file);
            System.out.println("File created: " + newFile.getAbsolutePath());
            return newFile;
        }
    }

}
