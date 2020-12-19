package com.nanolaba.filestorage;

import com.nanolaba.filestorage.gzip.GzipProxyStorage;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Random;

public abstract class AbstractStorageTest<T extends IStorage> {

    protected abstract T createStorage() throws Exception;

    private T storage;


    @Before
    public void setUp() throws Exception {
        storage = createStorage();
    }

    public T getStorage() {
        return storage;
    }

    @Test
    public void saveTest() throws Exception {
        String data = "123";

        Assert.assertFalse(storage.isExists(1L));
        byte[] bytes = data.getBytes();
        storage.save(1L, new ByteArrayInputStream(bytes), bytes.length);

        Assert.assertTrue(storage.isExists(1L));
        try (InputStream input = storage.readAsStream(1L)) {
            Assert.assertEquals(data, IOUtils.toString(input));
        }

        Assert.assertEquals((long) data.length(), storage.size(1L));

        storage.delete(1L);
        Assert.assertFalse(storage.isExists(1L));
    }


    //    @Ignore
    @Test
    public void hugeFileTest() throws Exception {

        int FILE_SIZE_IN_MB = 500;
        int MB = 1024 * 1024;

        int buff_size = 20 * MB;

        if (storage instanceof GzipProxyStorage) {
            ((GzipProxyStorage) storage).setBufferSize(buff_size);
        }

        File testFile = File.createTempFile("test", ".huge");
        FileUtils.deleteQuietly(testFile);
        try {
            try (RandomAccessFile raf = new RandomAccessFile(testFile, "rw"); FileChannel fc = raf.getChannel()) {
                ByteBuffer buffer = ByteBuffer.allocate(buff_size);
                for (int i = 0; i < FILE_SIZE_IN_MB * MB / buff_size; ++i) {
                    new Random().nextBytes(buffer.array());
                    fc.write(buffer);
                    System.out.println(i * buff_size / MB + " Mb");
                }
            }
            System.out.println("Test file created");


            long fileId = 1L;
            Assert.assertFalse(storage.isExists(fileId));

            try (InputStream in = new FileInputStream(testFile)) {
                storage.save(fileId, in, testFile.length());
            }


            System.out.println("Data saved");
            System.out.println("Check data equality...");

            Assert.assertTrue(storage.isExists(fileId));
            try (InputStream input = storage.readAsStream(fileId)) {
                try (RandomAccessFile fileInput = new RandomAccessFile(testFile, "r")) {
                    ByteBuffer buffer = ByteBuffer.allocate(buff_size);
                    byte[] buffFile = new byte[buff_size];
                    for (int i = 0; i < FILE_SIZE_IN_MB * MB / buff_size; ++i) {
                        input.read(buffer.array());
                        fileInput.read(buffFile);
                        Assert.assertTrue(Arrays.equals(buffer.array(), buffFile));
                        System.out.println(i * buff_size / MB + " Mb");
                    }
                }
            }

            Assert.assertEquals((long) testFile.length(), storage.size(fileId));

            storage.delete(fileId);
            Assert.assertFalse(storage.isExists(fileId));
        } finally {
            FileUtils.deleteQuietly(testFile);
        }
    }
}


