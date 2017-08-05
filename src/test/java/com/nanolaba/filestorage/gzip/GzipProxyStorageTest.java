package com.nanolaba.filestorage.gzip;

import com.nanolaba.filestorage.plain.FileStorage;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.*;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Random;

public class GzipProxyStorageTest {

    private FileStorage fileStorage;
    private GzipProxyStorage gzipStorage;

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


        gzipStorage = new GzipProxyStorage();
        gzipStorage.setOriginalStorage(fileStorage);
    }

    @After
    public void tearDown() throws Exception {
        FileUtils.deleteDirectory(new File(fileStorage.getRootDirectory()));
    }

    @Test
    public void saveTest() throws Exception {
        String data = "123";

        Assert.assertFalse(gzipStorage.isExists(1L));
        gzipStorage.save(1L, new ByteArrayInputStream(data.getBytes()));

        Assert.assertTrue(gzipStorage.isExists(1L));
        try (InputStream input = gzipStorage.readAsStream(1L)) {
            Assert.assertEquals(data, IOUtils.toString(input));
        }

        gzipStorage.delete(1L);
        Assert.assertFalse(gzipStorage.isExists(1L));
    }

    @Ignore
    @Test
    public void hugeFileTest() throws Exception {

        int FILE_SIZE_IN_GB = 2;
        int MB = 1024 * 1024;
        int GB = MB * 1024;

        int buff_size = 20 * MB;
        gzipStorage.setBufferSize(buff_size);

        File testFile = new File("/opt/test/huge_file_test");
        FileUtils.deleteQuietly(testFile);
        try {
            byte[] buff = new byte[buff_size];

            for (int i = 0; i < FILE_SIZE_IN_GB * (GB / buff_size); ++i) {
                new Random().nextBytes(buff);
                FileUtils.writeByteArrayToFile(testFile, buff, true);
                System.out.println(i * buff_size / MB + " Mb");
            }
            System.out.println("Test file created");


            long fileId = 1L;
            Assert.assertFalse(gzipStorage.isExists(fileId));

            try (InputStream in = new FileInputStream(testFile)) {
                gzipStorage.save(fileId, in);
            }


            System.out.println("Data saved");
            System.out.println("Check data equality...");

            Assert.assertTrue(gzipStorage.isExists(fileId));
            try (InputStream input = gzipStorage.readAsStream(fileId)) {
                try (InputStream fileInput = FileUtils.openInputStream(testFile)) {

                    byte[] buffFile = new byte[buff_size];
                    for (int i = 0; i < FILE_SIZE_IN_GB * (GB / buff_size); ++i) {
                        input.read(buff);
                        fileInput.read(buffFile);
                        Assert.assertArrayEquals(buff, buffFile);
                    }
                }
            }

            gzipStorage.delete(fileId);
            Assert.assertFalse(gzipStorage.isExists(fileId));
        } finally {
            FileUtils.deleteQuietly(testFile);
        }
    }
}
