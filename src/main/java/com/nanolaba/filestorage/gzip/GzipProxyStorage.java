package com.nanolaba.filestorage.gzip;

import com.nanolaba.filestorage.IStorage;
import com.nanolaba.filestorage.IStorageInfo;
import com.nanolaba.filestorage.StorageException;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipParameters;

import java.io.*;
import java.util.concurrent.CountDownLatch;
import java.util.zip.Deflater;

public class GzipProxyStorage implements IStorage {

    private int bufferSize = 1024 * 8;
    private int compressionLevel = Deflater.BEST_COMPRESSION;
    private IStorage originalStorage;

    public IStorage getOriginalStorage() {
        return originalStorage;
    }

    public void setOriginalStorage(IStorage originalStorage) {
        this.originalStorage = originalStorage;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public int getCompressionLevel() {
        return compressionLevel;
    }

    public void setCompressionLevel(int compressionLevel) {
        this.compressionLevel = compressionLevel;
    }

    public void init() {

    }

    @Override
    public void rebuild() throws StorageException {
        originalStorage.rebuild();
    }

    @Override
    public void save(Long id, InputStream in, long size) throws StorageException {

        try {


            CountDownLatch latch = new CountDownLatch(2);

            final PipedOutputStream output = new PipedOutputStream();
            final PipedInputStream input = new PipedInputStream(output);


            Thread thread1 = new Thread(() -> {
                try (GzipCompressorOutputStream gout = new GzipCompressorOutputStream(output, getGzipParameters())) {

                    int i;
                    byte[] buff = new byte[bufferSize];
                    while ((i = in.read(buff)) != -1) {
                        gout.write(buff, 0, i);
                    }
                    in.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                } finally {
                    try {
                        output.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    latch.countDown();
                }
            });


            Thread thread2 = new Thread(() -> {
                try {
                    originalStorage.save(id, input, size);
                } catch (StorageException e) {
                    throw new RuntimeException(e);
                } finally {
                    try {
                        input.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    latch.countDown();
                }
            });

            thread1.start();
            thread2.start();

            latch.await();

        } catch (IOException | InterruptedException e) {
            throw new StorageException("Can't save file", e, id);
        }
    }

    private GzipParameters getGzipParameters() {
        GzipParameters parameters = new GzipParameters();
        parameters.setCompressionLevel(compressionLevel);
        return parameters;
    }

    @Override
    public void read(Long id, OutputStream out) throws StorageException {
        try (InputStream in = readAsStream(id)) {
            int i;
            byte[] buff = new byte[bufferSize];
            while ((i = in.read(buff)) != -1) {
                out.write(buff, 0, i);
            }
        } catch (IOException e) {
            throw new StorageException("Can't read file", e, id);
        }
    }

    @Override
    public InputStream readAsStream(Long id) throws StorageException {
        try {
            InputStream inputStream = originalStorage.readAsStream(id);
            return new GzipCompressorInputStream(inputStream);
        } catch (IOException e) {
            throw new StorageException("Can't read file", e, id);
        }
    }

    @Override
    public void delete(Long id) throws StorageException {
        originalStorage.delete(id);
    }

    @Override
    public boolean isExists(Long id) throws StorageException {
        return originalStorage.isExists(id);
    }

    @Override
    public IStorageInfo getStorageInfo() throws StorageException {
        return originalStorage.getStorageInfo();
    }
}
