package com.nanolaba.filestorage.gzip;

import com.nanolaba.filestorage.IStorage;
import com.nanolaba.filestorage.IStorageInfo;
import com.nanolaba.filestorage.SaveResult;
import com.nanolaba.filestorage.StorageException;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipParameters;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.Deflater;
import java.util.zip.GZIPInputStream;

public class GzipProxyStorage implements IStorage {

    private int bufferSize = 1024 * 8;
    private int compressionLevel = Deflater.BEST_COMPRESSION;
    private IStorage originalStorage;
    private IStorage filesizeStorage;

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

    public IStorage getFilesizeStorage() {
        return filesizeStorage;
    }

    public void setFilesizeStorage(IStorage filesizeStorage) {
        this.filesizeStorage = filesizeStorage;
    }

    public void init() {

    }

    @Override
    public void rebuild() throws StorageException {
        originalStorage.rebuild();
    }

    @Override
    public SaveResult save(Long id, InputStream in) throws StorageException {

        try {

            CountDownLatch latch = new CountDownLatch(2);

            final PipedOutputStream output = new PipedOutputStream();
            final PipedInputStream input = new PipedInputStream(output);
            AtomicLong totalRead = new AtomicLong();

            Thread thread1 = new Thread(() -> {
                try (GzipCompressorOutputStream gout = new GzipCompressorOutputStream(output, getGzipParameters())) {

                    int i;
                    byte[] buff = new byte[bufferSize];
                    while ((i = in.read(buff)) != -1) {
                        gout.write(buff, 0, i);
                        if (i > 0) {
                            totalRead.addAndGet(i);
                        }
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
                    SaveResult result = originalStorage.save(id, input);
                    if (filesizeStorage != null) {
                        saveFilesize(id, result.getBytesSaved());
                    }
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

            return new SaveResult(totalRead.get());

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
        if (filesizeStorage != null) {
            filesizeStorage.delete(id);
        }
    }

    @Override
    public long size(Long id) throws StorageException {
        return filesizeStorage == null ? geFileSizeWithFullReading(id) : getFilesizeFromStorage(id);
    }

    private long getFilesizeFromStorage(Long id) throws StorageException {
        try {
            if (filesizeStorage.isExists(id)) {
                return Long.parseLong(IOUtils.toString(filesizeStorage.readAsStream(id), StandardCharsets.UTF_8));
            } else {
                long res = geFileSizeWithFullReading(id);
                saveFilesize(id, res);
                return res;
            }
        } catch (IOException e) {
            throw new StorageException("Can't read filesize", e, id);
        }
    }

    private void saveFilesize(Long id, long res) throws StorageException {
        filesizeStorage.save(id, new ByteArrayInputStream(String.valueOf(res).getBytes()));
    }

    private long geFileSizeWithFullReading(Long id) throws StorageException {
        try (GZIPInputStream zis = new GZIPInputStream(originalStorage.readAsStream(id))) {
            long size = 0L;
            byte[] buf = new byte[1024 * 8];
            while (zis.available() > 0) {
                int read = zis.read(buf);
                if (read > 0) {
                    size += read;
                }
            }

            return size;
        } catch (IOException e) {
            throw new StorageException("Can't read size", e, id);
        }
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
