package com.nanolaba.filestorage.gzip;

import com.nanolaba.filestorage.IStorage;
import com.nanolaba.filestorage.IStorageInfo;
import com.nanolaba.filestorage.StorageException;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;

import java.io.*;

public class GzipProxyStorage implements IStorage {


    private int bufferSize = 1024 * 8;

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

    public void init() {

    }

    @Override
    public void rebuild() throws StorageException {
        originalStorage.rebuild();
    }

    @Override
    public void save(Long id, InputStream in) throws StorageException {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            GzipCompressorOutputStream gout = new GzipCompressorOutputStream(out);

            int i;
            byte[] buff = new byte[bufferSize];
            while ((i = in.read(buff)) != -1) {
                gout.write(buff, 0, i);
            }
            in.close();
            gout.close();

            originalStorage.save(id, new ByteArrayInputStream(out.toByteArray()));
        } catch (IOException e) {
            throw new StorageException("Can't save file", e, id);
        }
    }

    @Override
    public void read(Long id, OutputStream out) throws StorageException {
        try {
            InputStream in = readAsStream(id);
            int i;
            byte[] buff = new byte[bufferSize];
            while ((i = in.read(buff)) != -1) {
                out.write(buff, 0, i);
            }
            in.close();
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
