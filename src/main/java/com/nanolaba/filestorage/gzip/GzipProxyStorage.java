package com.nanolaba.filestorage.gzip;

import com.nanolaba.filestorage.IStorage;
import com.nanolaba.filestorage.IStorageInfo;
import com.nanolaba.filestorage.StorageException;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipParameters;
import org.apache.commons.io.FileUtils;

import java.io.*;
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
    public void save(Long id, InputStream in) throws StorageException {

        try {
            File tempFile = File.createTempFile("gps", "gz");

            try (OutputStream out = new FileOutputStream(tempFile)) {
                try (GzipCompressorOutputStream gout = new GzipCompressorOutputStream(out, getGzipParameters())) {

                    int i;
                    byte[] buff = new byte[bufferSize];
                    while ((i = in.read(buff)) != -1) {
                        gout.write(buff, 0, i);
                    }
                    in.close();
                }

                try (FileInputStream fileInputStream = new FileInputStream(tempFile)) {
                    originalStorage.save(id, fileInputStream);    // TODO: 06.08.2017 можно избежать использование темпового файла если сделать метод сохранения с аппендом
                }
            } finally {
                FileUtils.deleteQuietly(tempFile);
            }
        } catch (IOException e) {
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
