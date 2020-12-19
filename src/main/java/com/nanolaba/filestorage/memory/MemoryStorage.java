package com.nanolaba.filestorage.memory;

import com.nanolaba.filestorage.IStorage;
import com.nanolaba.filestorage.IStorageInfo;
import com.nanolaba.filestorage.SaveResult;
import com.nanolaba.filestorage.StorageException;
import org.apache.commons.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

public class MemoryStorage implements IStorage {

    private final Map<Long, byte[]> data = new HashMap<>();
    private long totalSize;

    @Override
    public void rebuild() throws StorageException {

    }

    @Override
    public SaveResult save(Long id, InputStream in) throws StorageException {
        try {
            byte[] value = IOUtils.toByteArray(in);
            data.put(id, value);
            totalSize += (long) value.length;
            return new SaveResult(value.length);
        } catch (IOException e) {
            throw new StorageException(e, id);
        }
    }

    @Override
    public void read(Long id, OutputStream out) throws StorageException {
        if (isExists(id)) {
            try {
                IOUtils.write(data.get(id), out);
            } catch (IOException e) {
                throw new StorageException(e, id);
            }
        } else {
            throw new StorageException("The file for id '" + id + "' does not exist", id);
        }
    }

    @Override
    public InputStream readAsStream(Long id) throws StorageException {
        if (isExists(id)) {
            return new ByteArrayInputStream(data.get(id));
        } else {
            throw new StorageException("The file for id '" + id + "' does not exist", id);
        }
    }

    @Override
    public void delete(Long id) throws StorageException {
        if (isExists(id)) {
            totalSize -= (long) data.get(id).length;
            data.remove(id);
        }
    }

    @Override
    public long size(Long id) throws StorageException {
        return data.get(id).length;
    }

    @Override
    public boolean isExists(Long id) throws StorageException {
        return data.containsKey(id);
    }

    @Override
    public IStorageInfo getStorageInfo() throws StorageException {
        return new IStorageInfo() {
            @Override
            public long getFileCount() throws StorageException {
                return data.size();
            }

            @Override
            public long getTotalSize() throws StorageException {
                return totalSize;
            }
        };
    }
}
