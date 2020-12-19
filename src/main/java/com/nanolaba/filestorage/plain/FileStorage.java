package com.nanolaba.filestorage.plain;

import com.nanolaba.filestorage.IStorage;
import com.nanolaba.filestorage.IStorageInfo;
import com.nanolaba.filestorage.SaveResult;
import com.nanolaba.filestorage.StorageException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;

public class FileStorage implements IStorage {

    private int maxSubdirNameLength = 3;
    private int bufferSize = 1024 * 8;
    private FileStorageInfo storageInfo;

    private String datafileExtension = "dt";

    private String rootDirectory;

    public int getMaxSubdirNameLength() {
        return maxSubdirNameLength;
    }

    public void setMaxSubdirNameLength(int maxSubdirNameLength) {
        this.maxSubdirNameLength = maxSubdirNameLength;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public String getRootDirectory() {
        return rootDirectory;
    }

    public String getDatafileExtension() {
        return datafileExtension;
    }

    public void setDatafileExtension(String datafileExtension) {
        this.datafileExtension = datafileExtension;
    }

    public void setRootDirectory(String rootDirectory) {
        this.rootDirectory = rootDirectory;
    }

    public void setRootDirectoryFile(File rootDirectory) {
        this.rootDirectory = rootDirectory.getAbsolutePath();
    }

    public void init() throws StorageException {
        storageInfo = createFileStorage();
    }

    protected FileStorageInfo createFileStorage() throws StorageException {
        return new FileStorageInfo(rootDirectory);
    }

    @Override
    public void rebuild() throws StorageException {

        storageInfo.clearCount();

        rebuild(new File(rootDirectory));
    }

    private void rebuild(File file) throws StorageException {
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            if (files != null) {
                for (File f : files) {
                    rebuild(f);
                }
            }
        } else if (file.getName().endsWith('.' + datafileExtension)) {
            storageInfo.increaseStorageSize(file.length());
        }
    }

    @Override
    public SaveResult save(Long id, InputStream in) throws StorageException {
        File file = getFileForId(rootDirectory, id);
        if (file.exists()) {
            delete(id);
        }
        try {
            file.getParentFile().mkdirs();

            try (RandomAccessFile raf = new RandomAccessFile(file, "rw"); FileChannel fc = raf.getChannel()) {
                final FileLock fl = fc.tryLock();
                if (fl == null) {
                    throw new StorageException("Can't lock file for writing " + file, id);
                } else {
                    try (final ReadableByteChannel byteChannel = Channels.newChannel(in)) {
                        for (final ByteBuffer buffer = ByteBuffer.allocate(bufferSize); byteChannel.read(buffer) != -1; ) {
                            buffer.flip();
                            fc.write(buffer);
                            buffer.clear();
                        }
                    } finally {
                        fl.release();
                    }
                }
            } finally {
                IOUtils.closeQuietly(in);
            }
        } catch (IOException e) {
            throw new StorageException("Can't read file for id '" + id + '\'', e, id);
        }
        long length = file.length();
        storageInfo.increaseStorageSize(length);
        return new SaveResult(length);
    }

    @Override
    public void read(Long id, OutputStream out) throws StorageException {
        File file = getFileForId(rootDirectory, id);
        if (file.exists()) {
            try {
                try (RandomAccessFile in = new RandomAccessFile(file, "r")) {
                    int i;
                    for (byte[] buff = new byte[bufferSize]; (i = in.read(buff)) != -1; ) {
                        out.write(buff, 0, i);
                    }
                }
            } catch (IOException e) {
                throw new StorageException("Can't read file for id '" + id + '\'', e, id);
            }
        } else {
            throw new StorageException("The file for id '" + id + "' does not exist", id);
        }
    }

    @Override
    public InputStream readAsStream(Long id) throws StorageException {
        File file = getFileForId(rootDirectory, id);
        if (file.exists()) {
            try {
                return new FileInputStream(file);
            } catch (IOException e) {
                throw new StorageException("Can't read file for id '" + id + '\'', e, id);
            }
        } else {
            throw new StorageException("The file for id '" + id + "' does not exist", id);
        }
    }

    @Override
    public void delete(Long id) throws StorageException {
        File file = getFileForId(rootDirectory, id);
        if (file.exists()) {
            long size = file.length();
            try {
                FileUtils.forceDelete(file);
            } catch (Exception e) {
                throw new StorageException("Can't delete file for id '" + id + '\'', e, id);
            }
            storageInfo.decreaseStorageSize(size);
        }
    }

    @Override
    public long size(Long id) throws StorageException {
        return getFileForId(rootDirectory, id).length();
    }

    @Override
    public boolean isExists(Long id) throws StorageException {
        return getFileForId(rootDirectory, id).exists();
    }

    @Override
    public IStorageInfo getStorageInfo() throws StorageException {
        return storageInfo;
    }

    public File getFileForId(String rootDirectory, Long id) {
        String serializedId = serializeId(id);
        StringBuilder path = new StringBuilder(rootDirectory);

        int i = 0;
        for (char c : serializedId.toCharArray()) {
            if (i % maxSubdirNameLength == 0) {
                path.append('/');
            }

            path.append(c);
            i++;
        }

        path.append('.').append(datafileExtension);

        return new File(new File(path.toString()).getAbsolutePath());
    }

    public String serializeId(Long id) {
        return String.valueOf(id);
    }
}
