package com.nanolaba.filestorage.plain;

import com.nanolaba.filestorage.IStorage;
import com.nanolaba.filestorage.IStorageInfo;
import com.nanolaba.filestorage.StorageException;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.util.LinkedList;
import java.util.List;

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
            if (files != null) for (File f : files) {
                rebuild(f);
            }
        } else if (file.getName().endsWith("." + datafileExtension)) {
            storageInfo.increaseStorageSize(file.length());
        }
    }

    @Override
    public void save(Long id, InputStream in) throws StorageException {
        File file = getFileForId(rootDirectory, id);
        if (file.exists()) {
            FileUtils.deleteQuietly(file);
        }
        long size = 0L;
        try {
            file.getParentFile().mkdirs();
            FileOutputStream out = new FileOutputStream(file);
            try {
                int i;
                byte[] buff = new byte[bufferSize];
                while ((i = in.read(buff)) != -1) {
                    size += (long) i;
                    out.write(buff, 0, i);
                }
            } finally {
                out.close();
            }
        } catch (IOException e) {
            throw new StorageException("Can't read file for id '" + id + '\'', e, id);
        }
        storageInfo.increaseStorageSize(size);
    }

    @Override
    public void read(Long id, OutputStream out) throws StorageException {
        File file = getFileForId(rootDirectory, id);
        if (file.exists()) {
            try {
                FileInputStream in = new FileInputStream(file);
                try {
                    int i;
                    byte[] buff = new byte[bufferSize];
                    while ((i = in.read(buff)) != -1) {
                        out.write(buff, 0, i);
                    }
                } finally {
                    in.close();
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
            FileUtils.deleteQuietly(file);
            storageInfo.decreaseStorageSize(size);
        }
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

        path.append("." + datafileExtension);

        File res = new File(new File(path.toString()).getAbsolutePath());
        return res;
    }

    public static String serializeId(Long id) {
        return String.valueOf(id);
    }


    protected Iterable<Long> getAllIds() {
        return getAllIds(new File(rootDirectory));
    }

    private Iterable<Long> getAllIds(File root) {
        List<Long> res = new LinkedList<Long>();

        File[] files = root.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    for (Long id : getAllIds(file)) {
                        res.add(id);
                    }
                } else {
                    String name = file.getAbsolutePath();
                    if (name.endsWith("." + datafileExtension)) {
                        name = getTextBetweenWords(name, new File(rootDirectory).getAbsolutePath(), "." + datafileExtension);
                        name = name.replace("/", "").replace("\\", "");
                        res.add(Long.valueOf(name));
                    }
                }
            }
        }

        return res;
    }

    private static String getTextBetweenWords(String source, String begin, String end) {
        String res = null;
        if (source != null && begin != null && end != null) {
            int firstLetterIndex = source.indexOf(begin);
            if (firstLetterIndex >= 0) {
                firstLetterIndex += begin.length();

                int lastLetterIndex = source.indexOf(end, firstLetterIndex);
                if (lastLetterIndex >= 0 && firstLetterIndex <= lastLetterIndex) {
                    res = source.substring(firstLetterIndex, lastLetterIndex);
                }
            }
        }
        return res;
    }
}
