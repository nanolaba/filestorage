package com.nanolaba.filestorage.berkeleydb;

import com.nanolaba.filestorage.IStorage;
import com.nanolaba.filestorage.IStorageInfo;
import com.nanolaba.filestorage.StorageException;
import com.nanolaba.filestorage.plain.FileStorageInfo;
import com.sleepycat.je.*;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class BerkeleyDbStorage implements IStorage {


    public static final int LONG_SIZE = 8;
    private Database database;
    private Environment environment;

    public void init() throws StorageException {

        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setSharedCache(true);
        envConfig.setLocking(false);
        envConfig.setReadOnly(false);
        envConfig.setTransactional(false);
        envConfig.setDurability(Durability.COMMIT_WRITE_NO_SYNC);
        envConfig.setConfigParam(EnvironmentConfig.LOG_FILE_MAX, "1073741824");

        File envSimpleFile = new File(getRootDirectory());
        envSimpleFile.mkdirs();

        environment = new Environment(envSimpleFile, envConfig);

        DatabaseConfig databaseConfig = new DatabaseConfig();
        databaseConfig.setAllowCreate(true);
        databaseConfig.setReadOnly(false);
        databaseConfig.setTransactional(false);
        databaseConfig.setTemporary(false);
        databaseConfig.setDeferredWrite(true);

        database = environment.openDatabase(null, "fileBase", databaseConfig);

        environment.cleanLog();
        environment.compress();

    }

    private String rootDirectory;

    public String getRootDirectory() {
        return rootDirectory;
    }

    public void setRootDirectory(String rootDirectory) {
        this.rootDirectory = rootDirectory;
    }

    public void setRootDirectoryFile(File rootDirectory) {
        this.rootDirectory = rootDirectory.getAbsolutePath();
    }

    public void close() {
        environment.cleanLog();
        environment.compress();
        database.close();
        environment.close();
    }

    protected FileStorageInfo createFileStorage() throws StorageException {
        return new FileStorageInfo(rootDirectory);
    }

    @Override
    public void rebuild() throws StorageException {/**/}

    @Override
    public void save(Long id, InputStream in) throws StorageException {
        try {
            database.put(null,
                    new DatabaseEntry(idToBytes(id)),
                    new DatabaseEntry(IOUtils.toByteArray(in)));
            database.sync();
            environment.compress();
        } catch (IOException e) {
            throw new StorageException("Can't save data", e, id);
        }
    }

    @Override
    public void read(Long id, OutputStream out) throws StorageException {
        DatabaseEntry data = new DatabaseEntry();
        database.get(null, new DatabaseEntry(idToBytes(id)), data, LockMode.DEFAULT);
        if (data.getData() != null) {
            try {
                out.write(data.getData());
            } catch (IOException e) {
                throw new StorageException("Can't read file for id '" + id + '\'', e, id);
            }
        } else {
            throw new StorageException("The file for id '" + id + "' does not exist", id);
        }
    }

    @Override
    public InputStream readAsStream(Long id) throws StorageException {

        DatabaseEntry data = new DatabaseEntry();
        database.get(null, new DatabaseEntry(idToBytes(id)), data, LockMode.DEFAULT);
        if (data.getData() != null) {
            return new ByteArrayInputStream(data.getData());
        } else {
            throw new StorageException("The file for id '" + id + "' does not exist", id);
        }
    }

    @Override
    public void delete(Long id) throws StorageException {
        database.delete(null, new DatabaseEntry(idToBytes(id)));
        database.sync();
        environment.compress();
    }

    @Override
    public boolean isExists(Long id) throws StorageException {
        DatabaseEntry data = new DatabaseEntry();
        database.get(null, new DatabaseEntry(idToBytes(id)), data, LockMode.DEFAULT);
        return data.getData() != null;
    }

    @Override
    public IStorageInfo getStorageInfo() throws StorageException {
        throw new UnsupportedOperationException();
    }

    private byte[] idToBytes(Long id) {
        byte[] array = ByteBuffer.allocate(LONG_SIZE).putLong(id).array();

        int ii = -1;
        for (int i = 0; i < array.length; ++i) {
            if (array[i] == 0) {
                ii = i;
            } else {
                break;
            }
        }

        if (ii >= 0) {
            array = Arrays.copyOfRange(array, ii, array.length);
        }

        return array;
    }
}
