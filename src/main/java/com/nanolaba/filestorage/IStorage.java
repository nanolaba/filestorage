package com.nanolaba.filestorage;

import java.io.InputStream;
import java.io.OutputStream;

public interface IStorage {

    void rebuild() throws StorageException;

    void save(Long id, InputStream in, long size) throws StorageException;

    void read(Long id, OutputStream out) throws StorageException;

    InputStream readAsStream(Long id) throws StorageException;

    void delete(Long id) throws StorageException;

    boolean isExists(Long id) throws StorageException;

    IStorageInfo getStorageInfo() throws StorageException;
}
