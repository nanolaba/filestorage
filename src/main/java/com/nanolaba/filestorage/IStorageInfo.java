package com.nanolaba.filestorage;

public interface IStorageInfo {

    long getFileCount() throws StorageException;

    long getTotalSize() throws StorageException;
}
