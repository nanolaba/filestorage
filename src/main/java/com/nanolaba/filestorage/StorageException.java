package com.nanolaba.filestorage;

public class StorageException extends Exception {

    private final Long fileId;

    public StorageException() {
        fileId = null;
    }

    public StorageException(String message, Long fileId) {
        super(message);
        this.fileId = fileId;
    }

    public StorageException(String message, Throwable cause, Long fileId) {
        super(message, cause);
        this.fileId = fileId;
    }

    public StorageException(Throwable cause, Long fileId) {
        super(cause);
        this.fileId = fileId;
    }

    public Long getFileId() {
        return fileId;
    }
}
