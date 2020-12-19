package com.nanolaba.filestorage;

import java.io.Serializable;

public class SaveResult implements Serializable {

    private static final long serialVersionUID = 1L;

    private final long bytesSaved;

    public SaveResult(long bytesSaved) {
        this.bytesSaved = bytesSaved;
    }

    public long getBytesSaved() {
        return bytesSaved;
    }
}
