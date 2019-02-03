package com.nanolaba.filestorage.gzip;

import com.nanolaba.filestorage.AbstractStorageTest;
import com.nanolaba.filestorage.memory.MemoryStorage;

public class GzipProxyMemoryStorageTest extends AbstractStorageTest<GzipProxyStorage> {

    @Override
    protected GzipProxyStorage createStorage() {
        GzipProxyStorage gzipStorage = new GzipProxyStorage();
        gzipStorage.setOriginalStorage(new MemoryStorage());
        return gzipStorage;
    }
}
