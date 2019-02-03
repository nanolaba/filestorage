package com.nanolaba.filestorage.memory;

import com.nanolaba.filestorage.AbstractStorageTest;

public class MemoryStorageTest extends AbstractStorageTest<MemoryStorage> {

    @Override
    protected MemoryStorage createStorage() throws Exception {
        return new MemoryStorage();
    }

}