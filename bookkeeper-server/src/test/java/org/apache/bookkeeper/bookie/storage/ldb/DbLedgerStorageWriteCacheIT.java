package org.apache.bookkeeper.bookie.storage.ldb;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.ByteBuf;
import org.apache.bookkeeper.bookie.*;
import org.apache.bookkeeper.bookie.storage.EntryLogger;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.stats.StatsLogger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;


// methods addEntry and put
public class DbLedgerStorageWriteCacheIT {

    private DbLedgerStorage storage;
    private File tmpDir;
    protected static WriteCache spyWriteCache;

    private static class MockedDbLedgerStorage extends DbLedgerStorage {

        @Override
        protected SingleDirectoryDbLedgerStorage newSingleDirectoryDbLedgerStorage(ServerConfiguration conf,
                                                                                   LedgerManager ledgerManager, LedgerDirsManager ledgerDirsManager, LedgerDirsManager indexDirsManager,
                                                                                   EntryLogger entryLogger, StatsLogger statsLogger,
                                                                                   long writeCacheSize, long readCacheSize, int readAheadCacheBatchSize, long readAheadCacheBatchBytesSize)
                throws IOException {
            return new MockedSingleDirectoryDbLedgerStorage(conf, ledgerManager, ledgerDirsManager, indexDirsManager,
                    entryLogger, statsLogger, allocator, writeCacheSize,
                    readCacheSize, readAheadCacheBatchSize, readAheadCacheBatchBytesSize);
        }

        private static class MockedSingleDirectoryDbLedgerStorage extends SingleDirectoryDbLedgerStorage {
            public MockedSingleDirectoryDbLedgerStorage(ServerConfiguration conf, LedgerManager ledgerManager,
                                                        LedgerDirsManager ledgerDirsManager, LedgerDirsManager indexDirsManager, EntryLogger entryLogger,
                                                        StatsLogger statsLogger,
                                                        ByteBufAllocator allocator, long writeCacheSize,
                                                        long readCacheSize, int readAheadCacheBatchSize, long readAheadCacheBatchBytesSize)
                    throws IOException {
                super(conf, ledgerManager, ledgerDirsManager, indexDirsManager, entryLogger,
                        statsLogger, allocator, writeCacheSize, readCacheSize, readAheadCacheBatchSize,
                        readAheadCacheBatchBytesSize);

                spyWriteCache = spy(this.writeCache);
                this.writeCache = spyWriteCache;
            }
        }
    }

    @Before
    public void setup() throws Exception {
        tmpDir = File.createTempFile("bkTest", ".dir");
        tmpDir.delete();
        tmpDir.mkdir();
        File curDir = BookieImpl.getCurrentDirectory(tmpDir);
        BookieImpl.checkDirectoryStructure(curDir);

        int gcWaitTime = 1000;
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setGcWaitTime(gcWaitTime);
        conf.setLedgerStorageClass(MockedDbLedgerStorage.class.getName());
        conf.setProperty(DbLedgerStorage.WRITE_CACHE_MAX_SIZE_MB, 1);
        conf.setProperty(DbLedgerStorage.MAX_THROTTLE_TIME_MILLIS, 1000);
        conf.setLedgerDirNames(new String[] { tmpDir.toString() });
        Bookie bookie = new TestBookieImpl(conf);

        storage = (DbLedgerStorage) bookie.getLedgerStorage();
    }

    @After
    public void teardown() throws Exception {
        storage.shutdown();
        tmpDir.delete();
    }

    @Test
    public void writeCacheFull() throws Exception {

        // Add enough entries to fill the 1st write cache
        for (int i = 0; i < 5; i++) {
            ByteBuf entry = Unpooled.buffer(100 * 1024 + 2 * 8);
            entry.writeLong(4); // ledger id
            entry.writeLong(i); // entry id
            entry.writeZero(100 * 1024);
            storage.addEntry(entry);
        }

        verify(spyWriteCache, times(5)).put(anyLong(), anyLong(), any(ByteBuf.class));

/*
        for (int i = 0; i < 5; i++) {
            ByteBuf entry = Unpooled.buffer(100 * 1024 + 2 * 8);
            entry.writeLong(4); // ledger id
            entry.writeLong(5 + i); // entry id
            entry.writeZero(100 * 1024);
            storage.addEntry(entry);
        }

        // Next add should fail for cache full
        ByteBuf entry = Unpooled.buffer(100 * 1024 + 2 * 8);
        entry.writeLong(4); // ledger id
        entry.writeLong(22); // entry id
        entry.writeZero(100 * 1024);

 */


    }
}
