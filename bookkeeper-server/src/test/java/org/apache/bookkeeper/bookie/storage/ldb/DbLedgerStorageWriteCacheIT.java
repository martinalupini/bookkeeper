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

import java.io.File;
import java.io.IOException;

import static org.mockito.Mockito.*;


/**
 *  Test di integrazione tra le classi {@link DbLedgerStorage} e {@link WriteCache}. <br>
 *  Metodi coinvolti: {@link DbLedgerStorage#addEntry(ByteBuf)} e {@link WriteCache#put(long, long, ByteBuf)}
 */
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

                // In questo modo è possibile tenere traccia delle interazioni di write cache
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

        // Otteniamo il riferimento all'istanza di DbLedgerStorage
        storage = (DbLedgerStorage) bookie.getLedgerStorage();
    }

    @After
    public void teardown() throws Exception {
        storage.shutdown();
        tmpDir.delete();
    }

    public ByteBuf getBuf(long ledgerId, long entryId){
        ByteBuf entry = Unpooled.buffer(10 * 1024 + 2 * 8);
        entry.writeLong(ledgerId);
        entry.writeLong(entryId);
        entry.writeZero(10 * 1024);

        return entry;
    }

    @Test
    public void testReachability() {

        try {

            ByteBuf entry = getBuf(4, 0);
            storage.addEntry(entry);
            verify(spyWriteCache, times(1)).put(4, 0, entry);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testInteractionStub() {

        try{

            // creo uno stub tramite spyWriteCache. Faccio in modo che put() ritorni sempre true
            doReturn(true).when(spyWriteCache).put(anyLong(), anyLong(), any());
            ByteBuf entry = getBuf(4, 0);
            storage.addEntry(entry);
            verify(spyWriteCache, times(1)).put(4, 0, entry);

            // ora modifico lo stub in modo che put() ritorni false alla prima invocazione e true alla seconda
            doReturn(false, true).when(spyWriteCache).put(anyLong(), anyLong(), any());
            entry = getBuf(4, 1);
            storage.addEntry(entry);
            verify(spyWriteCache, times(2)).put(4, 1, entry);


        } catch (Exception e){
            throw new RuntimeException(e);
        }

    }

    @Test
    public void testInteractionEmptyCache(){

        try{

            ByteBuf entry = getBuf(4, 0);
            storage.addEntry(entry);
            verify(spyWriteCache, times(1)).put(4, 0, entry);

        } catch (Exception e){
            throw new RuntimeException(e);
        }

    }

    @Test
    public void testInteractionFullCache() {

        try{
            ByteBuf entry;

            // Aggiungiamo entries per riempire la cache
            for (int i = 0; i < 5; i++) {
                entry = Unpooled.buffer(100 * 1024 + 2 * 8);
                entry.writeLong(4); // ledger id
                entry.writeLong(i); // entry id
                entry.writeZero(100 * 1024);
                storage.addEntry(entry);
            }

            entry = Unpooled.buffer(100 * 1024 + 2 * 8);
            entry.writeLong(4); // ledger id
            entry.writeLong(5); // entry id
            entry.writeZero(100 * 1024);
            storage.addEntry(entry);


            // conto solo il numero di volte in cui viene invocata la put della entry con ledgerId 4 e entryId 5
            verify(spyWriteCache, atLeast(2)).put(4, 5, entry);

        } catch (Exception e){
            throw new RuntimeException(e);
        }

    }



}
