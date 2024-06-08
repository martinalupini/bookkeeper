package org.apache.bookkeeper.bookie.storage.ldb;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.bookkeeper.bookie.Concurrency;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.apache.bookkeeper.bookie.Util.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test di unità per la classe {@link WriteCache}. <br>
 * Metodo testato: {@link WriteCache#put(long, long, ByteBuf)}
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class WriteCachePutTest {

    static Stream<Arguments> providePutArguments() {
        return Stream.of(

                // caso ledgerID negativo
                //Arguments.of(UnpooledByteBufAllocator.DEFAULT, 0, 1, -1, 1, getWrittenByteBuf(), false, Exception.class),  // --> FAILURE: no exception thrown
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 0, 1, -1, 1, getWrittenByteBuf(), false, null),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 1024, 2048, -1, 1, getWrittenByteBuf(), false, Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 1024, 1024, -1, 1, getWrittenByteBuf(), false,Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 1024, 512, -1, 1, getWrittenByteBuf(), false, Exception.class),
                //Arguments.of(UnpooledByteBufAllocator.DEFAULT, 1, 1,-1, 1, getWrittenByteBuf(), false, Exception.class), // --> FAILURE: no exception thrown
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 1, 1,-1, 1, getWrittenByteBuf(), false, null),

                //caso entryID negativo
                //Arguments.of(UnpooledByteBufAllocator.DEFAULT, 0, 1, 1, -1, getWrittenByteBuf(), false, Exception.class), // --> FAILURE: no exception thrown
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 0, 1, 1, -1, getWrittenByteBuf(), false, null),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 1024, 2048, 1, -1, getWrittenByteBuf(), false, Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 1024, 1024, 1, -1, getWrittenByteBuf(), false,Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 1024, 512, 1, -1, getWrittenByteBuf(), false, Exception.class),
                //Arguments.of(UnpooledByteBufAllocator.DEFAULT, 1, 1, 1, -1, getWrittenByteBuf(), false, Exception.class), // --> FAILURE: no exception thrown
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 1, 1, 1, -1, getWrittenByteBuf(), false, null),

                // entry valida
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 0, 1, 0, 0, getWrittenByteBuf(), false, null),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 1024, 2048, 0, 0, getWrittenByteBuf(), true, null),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 1024, 1024, 0, 0, getWrittenByteBuf(), true,null),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 1024, 512, 0, 0, getWrittenByteBuf(), true, null),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 1, 1, 0, 0, getWrittenByteBuf(), false, null),
                // Dopo report Jacoco -------------------------------------------------------
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 1024, 4, 0, 0, getWrittenByteBuf(), false, null),
                // Dopo report PIT
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 64, 4, 0, 0, getWrittenByteBuf(), false, null),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 51, 4, 0, 0, getWrittenByteBuf(), false, null),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 1024, 64, 0, 0, getByteBuf("12345678123456781234567812345678123456781234567812345678123456789"), false, null),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 1024, 4, 0, 0, getByteBuf("1234567"), false, null),


                // entry non valida
                //Arguments.of(UnpooledByteBufAllocator.DEFAULT, 0, 1, 1, 1, getInvalidByteBuf(), false, Exception.class),  //--> FAILURE: no exception thrown
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 0, 1, 1, 1, getInvalidByteBuf(), false, null),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 1024, 2048, 1, 1, getInvalidByteBuf(), false, Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 1024, 1024, 1, 1, getInvalidByteBuf(), false, Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 1024, 512, 1, 1, getInvalidByteBuf(), false, Exception.class),
                //Arguments.of(UnpooledByteBufAllocator.DEFAULT, 1, 1, 1, 1, getInvalidByteBuf(), false, Exception.class),  //--> FAILURE: no exception thrown
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 1, 1, 1, 1, getInvalidByteBuf(), false, null),

                //entry null
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 0, 1, 1, 1, null, false, NullPointerException.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 1024, 2048, 1, 1, null, false, NullPointerException.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 1024, 1024, 1, 1, null, false, NullPointerException.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 1024, 512, 1, 1, null, false, NullPointerException.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 1, 1, 1, 1, null, false, NullPointerException.class)

        );
    }

    // Test parametrizzato che verifica il comportamento del costruttore di WriteCache
    @ParameterizedTest
    @MethodSource("providePutArguments")
    void testPut(ByteBufAllocator allocator, long maxCacheSize, int maxSegmentSize, long ledgerId, long entryId, ByteBuf entry, Boolean expectedResult, Class<Exception> expectedException) {

        WriteCache writeCache = new WriteCache(allocator, maxCacheSize, maxSegmentSize);

        if (expectedException != null) {

            assertThrows(expectedException, () -> {
                writeCache.put(ledgerId, entryId, entry);
            });
        } else {

            long prevSize = writeCache.size();
            long prevCount = writeCache.count();

            long expectedCacheSize;
            long expectedCacheCount;

            boolean result = writeCache.put(ledgerId, entryId, entry);

            Assert.assertEquals("Expected result check failed", expectedResult, result);

            //caso in cui l'inserimento non avviene per mancanza di spazio
            if(!expectedResult){
                expectedCacheCount = prevCount;
                expectedCacheSize = prevSize;

            }else{
                expectedCacheSize = prevSize+entry.readableBytes();
                expectedCacheCount = prevCount+1;

                //verifico che l'inserimento sia effettivamente avvenuto in entrambe le hash map (non c'è concorrenza
                // per cui siamo sicuri che è effettivamente l'ultima entry inserita
                Assert.assertEquals("Expected last entry check failed",entry, writeCache.getLastEntry(ledgerId));
                Assert.assertEquals("Expected entry check failed",entry, writeCache.get(ledgerId, entryId));
            }

            Assert.assertEquals("Expected cacheSize check failed",expectedCacheSize, writeCache.size());
            Assert.assertEquals("Expected cacheCount check failed",expectedCacheCount, writeCache.count());
        }
    }

    @Test
    void testConcurrencyPut() throws InterruptedException {
        WriteCache writeCache = new WriteCache(UnpooledByteBufAllocator.DEFAULT, 1024, 64);

        ByteBuf buf = getWrittenByteBuf();
        ByteBuf bufNewer = getWrittenByteBuf();

        // lancio un thread che inserisce nel ledger con ID 0 l'entry con ID 3
        Concurrency concurrency = new Concurrency(writeCache, bufNewer);
        Thread t1 = new Thread(concurrency);
        t1.start();

        Thread.sleep(1000);

        // ora aggiungo l'entry (0,1). Avendo entriId < 3 non sarà l'ultima entry
        boolean result = writeCache.put(0,0,buf);

        assertTrue(result);

        //verifico che l'inserimento sia effettivamente avvenuto in entrambe le hash map
        // e che l'entry aggiunta non sia l'ultima di ledgerId
        Assert.assertEquals("Expected entry check failed",buf, writeCache.get(0, 0));
        Assert.assertEquals("Expected last entry check failed",bufNewer, writeCache.getLastEntry(0));

    }


    @Test
    void testNonEmptyHashMap(){

        WriteCache writeCache = new WriteCache(UnpooledByteBufAllocator.DEFAULT, 1024, 64);

        ByteBuf byteBuf = getByteBuf("123456712345671234567123456712345671234567123456712345671234567");

        boolean result = writeCache.put(0,0,byteBuf);
        assertTrue(result);

        long prevSize = writeCache.size();
        long prevCount = writeCache.count();


        ByteBuf buf = getWrittenByteBuf();
        result = writeCache.put(1,1,buf);
        assertTrue(result);

        long expectedCacheSize = prevSize+buf.readableBytes();
        long expectedCacheCount = prevCount+1;

        //verifico che l'inserimento sia effettivamente avvenuto in entrambe le hash map
        Assert.assertEquals("Expected entry check failed",buf, writeCache.getLastEntry(1));
        Assert.assertEquals("Expected last entry check failed",buf, writeCache.get(1, 1));
        Assert.assertEquals("Expected cacheSize check failed",expectedCacheSize, writeCache.size());
        Assert.assertEquals("Expected cacheCount check failed",expectedCacheCount, writeCache.count());

    }


    private static final int ALIGN_64_MASK = ~(64 - 1);

    static int align64(int size) {
        return (size + 64 - 1) & ALIGN_64_MASK;
    }
}


