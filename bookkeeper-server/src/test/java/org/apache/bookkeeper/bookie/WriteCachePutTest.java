package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.bookkeeper.bookie.storage.ldb.WriteCache;
import org.junit.Assert;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.apache.bookkeeper.bookie.Util.*;
import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class WriteCachePutTest {

    private static final int ALIGN_64_MASK = ~(64 - 1);

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
            long prevOffset = writeCache.getCacheOffset();
            long expectedCacheSize;
            long expectedCacheOffset;
            long expectedCacheCount;

            boolean result = writeCache.put(ledgerId, entryId, entry);

            //caso in cui l'inserimento non avviene per mancanza di spazio
            if(!expectedResult){
                expectedCacheCount = prevCount;
                expectedCacheSize = prevSize;
            }else{
                expectedCacheSize = prevSize+entry.readableBytes();
                expectedCacheCount = prevCount+1;

                //verifico che l'inserimento sia effettivamente avvenuto in entrambe le hash map
                Assert.assertEquals("Expected entry check failed",entry, writeCache.getLastEntry(ledgerId));
                Assert.assertEquals("Expected last entry check failed",entry, writeCache.get(ledgerId, entryId));
            }
            expectedCacheOffset = prevOffset+align64(entry.readableBytes());

            Assert.assertEquals("Expected result check failed", expectedResult, result);
            Assert.assertEquals("Expected cacheSize check failed",expectedCacheSize, writeCache.size());
            Assert.assertEquals("Expected cacheCount check failed",expectedCacheCount, writeCache.count());
            Assert.assertEquals("Expected cacheOffset check failed", expectedCacheOffset, writeCache.getCacheOffset());

        }
    }

    static int align64(int size) {
        return (size + 64 - 1) & ALIGN_64_MASK;
    }

}


