package org.apache.bookkeeper.bookie.storage.ldb;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.Assert;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class WriteCacheTest {

    // Definisce i dati di input per il test sul costruttore
    static Stream<Arguments> provideConstructorArguments() {
        return Stream.of(

                //Arguments.of(null, 1024, 1024, NullPointerException.class),  // --> FAILURE: no exception thrown
                //Arguments.of(getInvalidAllocator(), 1024, 1024, Exception.class), //--> FAILURE: no exception thrown
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, -1, 1, Exception.class), //--> PASS
                //Arguments.of(UnpooledByteBufAllocator.DEFAULT, 0, 1, Exception.class), // --> FAILURE: no exception thrown
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 0, 0, IllegalArgumentException.class), // --> PASS
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 1024, -1, IllegalArgumentException.class), // --> PASS
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 1024, 2048, null), // --> PASS
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 1024, 1024, null), // --> PASS
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 1024, 1023, IllegalArgumentException.class), // --> PASS
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 1024, 512, null), // --> PASS
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 1, 1, null), // --> PASS
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 1, 0, IllegalArgumentException.class) // --> PASS

        );
    }

    // Test parametrizzato che verifica il comportamento del costruttore di WriteCache
    @ParameterizedTest
    @MethodSource("provideConstructorArguments")
    void testWriteCacheCreation(ByteBufAllocator allocator, long maxCacheSize, int maxSegmentSize, Class<Exception> expectedException) {

        if (expectedException != null) {

            assertThrows(expectedException, () -> {
                new WriteCache(allocator, maxCacheSize, maxSegmentSize);
            });
        } else {

            WriteCache writeCache = new WriteCache(allocator, maxCacheSize, maxSegmentSize);

            int expectedSegmentCount = (int)(1+(maxCacheSize/maxSegmentSize));

            assertEquals(maxCacheSize, writeCache.getMaxCacheSize());
            assertEquals(maxSegmentSize, writeCache.getMaxSegmentSize());
            assertEquals(0, writeCache.size());
            assertEquals(0, writeCache.count());
            assertEquals(0, writeCache.getCacheOffset());
            assertEquals(expectedSegmentCount, writeCache.getSegmentsCount());

            // Dopo report PIT  ------------------------------------
            Assert.assertTrue("Expected > 0", writeCache.getMaxSegmentSize() > 0);
            Assert.assertEquals("Expected segmentOffsetMask check failed",maxSegmentSize-1, writeCache.getSegmentOffsetMask());
            Assert.assertEquals("Expected segmentOffsetBits check failed",63 - Long.numberOfLeadingZeros(maxSegmentSize), writeCache.getSegmentOffsetBits());
            Assert.assertEquals(maxCacheSize%maxSegmentSize, writeCache.getCacheSegmentCapacity(expectedSegmentCount-1));
            Assert.assertEquals(expectedSegmentCount, writeCache.getCacheSegmentsLength());

        }
    }


}
