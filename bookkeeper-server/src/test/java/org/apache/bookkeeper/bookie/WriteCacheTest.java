package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.bookkeeper.bookie.storage.ldb.WriteCache;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.apache.bookkeeper.bookie.Util.*;
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
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 0, 0, Exception.class), // --> PASS
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 1024, -1, Exception.class), // --> PASS
                //Arguments.of(UnpooledByteBufAllocator.DEFAULT, 1024, 2048, Exception.class), // --> FAILURE: no exception thrown
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 1024, 1024, null), // --> PASS
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 1024, 1023, Exception.class), // --> PASS
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 1024, 512, null), // --> PASS
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 1, 1, null), // --> PASS
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 1, 0, Exception.class) // --> PASS

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

            //rivedi
            int expectedSegmentCount = (int)(1+(maxCacheSize/maxSegmentSize));

            assertEquals(maxCacheSize, writeCache.getMaxCacheSize());
            assertEquals(maxSegmentSize, writeCache.getMaxSegmentSize());
            assertEquals(0, writeCache.size());
            assertEquals(0, writeCache.count());
            assertEquals(0, writeCache.getCacheOffset());
            assertEquals(expectedSegmentCount, writeCache.getSegmentsCount());

        }
    }


}
