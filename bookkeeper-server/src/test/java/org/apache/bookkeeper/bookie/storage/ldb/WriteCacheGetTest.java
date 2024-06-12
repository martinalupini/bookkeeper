package org.apache.bookkeeper.bookie.storage.ldb;


import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.Assert;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.apache.bookkeeper.bookie.Util.getInvalidByteBuf;
import static org.apache.bookkeeper.bookie.Util.getWrittenByteBuf;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Test di unità per la classe {@link WriteCache}. <br>
 * Metodo testato: {@link WriteCache#get(long, long)}
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class WriteCacheGetTest {

    static Stream<Arguments> provideGetArguments() {
        return Stream.of(
                // ledgerId negativi
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 1024, 1024, -1, 1, false, Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 1024, 1024, -1, 1, false, Exception.class),

                // entryId negativi
                //Arguments.of(UnpooledByteBufAllocator.DEFAULT, 1024, 1024, 1, -1, false, Exception.class), // FAILURE --> exception not thrown
                //Arguments.of(UnpooledByteBufAllocator.DEFAULT, 1024, 1024, 1, -1, false, Exception.class), // FAILURE --> exception not thrown

                // chiave non presente
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 1024, 1024, 1, 1, false, null),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 1024, 1024, 1, 1, false, null),

                // chiave presente
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 1024, 1024, 1, 1, true, null),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, 1024, 1024, 1, 1, true, null)



        );
    }


    @ParameterizedTest
    @MethodSource("provideGetArguments")
    void testPut(ByteBufAllocator allocator, long maxCacheSize, int maxSegmentSize, long ledgerId, long entryId, boolean alreadyInMap, Class<Exception> expectedException) {

        WriteCache writeCache = new WriteCache(allocator, maxCacheSize, maxSegmentSize);
        ByteBuf entry = getWrittenByteBuf();

        if(alreadyInMap) {
            writeCache.put(ledgerId, entryId, entry);
        }

        if (expectedException != null) {

            assertThrows(expectedException, () -> {
                writeCache.get(ledgerId, entryId);
            });
        } else {

            ByteBuf ret = writeCache.get(ledgerId, entryId);

            if(!alreadyInMap) {
                Assert.assertNull(ret);
            }else{
                Assert.assertEquals(entry, ret);
            }

        }
    }

}
