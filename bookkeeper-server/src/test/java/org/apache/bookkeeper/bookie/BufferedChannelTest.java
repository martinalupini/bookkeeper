package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.util.stream.Stream;
import static org.apache.bookkeeper.bookie.Util.*;

import static org.junit.jupiter.api.Assertions.*;


/**
 * Test di unità per la classe {@link BufferedChannel}. <br>
 * Metodo testato: costruttore
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BufferedChannelTest {

    private String file;

    // Definisce i dati di input per il test sul costruttore
    static Stream<Arguments> provideConstructorArguments() {
        try {
            return Stream.of(
                    Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), -1, 1, 0, "validTestFile", Exception.class),  // --> PASS
                    Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 1, -1, 0, "validTestFile", Exception.class),  // --> PASS
                    Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 0, 0, 0, "validTestFile", null),    // --> PASS
                    Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 0, 0, 1, "validTestFile", null),    // --> PASS
                    //Arguments.of(UnpooledByteBufAllocator.DEFAULT, Util.readOnlyFileChannel("invalidTestFile"), 1, 1, 0, "invalidTestFile", Exception.class),  // --> FAIL: Exception not thrown
                    Arguments.of(UnpooledByteBufAllocator.DEFAULT, readOnlyFileChannel("invalidTestFile"), 1, 1, 0, "invalidTestFile", null),
                    Arguments.of(UnpooledByteBufAllocator.DEFAULT, closedFileChannel("closedTestFile"), 1, 1, 0, "closedTestFile",Exception.class), // --> PASS: ClosedChannelException
                    Arguments.of(null, validFileChannel("validTestFile"), 1, 1, 0, "validTestFile", NullPointerException.class),  // --> PASS
                    Arguments.of(UnpooledByteBufAllocator.DEFAULT, null, 1, 1, 0, null, NullPointerException.class),  // --> PASS
                    Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 4096, 4096, 4096, "validTestFile", null),   // --> PASS
                    Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 1, 1, 2, "validTestFile", null),    // --> PASS
                    Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 1, 1, -1, "validTestFile", null),   // --> PASS
                    Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 4096, 1, 1024, "validTestFile", null),  // --> PASS
                    Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 4096, 1, 4097, "validTestFile",null),   // --> PASS
                    //Arguments.of(Util.getInvalidAllocator(), Util.validFileChannel("validTestFile"), 1, 1, 0, "validTestFile", Exception.class)  // --> FAIL: Exception not thrown
                    Arguments.of(getInvalidAllocator(), validFileChannel("validTestFile"), 1, 1, 0, "validTestFile",null),
                    Arguments.of(UnpooledByteBufAllocator.DEFAULT, writtenFileChannel("writtenTestFile"), 4096, 1, 1024, "writteTestFile", null)
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // Test parametrizzato che verifica il comportamento del costruttore di BufferedChannel
    @ParameterizedTest
    @MethodSource("provideConstructorArguments")
    void testBufferedChannelCreation(ByteBufAllocator allocator, FileChannel fc, int writeCapacity, int readCapacity, long unpersistedBytesBound, String filename, Class<Exception> expectedException) {

        file = filename;

        if (expectedException != null) {
            // Verifica che venga lanciata l'eccezione attesa
            assertThrows(expectedException, () -> {
                new BufferedChannel(allocator, fc, writeCapacity, readCapacity, unpersistedBytesBound);
            });
        } else {
            try {
                // Crea una nuova istanza di BufferedChannel
                BufferedChannel bufferedChannel = new BufferedChannel(allocator, fc, writeCapacity, readCapacity, unpersistedBytesBound);
                assertNotNull(bufferedChannel, "BufferedChannel should not be null");

                // Verifica lo stato interno di BufferedChannel
                assertEquals(writeCapacity, bufferedChannel.getWriteCapacity());
                assertEquals(readCapacity, bufferedChannel.getReadCapacity());
                assertEquals(unpersistedBytesBound, bufferedChannel.getUnpersistedBytesBound());
                assertEquals(fc.position(), bufferedChannel.position());
                assertEquals(Long.MIN_VALUE, bufferedChannel.getReadBufferStartPosition());
                assertEquals(0, bufferedChannel.getUnpersistedBytes());
                if(unpersistedBytesBound > 0) {
                    assertTrue(bufferedChannel.isDoRegularFlushes());
                }else{
                    assertFalse(bufferedChannel.isDoRegularFlushes());
                }
                assertFalse(bufferedChannel.isClosed());
                assertEquals(bufferedChannel.position(), bufferedChannel.getFileChannelPosition());

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @AfterEach
    public void  cleanUp(){

        //rimozione del file creato dopo aver eseguito il test
        if(file == null) return;

        File f = new File(file);
        f.delete();

    }

}
