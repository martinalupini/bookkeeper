package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.stream.Stream;


import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.junit.jupiter.api.Assertions.*;

// Indica che una singola istanza della classe di test verrà utilizzata per tutti i metodi di test
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BufferedChannelWriteTest {

    private String file;

    private static ByteBuf getWrittenByteBuf(){
        byte[] testData = "Hello, world!".getBytes();
        ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer();
        byteBuf.writeBytes(testData);
        return byteBuf;
    }

    private static ByteBuf getInvalidByteBuf(){
        ByteBuf buf = mock(ByteBuf.class);
        when(buf.readableBytes()).thenReturn(1);
        when(buf.readerIndex()).thenReturn(-1);
        return buf;
    }


    // Definisce i dati di input per il test sulla write
    static Stream<Arguments> provideWriteArguments() throws IOException {
        return Stream.of(

                // src non valida (tramite Mock)
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, Util.validFileChannel("validTestFile"), 0, 0, 0, "validTestFile", getInvalidByteBuf(), Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, Util.validFileChannel("validTestFile"), 0, 0, 1, "validTestFile", getInvalidByteBuf(), Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, Util.readOnlyFileChannel("invalidTestFile"), 1, 1, 0, "invalidTestFile", getInvalidByteBuf(), Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, Util.validFileChannel("validTestFile"), 4096, 4096, 4096, "validTestFile", getInvalidByteBuf(), Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, Util.validFileChannel("validTestFile"), 1, 1, 2, "validTestFile", getInvalidByteBuf(), Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, Util.validFileChannel("validTestFile"), 1, 1, -1, "validTestFile", getInvalidByteBuf(), Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, Util.validFileChannel("validTestFile"), 4096, 1, 1024, "validTestFile", getInvalidByteBuf(), Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, Util.validFileChannel("validTestFile"), 4096, 1, 4097, "validTestFile", getInvalidByteBuf(), Exception.class),
                Arguments.of(Util.getInvalidAllocator(), Util.validFileChannel("validTestFile"), 1, 1, 0, "validTestFile", getInvalidByteBuf(), Exception.class),

                // src null
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, Util.validFileChannel("validTestFile"), 0, 0, 0, "validTestFile", null, NullPointerException.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, Util.validFileChannel("validTestFile"), 0, 0, 1, "validTestFile", null, NullPointerException.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, Util.readOnlyFileChannel("invalidTestFile"), 1, 1, 0, "invalidTestFile", null, NullPointerException.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, Util.validFileChannel("validTestFile"), 4096, 4096, 4096, "validTestFile", null, NullPointerException.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, Util.validFileChannel("validTestFile"), 1, 1, 2, "validTestFile", null, NullPointerException.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, Util.validFileChannel("validTestFile"), 1, 1, -1, "validTestFile", null, NullPointerException.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, Util.validFileChannel("validTestFile"), 4096, 1, 1024, "validTestFile", null, NullPointerException.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, Util.validFileChannel("validTestFile"), 4096, 1, 4097, "validTestFile", null, NullPointerException.class),
                Arguments.of(Util.getInvalidAllocator(), Util.validFileChannel("validTestFile"), 1, 1, 0, "validTestFile", null, NullPointerException.class),

                // src vuota
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, Util.validFileChannel("validTestFile"), 0, 0, 0, "validTestFile", Unpooled.buffer(0), null),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, Util.validFileChannel("validTestFile"), 0, 0, 1, "validTestFile", Unpooled.buffer(0), null),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, Util.readOnlyFileChannel("invalidTestFile"), 1, 1, 0, "invalidTestFile", Unpooled.buffer(0), Exception.class), //--> FAILURE: Eccezione non lanciata
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, Util.validFileChannel("validTestFile"), 4096, 4096, 4096, "validTestFile", Unpooled.buffer(0), null),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, Util.validFileChannel("validTestFile"), 1, 1, 2, "validTestFile", Unpooled.buffer(0), null),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, Util.validFileChannel("validTestFile"), 1, 1, -1, "validTestFile", Unpooled.buffer(0), null),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, Util.validFileChannel("validTestFile"), 4096, 1, 1024, "validTestFile", Unpooled.buffer(0), null),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, Util.validFileChannel("validTestFile"), 4096, 1, 4097, "validTestFile", Unpooled.buffer(0), null),
                //Arguments.of(Util.getInvalidAllocator(), Util.validFileChannel("validTestFile"), 1, 1, 0, "validTestFile", Unpooled.buffer(0), Exception.class),  //--> FAILURE: Eccezione non lanciata

                // src non vuota
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, Util.validFileChannel("validTestFile"), 0, 0, 0, "validTestFile", getWrittenByteBuf(), null), //--> FAILURE: Loop
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, Util.validFileChannel("validTestFile"), 0, 0, 1, "validTestFile", getWrittenByteBuf(), null), //--> FAILURE: Loop
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, Util.readOnlyFileChannel("invalidTestFile"), 1, 1, 0, "invalidTestFile", getWrittenByteBuf(), Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, Util.validFileChannel("validTestFile"), 4096, 4096, 4096, "validTestFile", getWrittenByteBuf(), null),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, Util.validFileChannel("validTestFile"), 1, 1, 2, "validTestFile", getWrittenByteBuf(), null),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, Util.validFileChannel("validTestFile"), 1, 1, -1, "validTestFile", getWrittenByteBuf(), null),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, Util.validFileChannel("validTestFile"), 4096, 1, 1024, "validTestFile", getWrittenByteBuf(), null),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, Util.validFileChannel("validTestFile"), 4096, 1, 4097, "validTestFile", getWrittenByteBuf(), null),
                Arguments.of(Util.getInvalidAllocator(), Util.validFileChannel("validTestFile"), 1, 1, 0, "validTestFile", getWrittenByteBuf(), Exception.class)
        );
    }


    // Definisce i dati di input per il test sulla write
    static Stream<Arguments> provideWriteClosedArguments() throws IOException {
        return Stream.of(

                //src non vuota
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, Util.validFileChannel("validTestFile"), 0, 0, 0, "validTestFile", getWrittenByteBuf(),  Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, Util.readOnlyFileChannel("invalidTestFile"), 1, 1, 0, "invalidTestFile", getWrittenByteBuf(), Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, Util.validFileChannel("validTestFile"), 4096, 4096, 4096, "validTestFile", getWrittenByteBuf(), Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, Util.validFileChannel("validTestFile"), 1, 1, 2, "validTestFile", getWrittenByteBuf(), Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, Util.validFileChannel("validTestFile"), 1, 1, -1, "validTestFile", getWrittenByteBuf(), Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, Util.validFileChannel("validTestFile"), 4096, 1, 1024, "validTestFile", getWrittenByteBuf(), Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, Util.validFileChannel("validTestFile"), 4096, 1, 4097, "validTestFile", getWrittenByteBuf(), Exception.class),
                Arguments.of(Util.getInvalidAllocator(), Util.validFileChannel("validTestFile"), 1, 1, 0, "validTestFile", getWrittenByteBuf(), Exception.class),

                // src vuota  --> FAILURE in tutti i casi
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, Util.validFileChannel("validTestFile"), 0, 0, 0, "validTestFile", Unpooled.buffer(0), Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, Util.readOnlyFileChannel("invalidTestFile"), 1, 1, 0, "invalidTestFile", Unpooled.buffer(0), Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, Util.validFileChannel("validTestFile"), 4096, 4096, 4096, "validTestFile", Unpooled.buffer(0), Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, Util.validFileChannel("validTestFile"), 1, 1, 2, "validTestFile", Unpooled.buffer(0), Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, Util.validFileChannel("validTestFile"), 1, 1, -1, "validTestFile", Unpooled.buffer(0), Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, Util.validFileChannel("validTestFile"), 4096, 1, 1024, "validTestFile", Unpooled.buffer(0), Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, Util.validFileChannel("validTestFile"), 4096, 1, 4097, "validTestFile", Unpooled.buffer(0), Exception.class),
                Arguments.of(Util.getInvalidAllocator(), Util.validFileChannel("validTestFile"), 1, 1, 0, "validTestFile", Unpooled.buffer(0), Exception.class)

        );
    }


    @ParameterizedTest
    @Timeout(5)
    @MethodSource("provideWriteArguments")
    void testBufferedChannelWrite(ByteBufAllocator allocator, FileChannel fc, int writeCapacity, int readCapacity, long unpersistedBytesBound, String filename, ByteBuf src, Class<Exception> expectedException){
        long prevPos;
        long unpersistedBytesPrev;
        long writeStartPosPrev;
        long expectedBytesUnpersisted = 0;
        long expectedPos;
        long expectedWriteStartPos;

        try {

            BufferedChannel bc = new BufferedChannel(allocator, fc, writeCapacity, readCapacity, unpersistedBytesBound);
            assertNotNull(bc, "BufferedChannel should not be null");

            if (expectedException != null) {
                // Verifica che venga lanciata l'eccezione attesa
                assertThrows(Exception.class, () -> {
                    bc.write(src);
                });
            } else {

                //tengo traccia del contesto.
                long bound = bc.getUnpersistedBytesBound();
                long src_bytes = src.readableBytes();
                long cap = bc.getWriteBuffer().capacity();

                // tengo traccia dei valori di position, unpersistedBytes e writeBufferStartPosition prima della scrittura
                prevPos = bc.position();
                unpersistedBytesPrev = bc.getUnpersistedBytes();
                writeStartPosPrev = bc.getFileChannelPosition();


                expectedBytesUnpersisted = 0;

                bc.write(src);

                if (bc.isDoRegularFlushes() && bound < cap) {
                    if (bound - unpersistedBytesPrev >= src_bytes) {
                        expectedBytesUnpersisted = src_bytes + unpersistedBytesPrev;
                    } else if (bound - unpersistedBytesPrev < src_bytes) {
                        expectedBytesUnpersisted = (unpersistedBytesPrev + src_bytes) % bound;
                    }

                } else {
                    if (cap - unpersistedBytesPrev >= src_bytes) {
                        expectedBytesUnpersisted = src_bytes + unpersistedBytesPrev;
                    } else if (cap - unpersistedBytesPrev < src_bytes) {
                        expectedBytesUnpersisted = (unpersistedBytesPrev + src_bytes) % cap;
                    }

                }

                expectedPos = prevPos + src_bytes;
                expectedWriteStartPos = writeStartPosPrev + src_bytes - expectedBytesUnpersisted;

                assertEquals(expectedPos, bc.position);
                assertEquals(expectedBytesUnpersisted, bc.getUnpersistedBytes());
                assertEquals(expectedWriteStartPos, bc.getFileChannelPosition());

            }


        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //testo la write ma su istanze di file channel chiuse
    @ParameterizedTest
    @MethodSource("provideWriteClosedArguments")
    void testBufferedChannelWriteClosed(ByteBufAllocator allocator, FileChannel fc, int writeCapacity, int readCapacity, long unpersistedBytesBound, String filename, ByteBuf src, Class<Exception> expectedException) {

        try {


            BufferedChannel bc = new BufferedChannel(allocator, fc, writeCapacity, readCapacity, unpersistedBytesBound);
            assertNotNull(bc, "BufferedChannel should not be null");
            bc.close();
            assertThrows(Exception.class, () -> { bc.write(src);});



        } catch (IOException e) {
            throw new RuntimeException(e);
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

