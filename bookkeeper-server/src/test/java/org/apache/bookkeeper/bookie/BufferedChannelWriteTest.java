package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.Assert;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import static org.apache.bookkeeper.bookie.Util.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.junit.jupiter.api.Assertions.*;

// Indica che una singola istanza della classe di test verrà utilizzata per tutti i metodi di test
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BufferedChannelWriteTest {

    private String file;


    // Definisce i dati di input per il test sulla write
    static Stream<Arguments> provideWriteArguments() throws IOException {

        return Stream.of(

                // src non valida (tramite Mock)
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 0, 0, 0, "validTestFile", getInvalidByteBuf(), null, Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 0, 0, 1, "validTestFile", getInvalidByteBuf(), null, Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, readOnlyFileChannel("invalidTestFile"), 1, 1, 0, "invalidTestFile", getInvalidByteBuf(), null, Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 4096, 4096, 4096, "validTestFile", getInvalidByteBuf(), null,  Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 1, 1, 2, "validTestFile", getInvalidByteBuf(), null, Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 1, 1, -1, "validTestFile", getInvalidByteBuf(), null, Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 4096, 1, 1024, "validTestFile", getInvalidByteBuf(), null, Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 4096, 1, 4097, "validTestFile", getInvalidByteBuf(), null, Exception.class),
                Arguments.of(getInvalidAllocator(), validFileChannel("validTestFile"), 1, 1, 0, "validTestFile", getInvalidByteBuf(), null, Exception.class),
                // Aggiunta dopo report PIT ------------------------------------------------
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, writtenFileChannel("writtenTestFile"), 4096, 1, 1024, "writtenTestFile", getInvalidByteBuf(), null, Exception.class),

                // src null
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 0, 0, 0, "validTestFile", null, null, NullPointerException.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 0, 0, 1, "validTestFile", null, null, NullPointerException.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, readOnlyFileChannel("invalidTestFile"), 1, 1, 0, "invalidTestFile", null, null, NullPointerException.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 4096, 4096, 4096, "validTestFile", null, null, NullPointerException.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 1, 1, 2, "validTestFile", null, null, NullPointerException.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 1, 1, -1, "validTestFile", null, null, NullPointerException.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 4096, 1, 1024, "validTestFile", null, null, NullPointerException.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 4096, 1, 4097, "validTestFile", null, null, NullPointerException.class),
                Arguments.of(getInvalidAllocator(), validFileChannel("validTestFile"), 1, 1, 0, "validTestFile", null, null, NullPointerException.class),
                // Aggiunta dopo report PIT ------------------------------------------
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, writtenFileChannel("writtenTestFile"), 4096, 1, 1024, "writtenTestFile", null, null, NullPointerException.class),

                // src vuota
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 0, 0, 0, "validTestFile", Unpooled.buffer(0), "", null),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 0, 0, 1, "validTestFile", Unpooled.buffer(0), "", null),
                //Arguments.of(UnpooledByteBufAllocator.DEFAULT, readOnlyFileChannel("invalidTestFile"), 1, 1, 0, "invalidTestFile", Unpooled.buffer(0), "", Exception.class), //--> FAILURE: Eccezione non lanciata
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 4096, 4096, 4096, "validTestFile", Unpooled.buffer(0), "", null),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 1, 1, 2, "validTestFile", Unpooled.buffer(0),"", null),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 1, 1, -1, "validTestFile", Unpooled.buffer(0), "",null),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 4096, 1, 1024, "validTestFile", Unpooled.buffer(0), "", null),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 4096, 1, 4097, "validTestFile", Unpooled.buffer(0), "", null),
                //Arguments.of(getInvalidAllocator(), validFileChannel("validTestFile"), 1, 1, 0, "validTestFile", Unpooled.buffer(0), "", Exception.class),  //--> FAILURE: Eccezione non lanciata
                // Aggiunta dopo report PIT -----------------------------------------------
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, writtenFileChannel("writtenTestFile"), 4096, 1, 1024, "writtenTestFile", Unpooled.buffer(0), "", null),

                // src non vuota
                //Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 0, 0, 0, "validTestFile", getWrittenByteBuf(), "Hello, world!", null), //--> FAILURE: Loop
                //Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 0, 0, 1, "validTestFile", getWrittenByteBuf(), "Hello, world!",  null), //--> FAILURE: Loop
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, readOnlyFileChannel("invalidTestFile"), 1, 1, 0, "invalidTestFile", getWrittenByteBuf(), "Hello, world!", Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 4096, 4096, 4096, "validTestFile", getWrittenByteBuf(), "Hello, world!",  null),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 1, 1, 2, "validTestFile", getWrittenByteBuf(), "Hello, world!", null),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 1, 1, -1, "validTestFile", getWrittenByteBuf(), "Hello, world!",  null),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 4096, 1, 1024, "validTestFile", getWrittenByteBuf(), "Hello, world!", null),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 4096, 1, 4097, "validTestFile", getWrittenByteBuf(), "Hello, world!",  null),
                Arguments.of(getInvalidAllocator(), validFileChannel("validTestFile"), 1, 1, 0, "validTestFile", getWrittenByteBuf(), "Hello, world!", Exception.class),
                // Aggiunta dopo report PIT ----------------------------------------------------------------
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, writtenFileChannel("writtenTestFile"), 4096, 1, 1024, "writtenTestFile", getWrittenByteBuf(), "Hello, world!", null),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, writtenFileChannel("writtenTestFile"), 4096, 13, 1, "writtenTestFile", getWrittenByteBuf(), "Hello, world!", null)
        );
    }


    // Definisce i dati di input per il test sulla write
    static Stream<Arguments> provideWriteClosedArguments() throws IOException {
        return Stream.of(

                //src non vuota
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 0, 0, 0, "validTestFile", getWrittenByteBuf(),  Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, readOnlyFileChannel("invalidTestFile"), 1, 1, 0, "invalidTestFile", getWrittenByteBuf(), Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 4096, 4096, 4096, "validTestFile", getWrittenByteBuf(), Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 1, 1, 2, "validTestFile", getWrittenByteBuf(), Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 1, 1, -1, "validTestFile", getWrittenByteBuf(), Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 4096, 1, 1024, "validTestFile", getWrittenByteBuf(), Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 4096, 1, 4097, "validTestFile", getWrittenByteBuf(), Exception.class),
                Arguments.of(getInvalidAllocator(), validFileChannel("validTestFile"), 1, 1, 0, "validTestFile", getWrittenByteBuf(), Exception.class)

                // src vuota  --> FAILURE in tutti i casi (non viene generata nessuna eccezione)
                /*
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 0, 0, 0, "validTestFile", Unpooled.buffer(0), Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, readOnlyFileChannel("invalidTestFile"), 1, 1, 0, "invalidTestFile", Unpooled.buffer(0), Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 4096, 4096, 4096, "validTestFile", Unpooled.buffer(0), Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 1, 1, 2, "validTestFile", Unpooled.buffer(0), Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 1, 1, -1, "validTestFile", Unpooled.buffer(0), Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 4096, 1, 1024, "validTestFile", Unpooled.buffer(0), Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 4096, 1, 4097, "validTestFile", Unpooled.buffer(0), Exception.class),
                Arguments.of(getInvalidAllocator(), validFileChannel("validTestFile"), 1, 1, 0, "validTestFile", Unpooled.buffer(0), Exception.class)


                 */


        );
    }


    @ParameterizedTest
    @Timeout(5)
    @MethodSource("provideWriteArguments")
    void testBufferedChannelWrite(ByteBufAllocator allocator, FileChannel fc, int writeCapacity, int readCapacity, long unpersistedBytesBound, String filename, ByteBuf src, String src_content, Class<Exception> expectedException){
        long prevPos;
        long unpersistedBytesPrev;
        long writeStartPosPrev;
        long expectedBytesUnpersisted = 0;
        long expectedPos;
        long expectedWriteStartPos;
        this.file = filename;

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


                // Aggiunta dopo report PIT ---------------------------------
                String prevFileContent;
                int bufferSize;

                if(fc.position() == 0){
                    prevFileContent = "";
                }else{
                    bufferSize = Math.toIntExact(bc.fileChannel.size());
                    ByteBuffer buff = ByteBuffer.allocate(bufferSize);

                    bc.fileChannel.read(buff,0);

                    prevFileContent = new String(buff.array());
                }
                //---------------------------------------------------------

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

                Assert.assertEquals("Expected position failed",expectedPos, bc.position);
                Assert.assertEquals("Expected bytes bound failed", expectedBytesUnpersisted, bc.getUnpersistedBytes());
                Assert.assertEquals("Expected file channel position failed",expectedWriteStartPos, bc.getFileChannelPosition());

                // Aggiunta dopo report PIT ---------------------------
                bufferSize = Math.toIntExact(bc.fileChannel.size());
                ByteBuffer buff1 = ByteBuffer.allocate(bufferSize);
                bc.fileChannel.read(buff1,0);
                String actualFileContent = new String(buff1.array());
                Assert.assertEquals("Expected content of file channel failed", prevFileContent+src_content.substring(0, Math.toIntExact(src_bytes - expectedBytesUnpersisted)), actualFileContent);

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
    public void  cleanUp() throws IOException {
        Path path = Paths.get(file);
        if (Files.exists(path)) {
            Files.delete(path);
        }
        Files.createFile(path);

    }

}

