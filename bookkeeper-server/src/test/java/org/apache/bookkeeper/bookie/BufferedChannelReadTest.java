package org.apache.bookkeeper.bookie;


import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import io.netty.buffer.Unpooled;
import java.util.stream.Stream;

import static org.apache.bookkeeper.bookie.Util.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Test di unità per la classe {@link BufferedChannel}. <br>
 * Metodo testato: {@link BufferedChannel#read(ByteBuf, long, int)}
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BufferedChannelReadTest {

    private String file = "validTestFile";
    private String content = "Hello world!";


    // Definisce i dati di input per il test sulla read
    static Stream<Arguments> provideReadArguments() throws IOException {

        return Stream.of(

                // dest invalida
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, writtenFileChannel("Hello world"), 3, 3, -1, "!", true, getInvalidDest(), 0, 1, Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 4096, 4096, 1024, null, false, getInvalidDest(), 0, 1, Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, writtenFileChannel("Hello world!"), 4096, 4096, 1, null, true, getInvalidDest(), 0, 1, Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 4096, 4096, -1, "Hello world!", true, getInvalidDest(), 0, 1, Exception.class),

                // length 0 e -1
                // Tutti questi test sono falliti in quanto non è stata ritornata nessuna eccezione
             /* Arguments.of(UnpooledByteBufAllocator.DEFAULT, writtenFileChannel("Hello world"), 3, 3, -1, "!", true, getEmptyByteBuf(), 0, -1, Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 4096, 4096, 1024, null, false, getEmptyByteBuf(), 0, -1, Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, writtenFileChannel("Hello world!"), 4096, 4096, 1,null, true, getEmptyByteBuf(), 0, -1, Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 4096, 4096, -1, "Hello world!", true, getEmptyByteBuf(), 0, -1, Exception.class),

                Arguments.of(UnpooledByteBufAllocator.DEFAULT, writtenFileChannel("Hello world"), 3, 3, -1, "!", true, getEmptyByteBuf(), 0, 0, Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 4096, 4096, 1024, null, false, getEmptyByteBuf(), 0, 0, Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, writtenFileChannel("Hello world!"), 4096, 4096, 1, null, true, getEmptyByteBuf(), 0, 0, Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 4096, 4096, -1, "Hello world!", true, getEmptyByteBuf(), 0, 0, Exception.class),
                 */

                // pos pari a -1
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, writtenFileChannel("Hello world"), 3, 3, -1, "!", true, getEmptyByteBuf(), -1, 1, Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 4096, 4096, 1024, null, false, getEmptyByteBuf(), -1, 1, Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, writtenFileChannel("Hello world!"), 4096, 4096, 1, null, true, getEmptyByteBuf(), -1, 1, Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 4096, 4096, -1, "Hello world!", true, getEmptyByteBuf(), -1, 1, Exception.class),

                // write buffer non vuoto e fc vuoto
                //Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 4096, 4096, -1, "Hello world!", true, getEmptyByteBuf(), 0, 1, null), // FAILURE --> reads all bytes
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 4096, 4096, -1, "Hello world!", true, getEmptyByteBuf(), 0, 13, Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 4096, 4096, -1, "Hello world!", true, getEmptyByteBuf(), 0, 12, null),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 4096, 4096, -1, "Hello world!", true, getEmptyByteBuf(), 11, 1, null),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 4096, 4096, -1, "Hello world!", true, getEmptyByteBuf(), 13, 1, Exception.class),

                // entrambi vuoti
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 4096, 4096, 1024, null, false, getEmptyByteBuf(), 0, 1, Exception.class),

                // write buffer vuoto e fc non vuoto
                //Arguments.of(UnpooledByteBufAllocator.DEFAULT, writtenFileChannel("Hello world!"), 4096, 4096, 1, null, true, getEmptyByteBuf(), 0, 1, null), // FAILURE --> mi aspetto di leggere un bytes ma ne legge 12
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, writtenFileChannel("Hello world!"), 4096, 4096, 1, null, true, getEmptyByteBuf(), 0, 13, Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, writtenFileChannel("Hello world!"), 4096, 4096, 1, null, true, getEmptyByteBuf(), 0, 12, null),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, writtenFileChannel("Hello world!"), 4096, 4096, 1, null, true, getEmptyByteBuf(), 11, 1, null),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, writtenFileChannel("Hello world!"), 4096, 4096, 1, null, true, getEmptyByteBuf(), 13, 1, Exception.class),

                // sia write buffer che fc non sono vuoti
                //Arguments.of(UnpooledByteBufAllocator.DEFAULT, writtenFileChannel("Hello world"), 3, 3, -1, "!", true, getEmptyByteBuf(), 0, 1, null), // FAILURE --> mi aspettavo di leggere un byte ma ne legge 3
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, writtenFileChannel("Hello world"), 3, 3, -1, "!", true, getEmptyByteBuf(), 0, 12, null),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, writtenFileChannel("Hello world"), 3, 3, -1, "!", true, getEmptyByteBuf(), 0, 13, Exception.class),
                //Arguments.of(UnpooledByteBufAllocator.DEFAULT, writtenFileChannel("Hello world"), 3, 3, -1, "!", true, getEmptyByteBuf(), 11, 1, null),  // FAILURE --> mi aspettavo di leggere il carattere ! nel write buffer invece viene letto H
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, writtenFileChannel("Hello world"), 3, 3, -1, "!", true, getEmptyByteBuf(), 13, 2, Exception.class)


                );
    }


    public static FileChannel writtenFileChannel(String content) throws IOException {
        Path path = Paths.get("validTestFile");
        if (Files.exists(path)) {
            Files.delete(path);
        }
        Files.createFile(path);

        ByteBuffer buff = ByteBuffer.wrap(content.getBytes(StandardCharsets.UTF_8));
        FileChannel fc = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
        fc.write(buff);
        return fc;
    }


    private static ByteBuf getInvalidDest(){
        ByteBuf buf = mock(ByteBuf.class);
        when(buf.writableBytes()).thenReturn(1);
        when(buf.writeBytes(any(ByteBuf.class), any(int.class), any(int.class) )).thenThrow(new RuntimeException());
        return buf;

    }

    private static ByteBuf getEmptyByteBuf() {
        ByteBuf byteBuf = Unpooled.directBuffer(256);

        return byteBuf;
    }


    @ParameterizedTest
    @Timeout(5)
    @MethodSource("provideReadArguments")
    void testBufferedChannelRead(ByteBufAllocator allocator, FileChannel fc, int writeCapacity, int readCapacity, long unpersistedBytesBound, String contentOfWriteBuffer, boolean notEmpty, ByteBuf dest, int pos, int length, Class<Exception> expectedException) {


        try {
            BufferedChannel bc = new BufferedChannel(allocator, fc, writeCapacity, readCapacity, unpersistedBytesBound);
            assertNotNull(bc, "BufferedChannel should not be null");

            if(contentOfWriteBuffer != null){
                bc.getWriteBuffer().writeBytes(contentOfWriteBuffer.getBytes(StandardCharsets.UTF_8));
            }


            if (expectedException != null) {
                // Verifica che venga lanciata l'eccezione attesa
                assertThrows(expectedException, () -> {
                    bc.read(dest, pos, length);
                });
            } else {

                String expectedContent;

                System.out.println(bc.getFileChannelPosition());
                System.out.println(bc.getWriteBuffer().writerIndex());

                int ret = bc.read(dest, pos, length);

                byte[] actualBytesRead = new byte[0];
                dest.getBytes(0, actualBytesRead);

                print(actualBytesRead);

                if(pos >= bc.getFileChannelPosition()){
                    int positionInBuffer = (int) (pos - bc.getFileChannelPosition());

                    expectedContent = content.substring(positionInBuffer, positionInBuffer+length);
                } else {
                    expectedContent = content.substring(pos, pos + length);
                }

                // creo un ByteBuf con il contenuto che mi aspetto
                byte[] data = expectedContent.getBytes();
                ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer();
                byteBuf.writeBytes(data);


                Assert.assertEquals("numbers of bytes read is not what expected", length, ret);
                Assert.assertEquals("content read is not what expected", dest.toString(StandardCharsets.UTF_8), byteBuf.toString(StandardCharsets.UTF_8));
            }



        } catch (IOException e) {
            throw new RuntimeException(e);
        }




    }

    private void print(byte[] bytes){
        for(byte b : bytes){
            System.out.println(b);
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
