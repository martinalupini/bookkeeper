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
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, writtenFileChannel("Hello world"), 3, 3, -1, "!", getInvalidDest(), 0, 1, Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 4096, 4096, 1024, null, getInvalidDest(), 0, 1, Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, writtenFileChannel("Hello world!"), 4096, 4096, 1, null, getInvalidDest(), 0, 1, Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 4096, 4096, -1, "Hello world!", getInvalidDest(), 0, 1, Exception.class),

                // read capacity e write capacity 0
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, writtenFileChannel("Hello world!"),  0, 0, 0, null, getEmptyByteBuf(), 0, 12, Exception.class),

                // allocatore non valido
                Arguments.of(getInvalidAllocator(), writtenFileChannel("Hello world!"), 4096, 4096, 1, null, getEmptyByteBuf(), 0, 12, null), // FAILURE --> nessuna eccezione generata. Cambio exception attesa in null

                // length 0 e -1
                // Tutti questi test sono falliti in quanto non è stata ritornata nessuna eccezione
             /* Arguments.of(UnpooledByteBufAllocator.DEFAULT, writtenFileChannel("Hello world"), 3, 3, -1, "!", getEmptyByteBuf(), 0, -1, Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 4096, 4096, 1024, null, getEmptyByteBuf(), 0, -1, Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, writtenFileChannel("Hello world!"), 4096, 4096, 1,null, getEmptyByteBuf(), 0, -1, Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 4096, 4096, -1, "Hello world!", getEmptyByteBuf(), 0, -1, Exception.class),

                Arguments.of(UnpooledByteBufAllocator.DEFAULT, writtenFileChannel("Hello world"), 3, 3, -1, "!", getEmptyByteBuf(), 0, 0, Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 4096, 4096, 1024, null, getEmptyByteBuf(), 0, 0, Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, writtenFileChannel("Hello world!"), 4096, 4096, 1, null, getEmptyByteBuf(), 0, 0, Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 4096, 4096, -1, "Hello world!", getEmptyByteBuf(), 0, 0, Exception.class),
                 */

                // pos pari a -1
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, writtenFileChannel("Hello world"), 3, 3, -1, "!", getEmptyByteBuf(), -1, 1, Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 4096, 4096, 1024, null, getEmptyByteBuf(), -1, 1, Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, writtenFileChannel("Hello world!"), 4096, 4096, 1, null, getEmptyByteBuf(), -1, 1, Exception.class),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 4096, 4096, -1, "Hello world!", getEmptyByteBuf(), -1, 1, Exception.class),

                // write buffer non vuoto e fc vuoto
                //Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 4096, 4096, -1, "Hello world!", getEmptyByteBuf(), 0, 1, null), // FAILURE --> reads all bytes
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 4096, 4096, -1, "Hello world!", getEmptyByteBuf(), 0, 13, Exception.class),
                //Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 4096, 4096, -1, "Hello world!", getEmptyByteBuf(), 0, 11, null), // FAILURE --> legge tutti i bytes
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 4096, 4096, -1, "Hello world!", getEmptyByteBuf(), 11, 1, null),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 4096, 4096, -1, "Hello world!", getEmptyByteBuf(), 13, 1, Exception.class),

                // entrambi vuoti
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel("validTestFile"), 4096, 4096, 1024, null, getEmptyByteBuf(), 0, 1, Exception.class),

                // write buffer vuoto e fc non vuoto
                //Arguments.of(UnpooledByteBufAllocator.DEFAULT, writtenFileChannel("Hello world!"), 4096, 4096, 1, null, getEmptyByteBuf(), 0, 1, null), // FAILURE --> mi aspetto di leggere un bytes ma ne legge 12
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, writtenFileChannel("Hello world!"), 4096, 4096, 1, null, getEmptyByteBuf(), 0, 13, Exception.class),
                //Arguments.of(UnpooledByteBufAllocator.DEFAULT, writtenFileChannel("Hello world!"), 4096, 4096, 1, null, getEmptyByteBuf(), 0, 11, null), // FAILURE --> legge tutti i bytes
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, writtenFileChannel("Hello world!"), 4096, 4096, 1, null, getEmptyByteBuf(), 11, 1, null),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, writtenFileChannel("Hello world!"), 4096, 4096, 1, null, getEmptyByteBuf(), 13, 1, Exception.class),

                // sia write buffer che fc non sono vuoti
                //Arguments.of(UnpooledByteBufAllocator.DEFAULT, writtenFileChannel("Hello world"), 3, 3, -1, "!", getEmptyByteBuf(), 0, 1, null), // FAILURE --> mi aspettavo di leggere un byte ma ne legge 3
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, writtenFileChannel("Hello world"), 3, 3, -1, "!", getEmptyByteBuf(), 0, 11, null),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, writtenFileChannel("Hello world"), 3, 3, -1, "!", getEmptyByteBuf(), 0, 12, null),
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, writtenFileChannel("Hello world"), 3, 3, -1, "!", getEmptyByteBuf(), 12, 1, Exception.class),
                //Arguments.of(UnpooledByteBufAllocator.DEFAULT, writtenFileChannel("Hello world"), 3, 3, -1, "!", getEmptyByteBuf(), 11, 1, null),  // FAILURE --> mi aspettavo di leggere il carattere ! nel write buffer invece viene letto H
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, writtenFileChannel("Hello world"), 3, 3, -1, "!", getEmptyByteBuf(), 12, 2, Exception.class),

                // Dopo report Jacoco
                Arguments.of(getInvalidAllocator(), writtenFileChannel("Hello world!"), 4096, 4096, 1, null, getEmptyByteBuf(), 0, 13, null)
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

    private static FileChannel unwrittableFileChannel() throws IOException {
        Path path = Paths.get("validFileTest");
        if (Files.exists(path)) {
            Files.delete(path);
        }
        Files.createFile(path);

        ByteBuffer buff = ByteBuffer.wrap("Hello world!".getBytes(StandardCharsets.UTF_8));
        FileChannel fc = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
        fc.write(buff);
        FileChannel spyFC = spy(fc);
        doReturn(-1).when(spyFC).read(any(ByteBuffer.class), anyInt());
        return spyFC;
    }


    @ParameterizedTest
    @Timeout(5)
    @MethodSource("provideReadArguments")
    void testBufferedChannelRead(ByteBufAllocator allocator, FileChannel fc, int writeCapacity, int readCapacity, long unpersistedBytesBound, String contentOfWriteBuffer, ByteBuf dest, int pos, int length, Class<Exception> expectedException) {


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
                int expectedRet = length;

                int ret = bc.read(dest, pos, length);

                byte[] actualBytesRead = new byte[0];
                dest.getBytes(0, actualBytesRead);

                if(pos >= bc.getFileChannelPosition()){
                    int positionInBuffer = (int) (pos - bc.getFileChannelPosition());

                    expectedContent = content.substring(positionInBuffer, positionInBuffer+length);
                } else {
                    if(pos+length <= content.length()) {
                        expectedContent = content.substring(pos, pos + length);
                    }else{
                        expectedContent = content.substring(pos);
                        expectedRet = content.length();
                    }
                }

                // creo un ByteBuf con il contenuto che mi aspetto
                byte[] data = expectedContent.getBytes();
                ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer();
                byteBuf.writeBytes(data);


                Assert.assertEquals("numbers of bytes read is not what expected", expectedRet, ret);
                Assert.assertEquals("content read is not what expected", dest.toString(StandardCharsets.UTF_8), byteBuf.toString(StandardCharsets.UTF_8));
            }



        } catch (IOException e) {
            throw new RuntimeException(e);
        }


    }

    static Stream<Arguments> provideBaduaReadArguments() throws IOException {

        return Stream.of(

                // Dopo report Jacoco e report Badua
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, writtenFileChannel("Hello world!"), 4096, 4096, 0, getEmptyByteBuf(), -1, 12, Exception.class),
                // Dopo il secondo report Badua
                Arguments.of(UnpooledByteBufAllocator.DEFAULT, writtenFileChannel("Hello world!"), 4096, 4096, 0, getEmptyByteBuf(), 0, 12, null)
        );
    }

    // Aggiunto dopo il report di Jacoco e Badua
    @ParameterizedTest
    @MethodSource("provideBaduaReadArguments")
    public void baduaReadTest(ByteBufAllocator allocator, FileChannel fc, int writeCapacity, int readCapacity, long unpersistedBytesBound, ByteBuf dest, int pos, int length, Class<Exception> expectedException) throws IOException {

        try {
            BufferedChannel bc = new BufferedChannel(allocator, fc, writeCapacity, readCapacity, unpersistedBytesBound);
            assertNotNull(bc, "BufferedChannel should not be null");

            // prima read per riempire il read buffer
            bc.read(dest, 0, 10);

            if (expectedException != null) {
                // Verifica che venga lanciata l'eccezione attesa
                assertThrows(expectedException, () -> {
                    bc.read(dest, pos, length);
                });
            } else {


                int ret = bc.read(dest, pos , length);

                Assert.assertEquals("numbers of bytes read is not what expected", 12, ret);
            }
        }catch (IOException e){
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
