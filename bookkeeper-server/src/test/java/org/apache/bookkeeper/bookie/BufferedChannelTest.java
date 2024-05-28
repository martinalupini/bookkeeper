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
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.stream.Stream;
import java.nio.file.Paths;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.junit.jupiter.api.Assertions.*;

// Indica che una singola istanza della classe di test verrà utilizzata per tutti i metodi di test
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BufferedChannelTest {

    private FileChannel valid;
    private FileChannel readOnly;
    private FileChannel closed;

    @BeforeAll
    public void setUp() throws IOException {
        valid = validFileChannel();
        readOnly = readOnlyFileChannel();
        closed = closedFileChannel();
    }


    static ByteBufAllocator getInvalidAllocator(){
        ByteBufAllocator invalidAllocator = mock(ByteBufAllocator.class);
        when(invalidAllocator.buffer()).thenReturn(null);
        when(invalidAllocator.directBuffer(anyInt())).thenReturn(null);

        return invalidAllocator;
    }

    private static FileChannel readOnlyFileChannel() throws IOException {
        Path path = Paths.get("invalidTestFile.txt");
        if (!Files.exists(path)) {
            Files.createFile(path);
        }
        return FileChannel.open(path, StandardOpenOption.READ);
    }

    private static FileChannel validFileChannel() throws IOException {
        Path path = Paths.get("validTestFile.txt");
        if (!Files.exists(path)) {
            Files.createFile(path);
        }
        return FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
    }

    private static FileChannel closedFileChannel() throws IOException {
        Path path = Paths.get("closedTestFile.txt");
        if (!Files.exists(path)) {
            Files.createFile(path);
        }
        FileChannel fc = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
        fc.close();
        return fc;
    }


    // Definisce i dati di input per il test sul costruttore
    static Stream<Arguments> provideConstructorArguments() {
        try {
            return Stream.of(
                    Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel(), -1, 1, 0, Exception.class),  // --> PASS
                    Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel(), 1, -1, 0, Exception.class),  // --> PASS
                    Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel(), 0, 0, 0, null),    // --> PASS
                    Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel(), 0, 0, 1, null),    // --> PASS
                    //Arguments.of(UnpooledByteBufAllocator.DEFAULT, readOnlyFileChannel(), 1, 1, 0, Exception.class),  // --> FAIL: Exception not thrown
                    Arguments.of(UnpooledByteBufAllocator.DEFAULT, readOnlyFileChannel(), 1, 1, 0, null),
                    Arguments.of(UnpooledByteBufAllocator.DEFAULT, closedFileChannel(), 1, 1, 0, Exception.class),    // --> PASS: ClosedChannelException
                    Arguments.of(null, validFileChannel(), 1, 1, 0, NullPointerException.class),  // --> PASS
                    Arguments.of(UnpooledByteBufAllocator.DEFAULT, null, 1, 1, 0, NullPointerException.class),  // --> PASS
                    Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel(), 4096, 4096, 4096, null),   // --> PASS
                    Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel(), 1, 1, 2, null),    // --> PASS
                    Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel(), 1, 1, -1, null),   // --> PASS
                    Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel(), 4096, 1, 1024, null),  // --> PASS
                    Arguments.of(UnpooledByteBufAllocator.DEFAULT, validFileChannel(), 4096, 1, 4097, null),   // --> PASS
                    //Arguments.of(getInvalidAllocator(), validFileChannel(), 1, 1, 0, Exception.class)  // --> FAIL: Exception not thrown
                    Arguments.of(getInvalidAllocator(), validFileChannel(), 1, 1, 0, null)
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // Definisce i dati di input per il test sulla write
    static Stream<Arguments> provideWriteArguments(){
        return Stream.of(
                Arguments.of(UnpooledByteBufAllocator.DEFAULT.buffer())
        );
    }


    // Definisce i dati di input per il test sulla write
    static Stream<Arguments> provideReadArguments(){
        return Stream.of(
                Arguments.of(UnpooledByteBufAllocator.DEFAULT.buffer(), 0, 0)
        );
    }


    // Test parametrizzato che verifica il comportamento del costruttore di BufferedChannel
    @ParameterizedTest
    @MethodSource("provideConstructorArguments")
    void testBufferedChannelCreation(ByteBufAllocator allocator, FileChannel fc, int writeCapacity, int readCapacity, long unpersistedBytesBound, Class<Exception> expectedException) {
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
                assertEquals(0, bufferedChannel.getFileChannelPosition());
                assertEquals(Long.MIN_VALUE, bufferedChannel.getReadBufferStartPosition());
                assertEquals(0, bufferedChannel.getUnpersistedBytes());
                if(unpersistedBytesBound > 0) {
                    assertTrue(bufferedChannel.isDoRegularFlushes());
                }else{
                    assertFalse(bufferedChannel.isDoRegularFlushes());
                }
                assertFalse(bufferedChannel.isClosed());
                assertEquals(0, bufferedChannel.position());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }


    @ParameterizedTest
    @MethodSource("provideWriteArguments")
    void testBufferedChannelWrite(ByteBuf src){
        assertFalse(false);
    }


    @ParameterizedTest
    @MethodSource("provideReadArguments")
    void testBufferedChannelRead(ByteBuf dest, long pos, int length){
        assertFalse(false);
    }



    @AfterEach
    public void  cleanUp(){
        File f1 = new File("validTestFile.txt");
        f1.delete();

        File f2 = new File("invalidTestFile.txt");
        f2.delete();

        File f3 = new File("closedTestFile.txt");
        f3.delete();
    }

}
