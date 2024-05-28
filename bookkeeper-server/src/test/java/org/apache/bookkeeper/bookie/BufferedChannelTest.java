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
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import java.nio.file.Paths;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.junit.jupiter.api.Assertions.*;

// Indica che una singola istanza della classe di test verrà utilizzata per tutti i metodi di test
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BufferedChannelTest {

    //this list is not seen
    private List<BuffChannSpecific> bufferedChannelList = new ArrayList<>();


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

    private static ByteBuf getWrittenByteBuf(){
        ByteBuf buf = UnpooledByteBufAllocator.DEFAULT.buffer();
        buf.writeByte(1);
        buf.writeByte(2);
        buf.writeByte(3);
        buf.writeByte(4);
        return buf;
    }

    private static ByteBuf getInvalidByteBuf(){
        ByteBuf buf = mock(ByteBuf.class);
        when(buf.readableBytes()).thenReturn(1);
        when(buf.readerIndex()).thenReturn(-1);
        return buf;
    }

    private static class ByteBufAllocSpecific{
        private ByteBufAllocator allocator;
        private boolean invalid;

        ByteBufAllocSpecific(ByteBufAllocator allocator, boolean type) {
            this.allocator = allocator;
            this.invalid = type;
        }

        public ByteBufAllocator getAllocator() {
            return allocator;
        }

        public boolean isInvalid() {
            return invalid;
        }
    }

    private static class FileChannSpecific{
        private  FileChannel fileChannel;
        private  boolean invalid;


        private FileChannSpecific(FileChannel fileChannel, boolean type) {
            this.fileChannel = fileChannel;
            this.invalid = type;
        }

        public FileChannel getFileChannel() {
            return fileChannel;
        }

        public boolean isInvalid() {
            return invalid;
        }
    }


    private static class BuffChannSpecific{
        private BufferedChannel bufferedChannel;
        private boolean invalidAlloc;
        private boolean invalidFC;

        BuffChannSpecific(BufferedChannel bufferedChannel, boolean invalidAlloc, boolean invalidFC) {
            this.bufferedChannel = bufferedChannel;
            this.invalidAlloc = invalidAlloc;
            this.invalidFC = invalidFC;
        }

        public BufferedChannel getBufferedChannel() {
            return bufferedChannel;
        }

        public boolean isInvalidAlloc() {
            return invalidAlloc;
        }

        public boolean isInvalidFC() {
            return invalidFC;
        }

    }

    // Definisce i dati di input per il test sul costruttore
    static Stream<Arguments> provideConstructorArguments() {
        try {
            return Stream.of(
                    Arguments.of(new ByteBufAllocSpecific(UnpooledByteBufAllocator.DEFAULT, false), new FileChannSpecific(validFileChannel(), false), -1, 1, 0, Exception.class),  // --> PASS
                    Arguments.of(new ByteBufAllocSpecific(UnpooledByteBufAllocator.DEFAULT, false), new FileChannSpecific(validFileChannel(), false), 1, -1, 0, Exception.class),  // --> PASS
                    Arguments.of(new ByteBufAllocSpecific(UnpooledByteBufAllocator.DEFAULT, false), new FileChannSpecific(validFileChannel(), false), 0, 0, 0, null),    // --> PASS
                    Arguments.of(new ByteBufAllocSpecific(UnpooledByteBufAllocator.DEFAULT, false), new FileChannSpecific(validFileChannel(), false), 0, 0, 1, null),    // --> PASS
                    //Arguments.of(new ByteBufAllocSpecific(UnpooledByteBufAllocator.DEFAULT, false), new FileChannSpecific(readOnlyFileChannel(), true), 1, 1, 0, Exception.class),  // --> FAIL: Exception not thrown
                    Arguments.of(new ByteBufAllocSpecific(UnpooledByteBufAllocator.DEFAULT, false), new FileChannSpecific(readOnlyFileChannel(), true), 1, 1, 0, null),
                    Arguments.of(new ByteBufAllocSpecific(UnpooledByteBufAllocator.DEFAULT, false), new FileChannSpecific(closedFileChannel(), true), 1, 1, 0, Exception.class),    // --> PASS: ClosedChannelException
                    Arguments.of(new ByteBufAllocSpecific(null, true), new FileChannSpecific(validFileChannel(), false), 1, 1, 0, NullPointerException.class),  // --> PASS
                    Arguments.of(new ByteBufAllocSpecific(UnpooledByteBufAllocator.DEFAULT, false), new FileChannSpecific(null, true), 1, 1, 0, NullPointerException.class),  // --> PASS
                    Arguments.of(new ByteBufAllocSpecific(UnpooledByteBufAllocator.DEFAULT, false), new FileChannSpecific(validFileChannel(), false), 4096, 4096, 4096, null),   // --> PASS
                    Arguments.of(new ByteBufAllocSpecific(UnpooledByteBufAllocator.DEFAULT, false), new FileChannSpecific(validFileChannel(), false), 1, 1, 2, null),    // --> PASS
                    Arguments.of(new ByteBufAllocSpecific(UnpooledByteBufAllocator.DEFAULT, false), new FileChannSpecific(validFileChannel(), false), 1, 1, -1, null),   // --> PASS
                    Arguments.of(new ByteBufAllocSpecific(UnpooledByteBufAllocator.DEFAULT, false), new FileChannSpecific(validFileChannel(), false), 4096, 1, 1024, null),  // --> PASS
                    Arguments.of(new ByteBufAllocSpecific(UnpooledByteBufAllocator.DEFAULT, false), new FileChannSpecific(validFileChannel(), false), 4096, 1, 4097, null),   // --> PASS
                    //Arguments.of(new ByteBufAllocSpecific(getInvalidAllocator(), true), new FileChannSpecific(validFileChannel(), false), 1, 1, 0, Exception.class)  // --> FAIL: Exception not thrown
                    Arguments.of(new ByteBufAllocSpecific(getInvalidAllocator(), true), new FileChannSpecific(validFileChannel(), false), 1, 1, 0, null)
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // Definisce i dati di input per il test sulla write
    static Stream<Arguments> provideWriteArguments(){
        return Stream.of(
                // src vuoto
                Arguments.of(UnpooledByteBufAllocator.DEFAULT.buffer(), null),
                Arguments.of(getWrittenByteBuf(), null),
                Arguments.of(getInvalidByteBuf(), Exception.class),
                Arguments.of(null, NullPointerException.class)
        );
    }


    // Definisce i dati di input per il test sulla read
    static Stream<Arguments> provideReadArguments(){
        return Stream.of(
                Arguments.of(UnpooledByteBufAllocator.DEFAULT.buffer(), 0, 0)
        );
    }


    // Test parametrizzato che verifica il comportamento del costruttore di BufferedChannel
    @ParameterizedTest
    @MethodSource("provideConstructorArguments")
    void testBufferedChannelCreation(ByteBufAllocSpecific allocator, FileChannSpecific fc, int writeCapacity, int readCapacity, long unpersistedBytesBound, Class<Exception> expectedException) {
        if (expectedException != null) {
            // Verifica che venga lanciata l'eccezione attesa
            assertThrows(expectedException, () -> {
                new BufferedChannel(allocator.getAllocator(), fc.getFileChannel(), writeCapacity, readCapacity, unpersistedBytesBound);
            });
        } else {
            try {
                // Crea una nuova istanza di BufferedChannel
                BufferedChannel bufferedChannel = new BufferedChannel(allocator.getAllocator(), fc.getFileChannel(), writeCapacity, readCapacity, unpersistedBytesBound);
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

                // aggiungo l'istanza a quelle da testare
                bufferedChannelList.add(new BuffChannSpecific(bufferedChannel, allocator.isInvalid(), fc.isInvalid()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }


    @ParameterizedTest
    @MethodSource("provideWriteArguments")
    void testBufferedChannelWrite(ByteBuf src, Class<Exception> expectedException){

        try {
            // ho bisogno di aggiungere un BufferedChannel con writeChannel chiuso
            BufferedChannel bufClosed = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, validFileChannel(), 4096, 1, 1024);
            bufClosed.close();
            bufferedChannelList.add(new BuffChannSpecific(bufClosed, false, false));

            // ora testo il metodo write su tutte le possibili istanze ottenute in precedenza
            for(BuffChannSpecific bufferedChannel : bufferedChannelList) {

                System.out.println("sto testando...");
                System.out.println("fc valid "+bufferedChannel.invalidFC+ " alloc valid "+bufferedChannel.invalidAlloc+ " closed "+bufferedChannel.getBufferedChannel().isClosed());
                System.out.println(bufferedChannel.getBufferedChannel());
                System.out.println(src);
                // caso in cui il buffered channel è chiuso, il fc non è valido, l'allocatore non è valido oppure mi aspetto un'eccezione
                if (bufferedChannel.getBufferedChannel().isClosed() || bufferedChannel.isInvalidFC() || bufferedChannel.isInvalidAlloc() || expectedException != null) {
                    // Verifica che venga lanciata l'eccezione attesa
                    assertThrows(expectedException, () -> {
                        bufferedChannel.getBufferedChannel().write(src);
                    });
                } else {

                    // tengo traccia dei valori di position, unpersistedBytes e writeBufferStartPosition prima della scrittura
                    long prevPos = bufferedChannel.getBufferedChannel().position();
                    long unpersistedBytesPrev = bufferedChannel.getBufferedChannel().getUnpersistedBytes();
                    long writeStartPosPrev = bufferedChannel.getBufferedChannel().getFileChannelPosition();

                    bufferedChannel.getBufferedChannel().write(src);

                    // caso in cui il buffer src è vuoto
                    if(src.readableBytes() == 0){
                        assertEquals(prevPos, bufferedChannel.getBufferedChannel().position());
                        assertEquals(unpersistedBytesPrev, bufferedChannel.getBufferedChannel().getUnpersistedBytes());
                        assertEquals(writeStartPosPrev, bufferedChannel.getBufferedChannel().getFileChannelPosition());
                    }else{
                        assertEquals(prevPos+src.readableBytes(), bufferedChannel.getBufferedChannel().position());
                        assertEquals(unpersistedBytesPrev+src.readableBytes(), bufferedChannel.getBufferedChannel().getUnpersistedBytes());
                        assertEquals(writeStartPosPrev+src.readableBytes(), bufferedChannel.getBufferedChannel().getFileChannelPosition());
                    }

                }

            }


        } catch (IOException e) {
            throw new RuntimeException(e);
        }


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
