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
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.bookkeeper.bookie.Util.*;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.junit.jupiter.api.Assertions.*;

// Indica che una singola istanza della classe di test verrà utilizzata per tutti i metodi di test
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BufferedChannelWriteTest {

    private List<String> files = new ArrayList<>();
    private List<BuffChannSpecific> bufferedChannelList = new ArrayList<>();
    private int istanza=1;


    @BeforeAll
    void setUp() throws IOException {

        BufferedChannel buf1 = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, validFileChannel("f1"), 4096, 4096, 4096);
        files.add("f1");
        bufferedChannelList.add(new BuffChannSpecific(buf1, false, false));

        BufferedChannel buf2 = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, validFileChannel("f2"), 1, 1, 2);
        files.add("f2");
        bufferedChannelList.add(new BuffChannSpecific(buf2, false, false));

        BufferedChannel buf3 = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, validFileChannel("f3"), 1, 1, -1);
        files.add("f3");
        bufferedChannelList.add(new BuffChannSpecific(buf3, false, false));

        BufferedChannel buf4 = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, validFileChannel("f4"), 4096, 1, 1024);
        files.add("f4");
        bufferedChannelList.add(new BuffChannSpecific(buf4, false, false));

        BufferedChannel buf5 = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, validFileChannel("f5"), 4096, 1, 4097);
        files.add("f5");
        bufferedChannelList.add(new BuffChannSpecific(buf5, false, false));

        //istanze problematiche
        BufferedChannel buf6 = new BufferedChannel(getInvalidAllocator(), validFileChannel("f6"), 1, 1, 0);
        files.add("f6");
        bufferedChannelList.add(new BuffChannSpecific(buf6, true, false));

        BufferedChannel buf7 = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, validFileChannel("f7"), 0, 0, 0);
        files.add("f7");
        bufferedChannelList.add(new BuffChannSpecific(buf7, false, false));

        BufferedChannel buf8 = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, validFileChannel("f8"), 0, 0, 1);
        files.add("f8");
        bufferedChannelList.add(new BuffChannSpecific(buf8, false, false));

        BufferedChannel buf9 = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, validFileChannel("f9"), 4096, 1, 1024);
        files.add("f9");
        buf9.close();
        bufferedChannelList.add(new BuffChannSpecific(buf9, false, false));

        BufferedChannel buf10 = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, readOnlyFileChannel("f10"), 1, 1, 0);
        files.add("f10");
        bufferedChannelList.add(new BuffChannSpecific(buf10, false, true));

    }

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
    static Stream<Arguments> provideWriteArguments(){
        return Stream.of(
                Arguments.of(getInvalidByteBuf(), Exception.class),
                Arguments.of(null, NullPointerException.class),
                Arguments.of(Unpooled.buffer(0), null),
                Arguments.of(getWrittenByteBuf(), null)
        );
    }


    @ParameterizedTest
    @Timeout(5)
    @MethodSource("provideWriteArguments")
    void testBufferedChannelWrite(ByteBuf src, Class<Exception> expectedException){
        istanza = 1;

        try {

            // ora testo il metodo write su tutte le possibili istanze ottenute in precedenza
            for(BuffChannSpecific bufferedChannel : bufferedChannelList) {

                System.out.println("Starting testing instance "+istanza);
                // caso in cui il buffered channel è chiuso, il fc non è valido, l'allocatore non è valido oppure mi aspetto un'eccezione
                if (bufferedChannel.getBufferedChannel().isClosed() || bufferedChannel.isInvalidFC() || bufferedChannel.isInvalidAlloc() || expectedException != null) {
                    // Verifica che venga lanciata l'eccezione attesa
                    assertThrows(Exception.class, () -> {
                        bufferedChannel.getBufferedChannel().write(src);
                    });
                } else {

                    // tengo traccia dei valori di position, unpersistedBytes e writeBufferStartPosition prima della scrittura
                    long prevPos = bufferedChannel.getBufferedChannel().position();
                    long unpersistedBytesPrev = bufferedChannel.getBufferedChannel().getUnpersistedBytes();
                    long writeStartPosPrev = bufferedChannel.getBufferedChannel().getFileChannelPosition();

                    long bound = bufferedChannel.getBufferedChannel().getUnpersistedBytesBound();
                    long src_bytes = src.readableBytes();
                    long cap = bufferedChannel.getBufferedChannel().getWriteBuffer().capacity();

                    long expectedBytesUnpersisted = 0;

                    bufferedChannel.getBufferedChannel().write(src);

                    if(bufferedChannel.getBufferedChannel().isDoRegularFlushes() && bound < cap){
                        if(bound - unpersistedBytesPrev >= src_bytes){
                            expectedBytesUnpersisted = src_bytes +unpersistedBytesPrev;
                        }else if(bound - unpersistedBytesPrev < src_bytes){
                            expectedBytesUnpersisted = (unpersistedBytesPrev+src_bytes)%bound;
                        }

                    }else{
                        if(cap - unpersistedBytesPrev >= src_bytes ){
                            expectedBytesUnpersisted = src_bytes +unpersistedBytesPrev;
                        }else if(cap - unpersistedBytesPrev < src_bytes ){
                            expectedBytesUnpersisted = (unpersistedBytesPrev+src_bytes)%cap;
                        }

                    }

                    long expectedPos = prevPos + src_bytes;
                    long expectedWriteStartPos = writeStartPosPrev + src_bytes -expectedBytesUnpersisted;

                    assertEquals(expectedPos, bufferedChannel.getBufferedChannel().position);
                    assertEquals(expectedBytesUnpersisted, bufferedChannel.getBufferedChannel().getUnpersistedBytes());
                    assertEquals(expectedWriteStartPos, bufferedChannel.getBufferedChannel().getFileChannelPosition());

                }

                System.out.println("Done testing instance "+istanza);
                istanza ++;

            }


        } catch (IOException e) {
            throw new RuntimeException(e);
        }


    }



    @AfterAll
    public void  cleanUp(){

        //rimozione del file creato dopo aver eseguito il test
        for(String file : files){
            File f = new File(file);
            f.delete();
        }

    }

}

