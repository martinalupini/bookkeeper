package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class Util {

    public static ByteBufAllocator getInvalidAllocator(){
        ByteBufAllocator invalidAllocator = mock(ByteBufAllocator.class);
        when(invalidAllocator.buffer()).thenReturn(null);
        when(invalidAllocator.directBuffer(anyInt())).thenReturn(null);

        return invalidAllocator;
    }

    public static FileChannel readOnlyFileChannel(String name) throws IOException {
        Path path = Paths.get(name);
        if (Files.exists(path)) {
            Files.delete(path);
        }
        Files.createFile(path);
        return FileChannel.open(path, StandardOpenOption.READ);
    }

    public static FileChannel validFileChannel(String name) throws IOException {
        Path path = Paths.get(name);
        if (Files.exists(path)) {
            Files.delete(path);
        }
        Files.createFile(path);
        return FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
    }

    public static FileChannel closedFileChannel(String name) throws IOException {
        Path path = Paths.get(name);
        if (Files.exists(path)) {
            Files.delete(path);
        }
        Files.createFile(path);
        FileChannel fc = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
        fc.close();
        return fc;
    }

    public static FileChannel writtenFileChannel(String name) throws IOException {
        Path path = Paths.get(name);
        if (Files.exists(path)) {
            Files.delete(path);
        }
        Files.createFile(path);

        ByteBuffer buff = ByteBuffer.wrap("Just some content".getBytes(StandardCharsets.UTF_8));
        FileChannel fc = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
        fc.write(buff);
        return fc;
    }

    public static ByteBuf getWrittenByteBuf(){
        byte[] testData = "Hello, world!".getBytes();
        ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer();
        byteBuf.writeBytes(testData);

        return byteBuf;
    }

    public static ByteBuf getByteBuf(String data) {
        byte[] testData = data.getBytes();
        ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer();
        byteBuf.writeBytes(testData);

        return byteBuf;
    }

    public static ByteBuf getInvalidByteBuf(){
        ByteBuf buf = mock(ByteBuf.class);
        when(buf.readableBytes()).thenReturn(2);
        when(buf.readerIndex()).thenReturn(-1);
        return buf;
    }


}
