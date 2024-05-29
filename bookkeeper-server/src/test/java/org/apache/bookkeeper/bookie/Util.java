package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBufAllocator;

import java.io.IOException;
import java.nio.channels.FileChannel;
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
        if (!Files.exists(path)) {
            Files.createFile(path);
        }
        return FileChannel.open(path, StandardOpenOption.READ);
    }

    public static FileChannel validFileChannel(String name) throws IOException {
        Path path = Paths.get(name);
        if (!Files.exists(path)) {
            Files.createFile(path);
        }
        return FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
    }

    public static FileChannel closedFileChannel(String name) throws IOException {
        Path path = Paths.get(name);
        if (!Files.exists(path)) {
            Files.createFile(path);
        }
        FileChannel fc = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
        fc.close();
        return fc;
    }

    public static class BuffChannSpecific{
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
}
