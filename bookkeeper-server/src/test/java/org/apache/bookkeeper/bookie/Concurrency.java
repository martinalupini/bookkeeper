package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import org.apache.bookkeeper.bookie.storage.ldb.WriteCache;

import static org.apache.bookkeeper.bookie.Util.getWrittenByteBuf;

public class Concurrency implements Runnable {

    private ByteBuf buf;
    private WriteCache writeCache;

    public void run() {
        writeCache.put(0,3, buf);
    }

    public Concurrency(WriteCache writeCache, ByteBuf buf) {
        this.writeCache = writeCache;
        this.buf = buf;
    }
}
