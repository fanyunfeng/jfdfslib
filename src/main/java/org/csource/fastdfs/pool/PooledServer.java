package org.csource.fastdfs.pool;

import java.io.IOException;
import java.net.Socket;

import org.apache.commons.pool2.impl.GenericObjectPool;

public interface PooledServer {
    <T> void setPool(GenericObjectPool<T> pool);

    public void finalClose() throws IOException;

    public Socket getSocket() throws IOException;
}
