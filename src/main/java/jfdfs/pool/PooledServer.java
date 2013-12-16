package jfdfs.pool;

import java.io.IOException;
import java.net.Socket;

public interface PooledServer {
    <T> void setPool(PooledFdfsServerFactory.ServerPool<T> pool);

    public void finalClose() throws IOException;

    public Socket getSocket() throws IOException;
}
