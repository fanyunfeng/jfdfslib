package org.csource.common.pool;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.csource.fastdfs.TrackerServer;

public class PooledTrackerServer extends TrackerServer implements PooledServer {
    GenericObjectPool<PooledStorageServer> pool;

    public PooledTrackerServer(Socket sock, InetSocketAddress inetSockAddr) {
        super(sock, inetSockAddr);
    }

    @Override
    public void close() throws IOException {

    }

    public void finalClose() throws IOException {
        super.close();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> void setPool(GenericObjectPool<T> pool) {
        this.pool = (GenericObjectPool<PooledStorageServer>) pool;
    }
    
    protected void finalize() throws Throwable {
        finalClose();
    }
}
