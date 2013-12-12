package org.csource.fastdfs.pool;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.csource.fastdfs.TrackerServer;

public class PooledTrackerServer extends TrackerServer implements PooledServer {
    GenericObjectPool<PooledTrackerServer> pool;

    public PooledTrackerServer(InetSocketAddress inetSockAddr) throws IOException {
        super(inetSockAddr);
    }

    @Override
    public void close() throws IOException {
        pool.returnObject(this);
    }

    public void finalClose() throws IOException {
        super.close();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> void setPool(GenericObjectPool<T> pool) {
        this.pool = (GenericObjectPool<PooledTrackerServer>) pool;
    }
    
    protected void finalize() throws Throwable {
        finalClose();
    }
}
