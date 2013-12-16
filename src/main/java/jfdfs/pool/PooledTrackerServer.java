package jfdfs.pool;

import java.io.IOException;
import java.net.InetSocketAddress;

import jfdfs.core.TrackerServer;
import jfdfs.pool.PooledFdfsServerFactory.ServerPool;


public class PooledTrackerServer extends TrackerServer implements PooledServer {
    PooledFdfsServerFactory.ServerPool<PooledTrackerServer> pool;

    public PooledTrackerServer(InetSocketAddress inetSockAddr) throws IOException {
        super(inetSockAddr);
    }

    @Override
    public void close() throws IOException {
        pool.returnObject(this.getInetSocketAddress(), this);
    }

    public void finalClose() throws IOException {
        super.close();
    }

    protected void finalize() throws Throwable {
        finalClose();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> void setPool(ServerPool<T> pool) {
        this.pool = (PooledFdfsServerFactory.ServerPool<PooledTrackerServer>) pool;
    }
}
