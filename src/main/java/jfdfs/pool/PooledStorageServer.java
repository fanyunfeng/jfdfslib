package jfdfs.pool;

import java.io.IOException;
import java.net.InetSocketAddress;

import jfdfs.core.StorageServer;
import jfdfs.pool.PooledFdfsServerFactory.ServerPool;


public class PooledStorageServer extends StorageServer implements PooledServer {
    PooledFdfsServerFactory.ServerPool<PooledStorageServer> pool;

    public PooledStorageServer(InetSocketAddress address, byte store_path) throws IOException {
        super(address, store_path);
    }

    public PooledStorageServer(InetSocketAddress address, int store_path) throws IOException {
        super(address, store_path);
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
        this.pool = (PooledFdfsServerFactory.ServerPool<PooledStorageServer>) pool;
    }
}
