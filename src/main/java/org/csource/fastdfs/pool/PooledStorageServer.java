package org.csource.fastdfs.pool;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.csource.fastdfs.StorageServer;

public class PooledStorageServer extends StorageServer implements PooledServer {
    GenericObjectPool<PooledStorageServer> pool;

    public PooledStorageServer(InetSocketAddress address, byte store_path) throws IOException {
        super(address, store_path);
    }

    public PooledStorageServer(InetSocketAddress address, int store_path) throws IOException {
        super(address, store_path);
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
        this.pool = (GenericObjectPool<PooledStorageServer>) pool;
    }

    protected void finalize() throws Throwable {
        finalClose();
    }
}
