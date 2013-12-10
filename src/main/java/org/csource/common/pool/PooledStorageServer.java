package org.csource.common.pool;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.csource.fastdfs.StorageServer;

public class PooledStorageServer extends StorageServer implements PooledServer {
    GenericObjectPool<PooledStorageServer> pool;

    public PooledStorageServer(String ip_addr, int port, byte store_path) throws IOException {
        super(ip_addr, port, store_path);
    }

    public PooledStorageServer(InetSocketAddress address, int store_path) throws IOException {
        super(address.getHostName(), address.getPort(), store_path);
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
