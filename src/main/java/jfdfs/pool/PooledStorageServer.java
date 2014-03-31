package jfdfs.pool;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import jfdfs.core.StorageServer;

public class PooledStorageServer extends StorageServer implements PooledServer {
    private static final Log log = LogFactory.getLog(PooledStorageServer.class);

    private FdfsKeyedObjectPool<PooledStorageServer> pool;

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
    public <T> void setPool(FdfsKeyedObjectPool<T> pool) {
        this.pool = (FdfsKeyedObjectPool<PooledStorageServer>) pool;
    }

    @Override
    public void closePooledObject() {
        try {
            pool.invalidateObject(getInetSocketAddress(), this);
        } catch (Exception e) {
            log.error("closePooledObject:", e);
        }
    }
}
