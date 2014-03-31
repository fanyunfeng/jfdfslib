package jfdfs.pool;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import jfdfs.core.TrackerServer;

public class PooledTrackerServer extends TrackerServer implements PooledServer {
    private static final Log log = LogFactory.getLog(PooledTrackerServer.class);

    private FdfsKeyedObjectPool<PooledTrackerServer> pool;

    public PooledTrackerServer(InetSocketAddress address) throws IOException {
        super(address);
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
        this.pool = (FdfsKeyedObjectPool<PooledTrackerServer>) pool;
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
