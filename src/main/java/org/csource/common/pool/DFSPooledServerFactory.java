package org.csource.common.pool;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.csource.fastdfs.DFSServerFactory;
import org.csource.fastdfs.StorageServer;
import org.csource.fastdfs.TrackerServer;

public class DFSPooledServerFactory extends DFSServerFactory {
    private ServerPool<PooledTrackerServer> trackerServers = new ServerPool<PooledTrackerServer>();
    private ServerPool<PooledStorageServer> storageServers = new ServerPool<PooledStorageServer>();

    private GenericObjectPoolConfig storageConfig;
    private GenericObjectPoolConfig trackerConfig;

    public void init() {
        {
            GenericObjectPoolConfig config = new GenericObjectPoolConfig();

            config.setMaxTotal(20);
            config.setLifo(true);
            config.setMaxIdle(2);

            trackerServers.setConfig(config);
            trackerServers.setFactory(new PooledServerFactory<PooledTrackerServer>() {
                
                @Override
                public PooledTrackerServer create() {
                    return new PooledTrackerServer(null, this.getAddress());
                }
            });
        }

        {
            GenericObjectPoolConfig config = new GenericObjectPoolConfig();

            config.setMaxTotal(20);
            config.setLifo(true);
            config.setMaxIdle(2);

            storageServers.setConfig(config);
            storageServers.setFactory(new PooledServerFactory<PooledStorageServer>() {
                
                @Override
                public PooledStorageServer create() throws IOException {
                    return new PooledStorageServer(this.getAddress(), 0);
                }
            });
        }
    }

    public StorageServer createStorageServer(String ip, int port, int path) throws Exception {
        InetSocketAddress address = new InetSocketAddress(ip, port);
        StorageServer server = storageServers.borrowObject(address);

        server.setStorePathIndex(path);

        return server;
    }

    public StorageServer createStorageServer(String ip, int port, byte path) throws Exception {
        InetSocketAddress address = new InetSocketAddress(ip, port);
        StorageServer server = storageServers.borrowObject(address);

        server.setStorePathIndex(path);

        return server;
    }

    public TrackerServer createTrackerServer(Socket socket, InetSocketAddress address) throws Exception {
        StorageServer server = storageServers.borrowObject(address);

        return server;
    }
}
