package org.csource.fastdfs.pool;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.csource.common.Config;
import org.csource.fastdfs.FdfsServerFactory;
import org.csource.fastdfs.StorageServer;
import org.csource.fastdfs.TrackerServer;

public class PooledFdfsServerFactory extends FdfsServerFactory {
    private ServerPool<PooledTrackerServer> trackerServers = new ServerPool<PooledTrackerServer>();
    private ServerPool<PooledStorageServer> storageServers = new ServerPool<PooledStorageServer>();

    public PooledFdfsServerFactory(Config config) {
        initTrackerConfig(config);
        initStorageConfig(config);
    }

    private void initTrackerConfig(Config config) {
        GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();

        poolConfig.setMaxTotal(20);
        poolConfig.setLifo(true);
        poolConfig.setMaxIdle(2);
        poolConfig.setTestWhileIdle(true);
        poolConfig.setTimeBetweenEvictionRunsMillis(20 * 1000);

        trackerServers.setConfig(poolConfig);
        trackerServers.setFactory(new PooledServerFactory<PooledTrackerServer>() {

            @Override
            public PooledTrackerServer create() {
                return new PooledTrackerServer(null, this.getAddress());
            }
        });
    }

    private void initStorageConfig(Config config) {
        GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();

        poolConfig.setMaxTotal(20);
        poolConfig.setLifo(true);
        poolConfig.setMaxIdle(2);
        poolConfig.setTestWhileIdle(true);
        poolConfig.setTimeBetweenEvictionRunsMillis(20 * 1000);

        storageServers.setConfig(poolConfig);
        storageServers.setFactory(new PooledServerFactory<PooledStorageServer>() {

            @Override
            public PooledStorageServer create() throws IOException {
                return new PooledStorageServer(this.getAddress(), 0);
            }
        });
    }

    public StorageServer createStorageServer(String ip, int port, int path) {
        try {
            InetSocketAddress address = new InetSocketAddress(ip, port);
            StorageServer server = storageServers.borrowObject(address);

            server.setStorePathIndex(path);

            return server;
        } catch (Exception e) {
            throw new PoolException(e);
        }
    }

    public StorageServer createStorageServer(String ip, int port, byte path) {
        try {
            InetSocketAddress address = new InetSocketAddress(ip, port);
            StorageServer server = storageServers.borrowObject(address);

            server.setStorePathIndex(path);

            return server;
        } catch (Exception e) {
            throw new PoolException(e);
        }
    }

    public TrackerServer createTrackerServer(Socket socket, InetSocketAddress address) {
        try {
            StorageServer server;

            server = storageServers.borrowObject(address);

            return server;
        } catch (Exception e) {
            throw new PoolException(e);
        }
    }
}
