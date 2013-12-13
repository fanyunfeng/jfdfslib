package org.csource.fastdfs.pool;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.csource.common.Config;
import org.csource.fastdfs.FdfsServerFactory;
import org.csource.fastdfs.StorageServer;
import org.csource.fastdfs.TrackerServer;

public class PooledFdfsServerFactory extends FdfsServerFactory {

    static class ServerPool<T> extends GenericKeyedObjectPool<InetSocketAddress, T> {
        public ServerPool(KeyedPooledObjectFactory<InetSocketAddress, T> factory,
                GenericKeyedObjectPoolConfig poolConfig) {
            super(factory, poolConfig);
        }
    }

    private ServerPool<PooledTrackerServer> trackerServers;
    private ServerPool<PooledStorageServer> storageServers;

    public PooledFdfsServerFactory(Config config) {
        initTrackerConfig(config);
        initStorageConfig(config);
    }

    private void initTrackerConfig(Config config) {
        GenericKeyedObjectPoolConfig poolConfig = new GenericKeyedObjectPoolConfig();

        poolConfig.setLifo(config.getBooleanValue("pool.tracker.lifo", true));
        poolConfig.setMaxTotalPerKey(config.getIntValue("pool.tracker.maxTotal", 10));
        poolConfig.setMinIdlePerKey(config.getIntValue("pool.tracker.minIdle", 0));
        poolConfig.setMaxIdlePerKey(config.getIntValue("pool.tracker.maxIdle", 4));
        poolConfig.setMaxWaitMillis(config.getIntValue("pool.tracker.maxWaitMillis", 10 * 1000));
        poolConfig.setMinEvictableIdleTimeMillis(config.getIntValue("pool.tracker.minEvictableIdleTimeMillis",
                60 * 1000));
        poolConfig.setTestWhileIdle(config.getBooleanValue("pool.tracker.testWhileIdle", true));
        poolConfig.setTimeBetweenEvictionRunsMillis(config.getIntValue("pool.tracker.timeBetweenEvictionRunsMillis",
                20 * 1000));

        trackerServers = new ServerPool<PooledTrackerServer>(new PooledServerFactory<PooledTrackerServer>() {
            @Override
            public PooledTrackerServer create(InetSocketAddress address) throws IOException {
                return new PooledTrackerServer(address);
            }
        }, poolConfig);
    }

    private void initStorageConfig(Config config) {
        GenericKeyedObjectPoolConfig poolConfig = new GenericKeyedObjectPoolConfig();

        poolConfig.setLifo(config.getBooleanValue("pool.storage.lifo", true));
        poolConfig.setMaxTotalPerKey(config.getIntValue("pool.storage.maxTotal", 20));
        poolConfig.setMinIdlePerKey(config.getIntValue("pool.storage.minIdle", 0));
        poolConfig.setMaxIdlePerKey(config.getIntValue("pool.storage.maxIdle", 2));
        poolConfig.setMaxWaitMillis(config.getIntValue("pool.storage.maxWaitMillis", 10 * 1000));
        poolConfig.setMinEvictableIdleTimeMillis(config.getIntValue("pool.storage.minEvictableIdleTimeMillis",
                60 * 1000));
        poolConfig.setTestWhileIdle(config.getBooleanValue("pool.storage.testWhileIdle", true));
        poolConfig.setTimeBetweenEvictionRunsMillis(config.getIntValue("pool.storage.timeBetweenEvictionRunsMillis",
                20 * 1000));

        storageServers = new ServerPool<PooledStorageServer>(new PooledServerFactory<PooledStorageServer>() {
            @Override
            public PooledStorageServer create(InetSocketAddress address) throws IOException {
                return new PooledStorageServer(address, 0);
            }
        }, poolConfig);
    }

    public StorageServer createStorageServer(String ip, int port, int path) {
        try {
            InetSocketAddress address = new InetSocketAddress(ip, port);
            StorageServer server = storageServers.borrowObject(address);

            server.setStorePathIndex(path);

            PooledServer pooledServer = (PooledServer) server;
            pooledServer.setPool(storageServers);

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

            PooledServer pooledServer = (PooledServer) server;
            pooledServer.setPool(storageServers);

            return server;
        } catch (Exception e) {
            throw new PoolException(e);
        }
    }

    public TrackerServer createTrackerServer(InetSocketAddress address) {
        try {
            TrackerServer server;

            server = trackerServers.borrowObject(address);

            PooledServer pooledServer = (PooledServer) server;
            pooledServer.setPool(trackerServers);

            return server;
        } catch (Exception e) {
            throw new PoolException(e);
        }
    }

    public void close() {
        trackerServers.close();
        storageServers.close();
    }
}
