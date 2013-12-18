package jfdfs.pool;

import java.io.IOException;
import java.net.InetSocketAddress;

import jfdfs.common.Config;
import jfdfs.core.FdfsServerFactory;
import jfdfs.core.StorageServer;
import jfdfs.core.TrackerServer;

import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;

public class PooledFdfsServerFactory extends FdfsServerFactory {

    static class ServerPool<T> extends GenericKeyedObjectPool<InetSocketAddress, T> {
        public ServerPool(KeyedPooledObjectFactory<InetSocketAddress, T> factory,
                GenericKeyedObjectPoolConfig poolConfig) {
            super(factory, poolConfig);
        }
    }

    private ServerPool<PooledTrackerServer> trackerServers = null;
    private ServerPool<PooledStorageServer> storageServers = null;

    public PooledFdfsServerFactory(Config config) {
        initTrackerConfig(config);
        initStorageConfig(config);
    }

    private void initTrackerConfig(Config config) {
        GenericKeyedObjectPoolConfig poolConfig = new GenericKeyedObjectPoolConfig();

        poolConfig.setLifo(config.getBooleanValue("pool.tracker.lifo", true));
        poolConfig.setMaxTotalPerKey(config.getIntValue("pool.tracker.maxTotal", 10));
        poolConfig.setMinIdlePerKey(config.getIntValue("pool.tracker.minIdle", 2));
        poolConfig.setMaxIdlePerKey(config.getIntValue("pool.tracker.maxIdle", 4));
        poolConfig.setMaxWaitMillis(config.getIntValue("pool.tracker.maxWaitMillis", 10 * 1000));
        poolConfig.setMinEvictableIdleTimeMillis(config.getIntValue("pool.tracker.minEvictableIdleTimeMillis",
                20 * 1000));
        poolConfig.setTestWhileIdle(config.getBooleanValue("pool.tracker.testWhileIdle", true));
        poolConfig.setTimeBetweenEvictionRunsMillis(config.getIntValue("pool.tracker.timeBetweenEvictionRunsMillis",
                10 * 1000));

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
        poolConfig.setMinIdlePerKey(config.getIntValue("pool.storage.minIdle", 2));
        poolConfig.setMaxIdlePerKey(config.getIntValue("pool.storage.maxIdle", 8));
        poolConfig.setMaxWaitMillis(config.getIntValue("pool.storage.maxWaitMillis", 10 * 1000));
        poolConfig.setMinEvictableIdleTimeMillis(config.getIntValue("pool.storage.minEvictableIdleTimeMillis",
                20 * 1000));
        poolConfig.setTestWhileIdle(config.getBooleanValue("pool.storage.testWhileIdle", true));
        poolConfig.setTimeBetweenEvictionRunsMillis(config.getIntValue("pool.storage.timeBetweenEvictionRunsMillis",
                10 * 1000));

        storageServers = new ServerPool<PooledStorageServer>(new PooledServerFactory<PooledStorageServer>() {
            @Override
            public PooledStorageServer create(InetSocketAddress address) throws IOException {
                return new PooledStorageServer(address, 0);
            }
        }, poolConfig);
    }

    public StorageServer createStorageServer(InetSocketAddress address, int path) {
        try {
            StorageServer server = storageServers.borrowObject(address);

            server.setStorePathIndex(path);

            PooledServer pooledServer = (PooledServer) server;
            pooledServer.setPool(storageServers);

            return server;
        } catch (Exception e) {
            throw new PoolException(e);
        }
    }

    public StorageServer createStorageServer(InetSocketAddress address, byte path) {
        try {
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
        if (trackerServers != null) {
            trackerServers.close();
            trackerServers = null;
        }

        if (storageServers != null) {
            storageServers.close();
            storageServers = null;
        }
    }
}
