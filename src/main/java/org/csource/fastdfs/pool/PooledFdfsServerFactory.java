package org.csource.fastdfs.pool;

import java.io.IOException;
import java.net.InetSocketAddress;

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

        poolConfig.setMaxTotal(config.getIntValue("pool.tracker.maxTotal", 10));
        poolConfig.setLifo(config.getBooleanValue("pool.tracker.lifo", true));
        poolConfig.setMinIdle(config.getIntValue("pool.tracker.minIdle", 0));
        poolConfig.setMaxIdle(config.getIntValue("pool.tracker.maxIdle", 4));
        poolConfig.setMaxWaitMillis(config.getIntValue("pool.tracker.maxWaitMillis", 10 * 1000));
        poolConfig.setMinEvictableIdleTimeMillis(config.getIntValue("pool.tracker.minEvictableIdleTimeMillis",
                60 * 1000));
        poolConfig.setTestWhileIdle(config.getBooleanValue("pool.tracker.testWhileIdle", true));
        poolConfig.setTimeBetweenEvictionRunsMillis(config.getIntValue("pool.tracker.timeBetweenEvictionRunsMillis",
                20 * 1000));

        trackerServers.setConfig(poolConfig);
        trackerServers.setFactory(new PooledServerFactory<PooledTrackerServer>() {

            @Override
            public PooledTrackerServer create() throws IOException {
                return new PooledTrackerServer(this.getAddress());
            }
        });
    }

    private void initStorageConfig(Config config) {
        GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();

        poolConfig.setMaxTotal(config.getIntValue("pool.storage.maxTotal", 20));
        poolConfig.setLifo(config.getBooleanValue("pool.storage.lifo", true));
        poolConfig.setMinIdle(config.getIntValue("pool.storage.minIdle", 0));
        poolConfig.setMaxIdle(config.getIntValue("pool.storage.maxIdle", 2));
        poolConfig.setMaxWaitMillis(config.getIntValue("pool.storage.maxWaitMillis", 10 * 1000));
        poolConfig.setMinEvictableIdleTimeMillis(config.getIntValue("pool.storage.minEvictableIdleTimeMillis",
                60 * 1000));
        poolConfig.setTestWhileIdle(config.getBooleanValue("pool.storage.testWhileIdle", true));
        poolConfig.setTimeBetweenEvictionRunsMillis(config.getIntValue("pool.storage.timeBetweenEvictionRunsMillis",
                20 * 1000));

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

    public TrackerServer createTrackerServer(InetSocketAddress address) {
        try {
            TrackerServer server;

            server = trackerServers.borrowObject(address);

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
