package jfdfs.pool;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import jfdfs.common.Config;
import jfdfs.core.FdfsServerFactory;
import jfdfs.core.StorageServer;
import jfdfs.core.TrackerServer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool2.impl.DefaultPooledObjectInfo;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;

public class PooledFdfsServerFactory extends FdfsServerFactory {
    private static final Log log = LogFactory.getLog(PooledFdfsServerFactory.class);

    private FdfsKeyedObjectPool<PooledTrackerServer> trackerServers = null;
    private FdfsKeyedObjectPool<PooledStorageServer> storageServers = null;

    public PooledFdfsServerFactory(Config config) {
        initTrackerConfig(config);
        initStorageConfig(config);
    }

    private void dumpPool(Map<String, List<DefaultPooledObjectInfo>> list) {
        for (Map.Entry<String, List<DefaultPooledObjectInfo>> i : list.entrySet()) {

            log.error("pool:" + i.getKey().toString());

            List<DefaultPooledObjectInfo> x = i.getValue();

            for (DefaultPooledObjectInfo xx : x) {
                log.error("\tinstance:" + xx.getPooledObjectToString());
            }
        }
    }

    private void initTrackerConfig(Config cf) {
        GenericKeyedObjectPoolConfig pc = new GenericKeyedObjectPoolConfig();

        pc.setLifo(cf.getBooleanValue("pool.tracker.lifo", true));
        pc.setMaxTotalPerKey(cf.getIntValue("pool.tracker.maxTotal", 10));
        pc.setMinIdlePerKey(cf.getIntValue("pool.tracker.minIdle", 2));
        pc.setMaxIdlePerKey(cf.getIntValue("pool.tracker.maxIdle", 4));
        pc.setMaxWaitMillis(cf.getIntValue("pool.tracker.maxWaitMillis", 10 * 1000));
        pc.setSoftMinEvictableIdleTimeMillis(cf.getIntValue("pool.tracker.softMinEvictableIdleTimeMillis",
                1000 * 60 * 2));
        pc.setTestWhileIdle(cf.getBooleanValue("pool.tracker.testWhileIdle", true));
        pc.setTimeBetweenEvictionRunsMillis(cf.getIntValue("pool.tracker.timeBetweenEvictionRunsMillis", 1000 * 12));
        pc.setMinEvictableIdleTimeMillis(cf.getIntValue("pool.tracker.minEvictableIdleTimeMillis", 1000 * 60 * 5));

        trackerServers = new FdfsKeyedObjectPool<PooledTrackerServer>(new PooledServerFactory<PooledTrackerServer>() {
            @Override
            public PooledTrackerServer create(InetSocketAddress address) throws IOException {

                PooledTrackerServer srv = new PooledTrackerServer(address);

                if (log.isDebugEnabled()) {
                    log.debug(String.format("create tracker server:%s address:%X", address.toString(), srv.hashCode()));
                }

                return srv;
            }
        }, pc);
    }

    private void initStorageConfig(Config cf) {
        GenericKeyedObjectPoolConfig pc = new GenericKeyedObjectPoolConfig();

        pc.setLifo(cf.getBooleanValue("pool.storage.lifo", true));
        pc.setMaxTotalPerKey(cf.getIntValue("pool.storage.maxTotal", 20));
        pc.setMinIdlePerKey(cf.getIntValue("pool.storage.minIdle", 2));
        pc.setMaxIdlePerKey(cf.getIntValue("pool.storage.maxIdle", 8));
        pc.setMaxWaitMillis(cf.getIntValue("pool.storage.maxWaitMillis", 10 * 1000));
        pc.setSoftMinEvictableIdleTimeMillis(cf.getIntValue("pool.storage.softMinEvictableIdleTimeMillis",
                1000 * 60 * 2));
        pc.setTestWhileIdle(cf.getBooleanValue("pool.storage.testWhileIdle", true));
        pc.setTimeBetweenEvictionRunsMillis(cf.getIntValue("pool.storage.timeBetweenEvictionRunsMillis", 1000 * 12));
        pc.setMinEvictableIdleTimeMillis(cf.getIntValue("pool.storage.minEvictableIdleTimeMillis", 1000 * 60 * 3));

        storageServers = new FdfsKeyedObjectPool<PooledStorageServer>(new PooledServerFactory<PooledStorageServer>() {
            @Override
            public PooledStorageServer create(InetSocketAddress address) throws IOException {

                PooledStorageServer srv = new PooledStorageServer(address, 0);

                if (log.isDebugEnabled()) {
                    log.debug(String.format("create storge server:%s address:%X", address.toString(), srv.hashCode()));
                }

                return srv;
            }
        }, pc);
    }

    public StorageServer createStorageServer(InetSocketAddress address, int path) {
        try {
            StorageServer server = storageServers.borrowObject(address);

            server.setStorePathIndex(path);

            PooledServer pooledServer = (PooledServer) server;
            pooledServer.setPool(storageServers);

            return server;
        } catch (NoSuchElementException e) {
            Map<String, List<DefaultPooledObjectInfo>> list = storageServers.listAllObjects();

            dumpPool(list);

            throw new PoolException(e);
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
        } catch (NoSuchElementException e) {
            Map<String, List<DefaultPooledObjectInfo>> list = storageServers.listAllObjects();

            dumpPool(list);

            throw new PoolException(e);
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
        } catch (NoSuchElementException e) {
            Map<String, List<DefaultPooledObjectInfo>> list = storageServers.listAllObjects();

            dumpPool(list);

            throw new PoolException(e);
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
