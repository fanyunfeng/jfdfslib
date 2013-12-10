package org.csource.common.pool;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

public class ServerPool<T> {
    private GenericObjectPoolConfig config;
    private PooledServerFactory<T> factory;

    public GenericObjectPoolConfig getConfig() {
        return config;
    }

    public void setConfig(GenericObjectPoolConfig config) {
        this.config = config;
    }

    public PooledObjectFactory<T> getFactory() {
        return factory;
    }

    public void setFactory(PooledServerFactory<T> factory) {
        this.factory = factory;
    }

    private ConcurrentHashMap<InetSocketAddress, GenericObjectPool<T>> servers = new ConcurrentHashMap<InetSocketAddress, GenericObjectPool<T>>();

    public T borrowObject(InetSocketAddress address) throws Exception {
        GenericObjectPool<T> pool = servers.get(address);

        if (pool == null) {
            // create new factory
            PooledServerFactory<T> _factory = factory.clone();
            _factory.setAddress(address);

            // create pool
            GenericObjectPool<T> _pool = new GenericObjectPool<T>(_factory, config);

            synchronized (servers) {
                pool = servers.get(address);

                if (pool == null) {
                    pool = _pool;
                    servers.put(address, _pool);
                    _pool = null;
                }
            }

            if (_pool != null) {
                _pool.close();
            }
        }

        T ret = pool.borrowObject();

        PooledServer server = (PooledServer) ret;
        server.setPool(pool);

        return ret;
    }

    public void close() {
        for (GenericObjectPool<T> pool : servers.values()) {
            pool.close();
        }
    }
    
    
}
