package jfdfs.pool;

import java.net.InetSocketAddress;

import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;

public class FdfsKeyedObjectPool<T> extends GenericKeyedObjectPool<InetSocketAddress, T> {

    public FdfsKeyedObjectPool(KeyedPooledObjectFactory<InetSocketAddress, T> factory,
            GenericKeyedObjectPoolConfig config) {

        super(factory, config);

        if (config.getTestWhileIdle()
                && config.getTimeBetweenEvictionRunsMillis() != GenericKeyedObjectPoolConfig.DEFAULT_TIME_BETWEEN_EVICTION_RUNS_MILLIS
                && config.getTimeBetweenEvictionRunsMillis() < config.getMinEvictableIdleTimeMillis()) {

            this.setTimeBetweenEvictionRunsMillis(config.getTimeBetweenEvictionRunsMillis());
        }
    }
}
