package org.csource.common.pool;

import java.io.IOException;

import org.apache.commons.pool2.impl.GenericObjectPool;

public interface PooledServer {
    <T> void setPool(GenericObjectPool<T> pool);
    
    public void finalClose() throws IOException;
}
