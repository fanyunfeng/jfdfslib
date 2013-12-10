package org.csource.common.pool;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;

public abstract class PooledServerFactory<T> implements PooledObjectFactory<T>, Cloneable {
    private InetSocketAddress address;

    public InetSocketAddress getAddress() {
        return address;
    }

    public void setAddress(InetSocketAddress address) {
        this.address = address;
    }

    public abstract T create() throws IOException;

    @Override
    public PooledObject<T> makeObject() throws Exception {
        return new DefaultPooledObject<T>(create());
    }

    @Override
    public void destroyObject(PooledObject<T> p) throws Exception {
        PooledServer server = (PooledServer) p.getObject();
        
        server.finalClose();
    }

    @Override
    public boolean validateObject(PooledObject<T> p) {
        return true;
    }

    @Override
    public void activateObject(PooledObject<T> p) throws Exception {

    }

    @Override
    public void passivateObject(PooledObject<T> p) throws Exception {

    }
    
    @SuppressWarnings("unchecked")
    public PooledServerFactory<T> clone() throws CloneNotSupportedException{
        return (PooledServerFactory<T>) super.clone();
    }
}
