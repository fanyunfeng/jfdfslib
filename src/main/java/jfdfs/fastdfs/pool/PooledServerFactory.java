package jfdfs.fastdfs.pool;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

import jfdfs.fastdfs.ProtoCommon;

import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

public abstract class PooledServerFactory<T> implements KeyedPooledObjectFactory<InetSocketAddress, T> {

    public abstract T create(InetSocketAddress address) throws IOException;

    @Override
    public PooledObject<T> makeObject(InetSocketAddress address) throws Exception {
        return new DefaultPooledObject<T>(create(address));
    }

    @Override
    public void destroyObject(InetSocketAddress key, PooledObject<T> p) throws Exception {
        PooledServer server = (PooledServer) p.getObject();

        server.finalClose();
    }

    @Override
    public boolean validateObject(InetSocketAddress key, PooledObject<T> p) {
        PooledServer server = (PooledServer) p.getObject();

        Socket sock;
        try {
            sock = server.getSocket();

            if (sock.isClosed()) {
                return false;
            }

            return ProtoCommon.activeTest(sock);
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    public void activateObject(InetSocketAddress key, PooledObject<T> p) throws Exception {

    }

    @Override
    public void passivateObject(InetSocketAddress key, PooledObject<T> p) throws Exception {

    }
}
