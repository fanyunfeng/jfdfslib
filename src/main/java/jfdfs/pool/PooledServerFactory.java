package jfdfs.pool;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

import jfdfs.core.ProtoCommon;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

public abstract class PooledServerFactory<T> implements KeyedPooledObjectFactory<InetSocketAddress, T> {
    private static final Log log = LogFactory.getLog(PooledServerFactory.class);

    public abstract T create(InetSocketAddress address) throws IOException;

    @Override
    public PooledObject<T> makeObject(InetSocketAddress address) throws Exception {
        return new DefaultPooledObject<T>(create(address));
    }

    @Override
    public void destroyObject(InetSocketAddress key, PooledObject<T> p) throws Exception {
        PooledServer server = (PooledServer) p.getObject();

        if (log.isDebugEnabled()) {
            log.debug(String.format("destroyObject key:%s address:%X.", key.toString(), server.hashCode()));
        }

        server.finalClose();
    }

    @Override
    public boolean validateObject(InetSocketAddress key, PooledObject<T> p) {
        PooledServer server = (PooledServer) p.getObject();

        Socket sock;
        try {
            sock = server.getSocket();

            if (sock.isClosed()) {
                if (log.isDebugEnabled()) {
                    log.debug(String.format("validateObject key:%s address:%X. sokcet is closed.", key.toString(),
                            server.hashCode()));
                }
                
                return false;
            }

            if (log.isDebugEnabled()) {
                log.debug(String.format("validateObject key:%s address:%X. check ok.", key.toString(),
                        server.hashCode()));
            }
            
            return ProtoCommon.activeTest(sock);
        } catch (IOException e) {

            log.error(String.format("validateObject address:%X key:%s. exception.", key.toString(), server.hashCode()),
                    e);
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
