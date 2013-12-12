package org.csource.fastdfs;

import java.io.IOException;
import java.net.InetSocketAddress;

public class FdfsServerFactory {
    public StorageServer createStorageServer(String ip, int port, int path) throws IOException {
        return new StorageServer(ip, port, path);
    }

    public StorageServer createStorageServer(String ip, int port, byte path) throws IOException {
        return new StorageServer(ip, port, path);
    }

    public TrackerServer createTrackerServer(InetSocketAddress inetSockAddr) throws IOException {
        return new TrackerServer(inetSockAddr);
    }
    
    public void close(){
        
    }
    
    protected void finalize() throws Throwable {
        close();
    }
}
