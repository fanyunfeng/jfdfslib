package jfdfs.core;

import java.io.IOException;
import java.net.InetSocketAddress;

public class FdfsServerFactory {
    public StorageServer createStorageServer(InetSocketAddress addr, int path) throws IOException {
        return new StorageServer(addr, path);
    }

    public StorageServer createStorageServer(InetSocketAddress addr, byte path) throws IOException {
        return new StorageServer(addr, path);
    }

    public TrackerServer createTrackerServer(InetSocketAddress addr) throws IOException {
        return new TrackerServer(addr);
    }
    
    public void close(){
        
    }
    
    protected void finalize() throws Throwable {
        close();
    }
}
