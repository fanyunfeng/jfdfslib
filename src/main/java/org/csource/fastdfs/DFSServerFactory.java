package org.csource.fastdfs;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

public class DFSServerFactory {
    public StorageServer createStorageServer(String ip, int port, int path) throws IOException, Exception {
        return new StorageServer(ip, port, path);
    }

    public StorageServer createStorageServer(String ip, int port, byte path) throws IOException, Exception {
        return new StorageServer(ip, port, path);
    }

    public TrackerServer createTrackerServer(Socket socket, InetSocketAddress inetSockAddr) throws Exception {
        return new TrackerServer(socket, inetSockAddr);
    }
}
