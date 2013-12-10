package org.csource.fastdfs;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

public class FdfsServerFactory {
    public StorageServer createStorageServer(String ip, int port, int path) throws IOException {
        return new StorageServer(ip, port, path);
    }

    public StorageServer createStorageServer(String ip, int port, byte path) throws IOException {
        return new StorageServer(ip, port, path);
    }

    public TrackerServer createTrackerServer(Socket socket, InetSocketAddress inetSockAddr) {
        return new TrackerServer(socket, inetSockAddr);
    }
}
