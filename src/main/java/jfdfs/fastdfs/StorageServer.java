/**
 * Copyright (C) 2008 Happy Fish / YuQing
 *
 * FastDFS Java Client may be copied only under the terms of the GNU Lesser
 * General Public License (LGPL).
 * Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
 */

package jfdfs.fastdfs;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Storage Server Info
 * 
 * @author Happy Fish / YuQing
 * @version Version 1.11
 */
public class StorageServer extends TrackerServer {
    private int store_path_index = 0;

    /**
     * Constructor
     * 
     * @param ip_addr the ip address of storage server
     * @param port the port of storage server
     * @param store_path the store path index on the storage server
     */
    public StorageServer(InetSocketAddress addr, int store_path) throws IOException {
        super(addr);
        this.store_path_index = store_path;
    }

    /**
     * Constructor
     * 
     * @param ip_addr the ip address of storage server
     * @param port the port of storage server
     * @param store_path the store path index on the storage server
     */
    public StorageServer(InetSocketAddress addr, byte store_path) throws IOException {
        super(addr);
        if (store_path < 0) {
            this.store_path_index = 256 + store_path;
        } else {
            this.store_path_index = store_path;
        }
    }

    /**
     * @return the store path index on the storage server
     */
    public int getStorePathIndex() {
        return this.store_path_index;
    }

    public void setStorePathIndex(byte store_path) {
        if (store_path < 0) {
            this.store_path_index = 256 + store_path;
        } else {
            this.store_path_index = store_path;
        }
    }

    public void setStorePathIndex(int store_path) {
        this.store_path_index = store_path;
    }
}
