/**
 * Copyright (C) 2008 Happy Fish / YuQing
 *
 * FastDFS Java Client may be copied only under the terms of the GNU Lesser
 * General Public License (LGPL).
 * Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
 */

package jfdfs.core;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Tracker server group
 * 
 * @author Happy Fish / YuQing
 * @version Version 1.17
 */
public class TrackerGroup {
    protected Object lock;
    public int allocindex;
    public InetSocketAddress[] trackerServers;

    /**
     * Constructor
     * 
     * @param tracker_servers tracker servers
     */
    public TrackerGroup(InetSocketAddress[] tracker_servers) {
        this.trackerServers = tracker_servers;
        this.lock = new Object();
        this.allocindex = 0;
    }

    public TrackerServer getTrackerServer() throws IOException {
        return getConnection();
    }

    /**
     * return connected tracker server
     * 
     * @return connected tracker server, null for fail
     */
    public TrackerServer getConnection(int serverIndex) throws IOException {
        return ClientGlobal.getFactory().createTrackerServer(this.trackerServers[serverIndex]);
    }

    /**
     * return connected tracker server
     * 
     * @return connected tracker server, null for fail
     */
    public TrackerServer getConnection() throws IOException {
        int current_index;

        synchronized (this.lock) {
            this.allocindex++;
            if (this.allocindex >= this.trackerServers.length) {
                this.allocindex = 0;
            }

            current_index = this.allocindex;
        }

        try {
            return this.getConnection(current_index);
        } catch (IOException ex) {
            System.err.println("connect to server " + this.trackerServers[current_index].getAddress().getHostAddress()
                    + ":" + this.trackerServers[current_index].getPort() + " fail");
            ex.printStackTrace(System.err);
        }

        for (int i = 0; i < this.trackerServers.length; i++) {
            if (i == current_index) {
                continue;
            }

            try {
                TrackerServer trackerServer = this.getConnection(i);

                synchronized (this.lock) {
                    if (this.allocindex == current_index) {
                        this.allocindex = i;
                    }
                }

                return trackerServer;
            } catch (IOException ex) {
                System.err.println("connect to server " + this.trackerServers[i].getAddress().getHostAddress() + ":"
                        + this.trackerServers[i].getPort() + " fail");
                ex.printStackTrace(System.err);
            }
        }

        return null;
    }

    public Object clone() {
        InetSocketAddress[] trackerServers = new InetSocketAddress[this.trackerServers.length];
        for (int i = 0; i < this.trackerServers.length; i++) {
            trackerServers[i] = new InetSocketAddress(this.trackerServers[i].getAddress().getHostAddress(),
                    this.trackerServers[i].getPort());
        }

        return new TrackerGroup(trackerServers);
    }

    /**
     * delete a storage server from the FastDFS cluster
     * 
     * @param trackerGroup the tracker server group
     * @param groupName the group name of storage server
     * @param storageIpAddr the storage server ip address
     * @return true for success, false for fail
     * @throws Exception
     */
    public boolean deleteStorage(String groupName, String storageIpAddr) throws Exception {
        int notFoundCount = 0;
        TrackerServer trackerServer;

        notFoundCount = 0;
        for (int serverIndex = 0; serverIndex < trackerServers.length; serverIndex++) {
            try {
                trackerServer = getConnection(serverIndex);
            } catch (IOException ex) {
                ex.printStackTrace(System.err);
                return false;
            }

            try {

                TrackerClient trackerClient = new TrackerClient(trackerServer);
                StructStorageStat[] storageStats = trackerClient.listStorages(groupName, storageIpAddr);
                if (storageStats == null) {
                    if (trackerClient.getErrorCode() == ProtoCommon.ERR_NO_ENOENT) {
                        notFoundCount++;
                    } else {
                        return false;
                    }
                } else if (storageStats.length == 0) {
                    notFoundCount++;
                } else if (storageStats[0].getStatus() == ProtoCommon.FDFS_STORAGE_STATUS_ONLINE
                        || storageStats[0].getStatus() == ProtoCommon.FDFS_STORAGE_STATUS_ACTIVE) {
                    return false;
                }
            } finally {
                try {
                    trackerServer.close();
                } catch (IOException ex1) {
                    ex1.printStackTrace();
                }
            }
        }

        if (notFoundCount == trackerServers.length) {
            return false;
        }

        notFoundCount = 0;
        for (int serverIndex = 0; serverIndex < trackerServers.length; serverIndex++) {
            try {
                trackerServer = getConnection(serverIndex);
            } catch (IOException ex) {
                System.err.println("connect to server " + trackerServers[serverIndex].getAddress().getHostAddress()
                        + ":" + trackerServers[serverIndex].getPort() + " fail");
                ex.printStackTrace(System.err);
                return false;
            }

            TrackerClient trackerClient = new TrackerClient(trackerServer);

            try {
                if (!trackerClient.deleteStorage(groupName, storageIpAddr)) {
                    if (trackerClient.getErrorCode() != 0) {
                        if (trackerClient.getErrorCode() == ProtoCommon.ERR_NO_ENOENT) {
                            notFoundCount++;
                        } else if (trackerClient.getErrorCode() != ProtoCommon.ERR_NO_EALREADY) {
                            return false;
                        }
                    }
                }
            } finally {
                try {
                    trackerServer.close();
                } catch (IOException ex1) {
                    ex1.printStackTrace();
                }
            }
        }

        if (notFoundCount == trackerServers.length) {
            return false;
        }

        return true;
    }
}
