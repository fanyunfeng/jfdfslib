package jfdfs.template;

import java.io.IOException;

import jfdfs.common.MyException;
import jfdfs.core.ClientGlobal;
import jfdfs.core.StorageClient1;
import jfdfs.core.StorageServer;
import jfdfs.core.TrackerClient;
import jfdfs.core.TrackerServer;
import jfdfs.pool.PooledStorageServer;
import jfdfs.pool.PooledTrackerServer;

public class FastDFSTemplate {
    public <T> T execFileUpload(String groupName, FastDFSCallback<T> op) throws MyException, IOException {
        TrackerServer trackerServer = null;
        StorageServer storageServer = null;

        try {
            trackerServer = ClientGlobal.getTrackerGroup().getTrackerServer();

            TrackerClient tracker = new TrackerClient(trackerServer);

            try {
                storageServer = tracker.getStoreStorage(groupName);
            } catch (IOException e) {
                if (trackerServer instanceof PooledTrackerServer) {
                    PooledTrackerServer tmp = (PooledTrackerServer) trackerServer;

                    tmp.closePooledObject();
                    trackerServer = null;
                }
                throw e;
            }

            closeTrackerServer(trackerServer);
            trackerServer = null;

            StorageClient1 client = new StorageClient1(storageServer);

            try {
                T o = op.run(client);

                return o;
            } catch (IOException e) {
                if (storageServer instanceof PooledStorageServer) {
                    PooledStorageServer tmp = (PooledStorageServer) storageServer;

                    tmp.closePooledObject();
                    storageServer = null;
                }
                throw e;
            }
        } finally {
            closeTrackerServer(trackerServer);
            closeStorageServer(storageServer);
        }
    }

    public <T> T execFileDownload(String file, FastDFSCallback<T> op) throws MyException, IOException {
        String[] parts = new String[] { null, null };

        TrackerServer trackerServer = null;
        StorageServer storageServer = null;

        try {
            trackerServer = ClientGlobal.getTrackerGroup().getTrackerServer();

            TrackerClient tracker = new TrackerClient(trackerServer);

            if (file != null) {
                parts = new String[2];
                if (0 != StorageClient1.split_file_id(file, parts)) {
                    return null;
                }
            }

            try {
                storageServer = tracker.getFetchStorage(parts[0], parts[1]);
            } catch (IOException e) {
                if (trackerServer instanceof PooledTrackerServer) {
                    PooledTrackerServer tmp = (PooledTrackerServer) trackerServer;

                    tmp.closePooledObject();
                    trackerServer = null;
                }
                throw e;
            }
            closeTrackerServer(trackerServer);
            trackerServer = null;

            StorageClient1 client = new StorageClient1(storageServer);

            try {
                T o = op.run(client);

                return o;
            } catch (IOException e) {
                if (storageServer instanceof PooledStorageServer) {
                    PooledStorageServer tmp = (PooledStorageServer) storageServer;

                    tmp.closePooledObject();
                    storageServer = null;
                }
                throw e;
            }
        } finally {
            closeTrackerServer(trackerServer);
            closeStorageServer(storageServer);
        }
    }

    public <T> T execFileOperation(String file, FastDFSCallback<T> op) throws MyException, IOException {
        String[] parts = new String[] { null, null };

        TrackerServer trackerServer = null;
        StorageServer storageServer = null;

        try {
            trackerServer = ClientGlobal.getTrackerGroup().getTrackerServer();

            TrackerClient tracker = new TrackerClient(trackerServer);

            if (file != null) {
                parts = new String[2];
                if (0 != StorageClient1.split_file_id(file, parts)) {
                    return null;
                }
            }

            try {
                storageServer = tracker.getUpdateStorage(parts[0], parts[1]);
            } catch (IOException e) {
                if (trackerServer instanceof PooledTrackerServer) {
                    PooledTrackerServer tmp = (PooledTrackerServer) trackerServer;

                    tmp.closePooledObject();
                    trackerServer = null;
                }
                throw e;
            }

            closeTrackerServer(trackerServer);
            trackerServer = null;

            StorageClient1 client = new StorageClient1(storageServer);

            try {
                T o = op.run(client);

                return o;
            } catch (IOException e) {
                if (storageServer instanceof PooledStorageServer) {
                    PooledStorageServer tmp = (PooledStorageServer) storageServer;

                    tmp.closePooledObject();
                    storageServer = null;
                }
                throw e;
            }
        } finally {
            closeTrackerServer(trackerServer);
            closeStorageServer(storageServer);
        }
    }

    private static void closeTrackerServer(TrackerServer trackerServer) {
        try {
            if (trackerServer != null) {
                trackerServer.close();
            }
        } catch (IOException e) {

        }
    }

    private static void closeStorageServer(StorageServer storageServer) {
        try {
            if (storageServer != null) {
                storageServer.close();
            }
        } catch (IOException e) {

        }
    }
}
