package jfdfs.template;

import java.io.IOException;

import jfdfs.common.MyException;
import jfdfs.core.ClientGlobal;
import jfdfs.core.StorageClient1;
import jfdfs.core.StorageServer;
import jfdfs.core.TrackerClient;
import jfdfs.core.TrackerServer;

public class FastDFSTemplate {
    public <T> T execFileUpload(String groupName, FastDFSCallback<T> op) throws MyException, IOException {
        TrackerServer trackerServer = null;
        StorageServer storageServer = null;

        try {
            trackerServer = ClientGlobal.getTrackerGroup().getTrackerServer();

            TrackerClient tracker = new TrackerClient(trackerServer);

            storageServer = tracker.getStoreStorage(groupName);

            trackerServer.close();

            StorageClient1 client = new StorageClient1(storageServer);

            T o = op.run(client);

            return o;
        } finally {
            closeTrackerServer(trackerServer);
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

            storageServer = tracker.getFetchStorage(parts[0], parts[1]);

            trackerServer.close();

            StorageClient1 client = new StorageClient1(storageServer);

            T o = op.run(client);

            return o;

        } finally {
            closeTrackerServer(trackerServer);
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

            storageServer = tracker.getUpdateStorage(parts[0], parts[1]);

            trackerServer.close();

            StorageClient1 client = new StorageClient1(storageServer);

            T o = op.run(client);

            return o;
        } finally {
            closeTrackerServer(trackerServer);
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
}
