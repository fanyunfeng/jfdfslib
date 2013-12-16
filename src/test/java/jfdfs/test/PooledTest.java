package jfdfs.test;

import jfdfs.common.NameValuePair;
import jfdfs.fastdfs.ClientGlobal;
import jfdfs.fastdfs.StorageClient1;
import jfdfs.fastdfs.StorageServer;
import jfdfs.fastdfs.TrackerClient;
import jfdfs.fastdfs.TrackerServer;
import jfdfs.fastdfs.pool.PooledFdfsServerFactory;


public class PooledTest {
    private static void testTrackerConnect(String config, int times) {
        long start = 0;

        try {
            start = System.currentTimeMillis();

            ClientGlobal.init(config);

            for (int c = 1; c <= times; c++) {
                TrackerServer trackerServer = ClientGlobal.getTrackerGroup().getTrackerServer();
                trackerServer.close();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        long end = System.currentTimeMillis();

        ClientGlobal.getFactory().close();

        System.out.println("consume of connect:" + (end - start));
    }

    private static void testWithPool(String config, String file, int times) {
        long start = 0;

        try {
            start = System.currentTimeMillis();

            ClientGlobal.init(config);
            ClientGlobal.setFactory(new PooledFdfsServerFactory(ClientGlobal.getConfig()));

            for (int c = 1; c <= times; c++) {
                TrackerServer trackerServer = ClientGlobal.getTrackerGroup().getTrackerServer();
                TrackerClient tracker = new TrackerClient(trackerServer);

                StorageServer storageServer = tracker.getStoreStorage();
                StorageClient1 client = new StorageClient1(storageServer);

                NameValuePair[] metaList = new NameValuePair[1];
                metaList[0] = new NameValuePair("fileName", file);
                String fileId = client.upload_file1(file, null, metaList);
                System.out.println("upload success. file id is: " + fileId);

                byte[] result = client.download_file1(fileId);
                System.out.println("test:" + c + " download result is: " + result.length);

                trackerServer.close();
                storageServer.close();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        long end = System.currentTimeMillis();

        ClientGlobal.getFactory().close();

        System.out.println("consume with pool:" + (end - start));
    }

    private static void testWithoutPool(String config, String file, int times) {
        long start = 0;

        try {
            start = System.currentTimeMillis();
            ClientGlobal.init(config);

            for (int c = 1; c <= times; c++) {
                TrackerServer trackerServer = ClientGlobal.getTrackerGroup().getTrackerServer();
                TrackerClient tracker = new TrackerClient(trackerServer);

                StorageServer storageServer = tracker.getStoreStorage();
                StorageClient1 client = new StorageClient1(storageServer);

                NameValuePair[] metaList = new NameValuePair[1];
                metaList[0] = new NameValuePair("fileName", file);
                String fileId = client.upload_file1(file, null, metaList);
                System.out.println("upload success. file id is: " + fileId);

                byte[] result = client.download_file1(fileId);
                System.out.println("test:" + c + " download result is: " + result.length);

                trackerServer.close();
                storageServer.close();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        long end = System.currentTimeMillis();

        System.out.println("consume without pool:" + (end - start));
    }

    public static void main(String args[]) {
        if (args.length < 2) {
            System.out.println("Error: Must have 2 parameters, one is config filename, "
                    + "the other is the local filename to upload");
            return;
        }

        System.out.println("java.version=" + System.getProperty("java.version"));

        String config = args[0];
        String file = args[1];

        testTrackerConnect(config, 100);
        testTrackerConnect(config, 200);
        testTrackerConnect(config, 500);
        testTrackerConnect(config, 1000);

        testWithoutPool(config, file, 100);

        testWithPool(config, file, 100);
    }
}
