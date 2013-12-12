package org.csource.fastdfs.test;

import org.csource.common.NameValuePair;
import org.csource.fastdfs.ClientGlobal;
import org.csource.fastdfs.StorageClient1;
import org.csource.fastdfs.StorageServer;
import org.csource.fastdfs.TrackerClient;
import org.csource.fastdfs.TrackerServer;
import org.csource.fastdfs.pool.PooledFdfsServerFactory;

public class PooledTest {

    public static void main(String args[]) {
        if (args.length < 2) {
            System.out.println("Error: Must have 2 parameters, one is config filename, "
                    + "the other is the local filename to upload");
            return;
        }

        System.out.println("java.version=" + System.getProperty("java.version"));

        String conf_filename = args[0];
        String local_filename = args[1];

        for (int c = 0; c < 10; c++) {
            try {
                ClientGlobal.init(conf_filename);
                ClientGlobal.setFactory(new PooledFdfsServerFactory(null));

                System.out.println("network_timeout=" + ClientGlobal.g_network_timeout + "ms");
                System.out.println("charset=" + ClientGlobal.g_charset);

                TrackerServer trackerServer = ClientGlobal.getTrackerGroup().getTrackerServer();
                TrackerClient tracker = new TrackerClient(trackerServer);

                StorageServer storageServer = tracker.getStoreStorage();
                StorageClient1 client = new StorageClient1(storageServer);

                NameValuePair[] metaList = new NameValuePair[1];
                metaList[0] = new NameValuePair("fileName", local_filename);
                String fileId = client.upload_file1(local_filename, null, metaList);
                System.out.println("upload success. file id is: " + fileId);

                int i = 0;
                while (i++ < 10) {
                    byte[] result = client.download_file1(fileId);
                    System.out.println(i + ", download result is: " + result.length);
                }

                trackerServer.close();
                storageServer.close();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }
}
