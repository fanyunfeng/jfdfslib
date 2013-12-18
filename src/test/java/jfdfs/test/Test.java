/**
 * Copyright (C) 2008 Happy Fish / YuQing
 *
 * FastDFS Java Client may be copied only under the terms of the GNU Lesser
 * General Public License (LGPL).
 * Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
 **/

package jfdfs.test;

import jfdfs.common.NameValuePair;
import jfdfs.core.ClientGlobal;
import jfdfs.core.StorageClient1;
import jfdfs.core.StorageServer;
import jfdfs.core.TrackerClient;
import jfdfs.core.TrackerServer;


/**
 * client test
 * 
 * @author Happy Fish / YuQing
 * @version Version 1.18
 */
public class Test {
    private Test() {
    }

    /**
     * entry point
     * 
     * @param args comand arguments <ul><li>args[0]: config filename</li></ul> <ul><li>args[1]: local filename to
     *            upload</li></ul>
     */
    public static void main(String args[]) {
        if (args.length < 2) {
            System.out.println("Error: Must have 2 parameters, one is config filename, "
                    + "the other is the local filename to upload");
            return;
        }

        System.out.println("java.version=" + System.getProperty("java.version"));

        String conf_filename = args[0];
        String local_filename = args[1];

        try {
            ClientGlobal.init(conf_filename);
            
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