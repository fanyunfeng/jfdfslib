package jfdfs.test;

import java.net.InetSocketAddress;

import jfdfs.common.NameValuePair;
import jfdfs.core.ClientGlobal;
import jfdfs.core.StorageClient1;
import jfdfs.core.StorageServer;
import jfdfs.core.TrackerClient;
import jfdfs.core.TrackerGroup;
import jfdfs.core.TrackerServer;


public class Test1 {
    public static void main(String args[]) {
        try {
            ClientGlobal.init("fdfs_client.conf");
            System.out.println("network_timeout=" + ClientGlobal.g_network_timeout + "ms");
            System.out.println("charset=" + ClientGlobal.g_charset);

            TrackerGroup tg = new TrackerGroup(
                    new InetSocketAddress[] { new InetSocketAddress("192.168.0.196", 22122) });

            TrackerServer ts = tg.getTrackerServer();

            if (ts == null) {
                System.out.println("getConnection return null");
                return;
            }

            TrackerClient tc = new TrackerClient(ts);
            StorageServer ss = tc.getStoreStorage();
            if (ss == null) {
                System.out.println("getStoreStorage return null");
            }

            StorageClient1 sc1 = new StorageClient1(ss);

            NameValuePair[] meta_list = null; //new NameValuePair[0];
            String item = "c:/windows/system32/notepad.exe";
            String fileid = sc1.upload_file1(item, "exe", meta_list);

            System.out.println("Upload local file " + item + " ok, fileid=" + fileid);
            
            ts.close();
            ss.close();        
            } catch (Exception ex) {
            ex.printStackTrace();
        }

    }
}
