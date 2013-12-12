jfdfslib
========

A fastDFS java library.

example:
========


//init
ClientGlobal.init(confFileName);

//replace the default ServerFactory
ClientGlobal.setFactory(new PooledFdfsServerFactory(null));

//get tracker server from tracker group
TrackerServer trackerServer = ClientGlobal.getTrackerGroup().getTrackerServer();

//construct TrackerClient
TrackerClient tracker = new TrackerClient(trackerServer);


//get storage server from tracker server
StorageServer storageServer = tracker.getStoreStorage();

//
StorageClient1 client = new StorageClient1(storageServer);
