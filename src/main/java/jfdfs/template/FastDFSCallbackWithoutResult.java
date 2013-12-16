package jfdfs.template;

import java.io.IOException;

import jfdfs.common.MyException;
import jfdfs.core.StorageClient1;

public abstract class FastDFSCallbackWithoutResult implements FastDFSCallback<Object> {

    @Override
    public Object run(StorageClient1 client) throws IOException, MyException {
        this.runWithoutResult(client);
        return null;
    }

    public abstract void runWithoutResult(StorageClient1 client) throws IOException, MyException;

}
