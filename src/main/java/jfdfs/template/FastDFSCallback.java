package jfdfs.template;

import java.io.IOException;

import jfdfs.common.MyException;
import jfdfs.core.StorageClient1;


public interface FastDFSCallback<T> {
    public T run(StorageClient1 client) throws IOException, MyException;
}