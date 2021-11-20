package com.changgou.util;

import com.changgou.file.FastDFSFile;
import org.csource.common.NameValuePair;
import org.csource.fastdfs.*;
import org.springframework.core.io.ClassPathResource;

import java.io.*;

/**
 * @author 梁耀其
 */
public class FastDFSUtil {

    static {

        try {
            //获取calsspath下的fdfs_client.conf的文件路径
            String filename = new ClassPathResource("fdfs_client.conf").getPath();
            /**
             * 加载track链接信息
             */
            ClientGlobal.init(filename);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取tracker
     * @return
     * @throws Exception
     */
    public static TrackerServer getTrackerServer() throws Exception {
        //创建TrackerClient客户端对象
        TrackerClient trackerClient = new TrackerClient();
        //通过TrackerClient对象获取TrackerServer信息
        TrackerServer trackerServer = trackerClient.getConnection();
        return trackerServer;
    }

    /**
     * 获取storageClient
     * @param trackerServer
     * @return
     */
    public static StorageClient getStorageClient(TrackerServer trackerServer){
        //获取StorageClient对象
        StorageClient storageClient = new StorageClient(trackerServer, null);
        return storageClient;
    }

    public static String[] upload(FastDFSFile fastDFSFile) throws  Exception{
        //创建附加信息
        NameValuePair[] meta_list = new NameValuePair[1];
        meta_list[0]= new NameValuePair("author",fastDFSFile.getAuthor());


        TrackerServer trackerServer = getTrackerServer();

        StorageClient storageClient = getStorageClient(trackerServer);
        //执行文件上传
        /**
         * uploads[]
         *      uploads[0]:文件上传到storage存储的组名 group1
         *      uploads[1]:文件上传到storage的文件名 M00/02/39/1.jpg
         */
        String[] uploads = storageClient.upload_file(fastDFSFile.getContent(), fastDFSFile.getExt(), meta_list);
        return uploads;
    }

    /**
     * 获取文件信息
     * @param groupName
     * @param remoteFileName
     */
    public static FileInfo getFile(String groupName,String remoteFileName) throws Exception {
        TrackerServer trackerServer = getTrackerServer();
        StorageClient storageClient = getStorageClient(trackerServer);
        //获取文件信息
        return storageClient.get_file_info(groupName,remoteFileName);
    }

    /**
     * 文件下载
     * @param groupName
     * @param remoteFileName
     */
    public static InputStream downloadFile(String groupName, String remoteFileName) throws Exception {
        TrackerServer trackerServer = getTrackerServer();
        StorageClient storageClient = getStorageClient(trackerServer);
        byte[] buffer = storageClient.download_file(groupName, remoteFileName);
            return new ByteArrayInputStream(buffer);
    }

    /**
     * 下载
     * @param groupName
     * @param remoteFileName
     */
    public static void deletedFile(String groupName, String remoteFileName) throws Exception {
        TrackerServer trackerServer = getTrackerServer();
        StorageClient storageClient = getStorageClient(trackerServer);

        storageClient.delete_file(groupName,remoteFileName);
    }

    /**
     * 获取storage的信息
     * @return
     * @throws Exception
     */
    public static StorageServer getStorages() throws Exception {
        //创建TrackerClient客户端对象
        TrackerClient trackerClient = new TrackerClient();
        //通过TrackerClient对象获取TrackerServer信息
        TrackerServer trackerServer = trackerClient.getConnection();

        StorageServer storageServer = trackerClient.getStoreStorage(trackerServer, "group1");
        return storageServer;
    }

    /**
     * 获取storage端口和ip
     * @param groupName
     * @param remoteFileName
     * @return
     * @throws Exception
     */
    public static ServerInfo[] getServerInfos(String groupName, String remoteFileName) throws Exception {
        //创建TrackerClient客户端对象
        TrackerClient trackerClient = new TrackerClient();
        //通过TrackerClient对象获取TrackerServer信息
        TrackerServer trackerServer = trackerClient.getConnection();

        return trackerClient.getFetchStorages(trackerServer, groupName, remoteFileName);
    }

    public static String getTrackerInfo() throws Exception {
        //创建TrackerClient客户端对象
        TrackerClient trackerClient = new TrackerClient();
        //通过TrackerClient对象获取TrackerServer信息
        TrackerServer trackerServer = trackerClient.getConnection();

        //获取trackerIP和端口
        String ip = trackerServer.getInetSocketAddress().getHostString();
        int tracker_http_port = ClientGlobal.getG_tracker_http_port();
        String url = "http://"+ip+":"+tracker_http_port;
        return url;
    }


    public static void main(String[] args) throws Exception {
        /*FileInfo fileInfo = getFile("group1", "M00/00/00/wKh-gWGFMa6AVkZ8AADlcXlWxQk498.jpg");
        System.out.println(fileInfo.getSourceIpAddr());
        System.out.println(fileInfo.getFileSize());

        *//**
         * 文件下载
         *//*
        InputStream is = downloadFile("group1", "M00/00/00/wKh-gWGFMa6AVkZ8AADlcXlWxQk498.jpg");
        FileOutputStream os = new FileOutputStream("D://2.jpg");


        byte[] buffers = new byte[1024];
        while (is.read(buffers)!=-1){
            os.write(buffers);
        }
        os.flush();
        is.close();
        os.close();

        //删除文件
        deletedFile("group1","M00/00/00/wKh-gWGGCsiABVZjAAEFoldOtnE338.jpg");

        //获取文件信息
        StorageServer storages = getStorages();
        System.out.println(storages.getStorePathIndex());
        System.out.println(storages.getInetSocketAddress().getHostName());

        //获取storage端口和ip
        ServerInfo[] group1s = getServerInfos("group1", "M00/00/00/wKh-gWGFMa6AVkZ8AADlcXlWxQk498.jpg");
        for (ServerInfo group:group1s) {
            System.out.println(group.getIpAddr());
            System.out.println(group.getPort());
        }*/

        //获取tracker端口和ip
        System.out.println(getTrackerInfo());
    }
}
