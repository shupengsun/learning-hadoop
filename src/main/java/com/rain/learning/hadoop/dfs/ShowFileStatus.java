/*
 * Copyright(c) 2015-2017 Beijing Tech Co.,Ltd. All Rights Reserved.
 * 
 */
package com.rain.learning.hadoop.dfs;

import java.net.URI;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.common.IOUtils;

/**
 * 查看HDFS文件状态信息.
 * 
 * @version 1.0
 * @date 2017年1月7日 下午4:17:19
 * @since 1.0
 */
public class ShowFileStatus {

	/**
	 * hdfs://192.168.162.111:8020/user/hadoop/ssp/input/a.txt
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		String uri = args[0];
		Configuration conf = new Configuration();
		FileSystem fs = null;
		try {
			fs = FileSystem.get(URI.create(uri), conf);

			Path path = new Path(uri);
			boolean isExists = fs.exists(path);
			System.out.println(">>>目录是否存在：" + isExists);

			FileStatus status[] = fs.listStatus(path);
			System.out.println(">>>打印当前目录信息：");
			for (int i = 0; i < status.length; i++) {
				System.out.println(status[i].getPath().toString());
			}

			FileStatus fileStatus = fs.getFileStatus(path);
			BlockLocation[] locations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());

			String[] hosts = null;
			System.out.println(">>>打印文件存储信息：");
			System.out.println("Group:" + fileStatus.getGroup());
			System.out.println("Owner:" + fileStatus.getOwner());
			System.out.println("BlockSize:" + fileStatus.getBlockSize());
			System.out.println("AccessTime:" + fileStatus.getAccessTime());
			for (int i = 0; i < locations.length; i++) {
				hosts = locations[i].getHosts();
				System.out.println(Arrays.toString(hosts));

			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(fs);
		}

	}

}
