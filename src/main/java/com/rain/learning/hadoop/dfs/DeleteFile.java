/*
 * Copyright(c) 2015-2017 Beijing Tech Co.,Ltd. All Rights Reserved.
 * 
 */
package com.rain.learning.hadoop.dfs;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 删除文件或目录.
 * 
 * @version 1.0
 * @date 2017年1月7日 下午3:58:53
 * @since 1.0
 */
public class DeleteFile {

	/**
	 * 删除目录文件： hdfs://192.168.162.111:8020/user/hadoop/ssp/test
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		String uri = args[0];
		FileSystem fs = null;

		try {
			Configuration conf = new Configuration();
			fs = FileSystem.get(URI.create(uri), conf);
			Path delDir = new Path(uri);

			// 参数为true时表示递归删除文件夹及文件夹下的文件
			boolean isDeleted = fs.delete(delDir, false);
			System.out.println(">>>删除结果：" + isDeleted);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
