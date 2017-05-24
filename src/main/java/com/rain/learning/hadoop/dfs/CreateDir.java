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
 * 创建HDFS目录.<br>
 * 
 * 一般情况下不用显示地创建一个目录，因为在调用create()方法写一个文件时会自动地创建父目录.
 * 
 * @version 1.0
 * @date 2017年1月7日 下午3:37:35
 * @since 1.0
 */
public class CreateDir {

	/**
	 * 远程目录：hdfs://192.168.162.111:8020/user/hadoop/ssp/<br>
	 * 创建目录：test
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		String uri = args[0];
		String newDir = args[1];
		FileSystem fs = null;

		try {
			Configuration conf = new Configuration();
			fs = FileSystem.get(URI.create(uri), conf);

			Path dfs = new Path(uri + newDir);
			fs.mkdirs(dfs);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
