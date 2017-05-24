/*
 * Copyright(c) 2015-2017 Beijing Tech Co.,Ltd. All Rights Reserved.
 * 
 */
package com.rain.learning.hadoop.dfs;

import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.common.IOUtils;

/**
 * Reading Data Using the FileSystem API.<br>
 * Displaying files from a Hadoop filesystem on standard output by using<br>
 * the FileSystem directly
 * 
 * @version 1.0
 * @date 2017年1月7日 下午3:01:25
 * @since 1.0
 */
public class FileSystemCat {

	/**
	 * hdfs://192.168.162.111:8020/user/hadoop/ssp/input/a.txt
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		String uri = args[0];
		Configuration conf = new Configuration();
		FileSystem fs = null;
		InputStream in = null;
		try {
			fs = FileSystem.get(URI.create(uri), conf);
			in = fs.open(new Path(uri));
			IOUtils.copyBytes(in, System.out, 4096, false);
		} catch (Exception e) {
		} finally {
			IOUtils.closeStream(in);
		}

	}

}
