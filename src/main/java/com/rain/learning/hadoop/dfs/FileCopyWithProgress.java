/*
 * Copyright(c) 2015-2017 Beijing Tech Co.,Ltd. All Rights Reserved.
 * 
 */
package com.rain.learning.hadoop.dfs;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;
import org.apache.zookeeper.common.IOUtils;

/**
 * Copying a local file to a Hadoop filesystem.<br>
 * 
 * @version 1.0
 * @date 2017年1月7日 下午3:15:32
 * @since 1.0
 */
public class FileCopyWithProgress {

	/**
	 * 本地文件：input/a.txt
	 * 远程目录：hdfs://192.168.162.111:8020/user/hadoop/ssp/input/b.txt
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		String localSrc = args[0];
		String dst = args[1];
		InputStream in = null;
		OutputStream out = null;
		FileSystem fs = null;

		try {
			Configuration conf = new Configuration();
			in = new BufferedInputStream(new FileInputStream(localSrc));
			fs = FileSystem.get(URI.create(dst), conf);
			out = fs.create(new Path(dst), new Progressable() {
				@Override
				public void progress() {
					System.out.print(".");
				}
			});

			IOUtils.copyBytes(in, out, 4096, true);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
