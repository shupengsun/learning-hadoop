package com.rain.learning.hadoop.dfs;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class FileSystemUploading {

	public static void main(String[] args) {
		// 配置环境不重启电脑时可以这样用
		System.setProperty("hadoop.home.dir", "D:\\devenv\\hadoop-2.5.2");
		String localFile = "input/a.txt";
		String dst = "hdfs://192.168.162.111:8020/user/hadoop/ssp/input/a.txt";
		InputStream in =null;
		OutputStream out =null;
		FileSystem fs =null;
		Configuration conf = new Configuration();
		try {
			// 本地文件系统目录文件
			in = new BufferedInputStream(new FileInputStream(localFile));
			fs = FileSystem.get(URI.create(dst),conf);
			
			out = fs.create(new Path(dst));
			IOUtils.copyBytes(in, out,4096,true);
			
			System.out.println("Suceess");
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			IOUtils.closeStream(out);
			IOUtils.closeStream(in);
		}

	}

}
