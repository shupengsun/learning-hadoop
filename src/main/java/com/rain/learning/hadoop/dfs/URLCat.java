package com.rain.learning.hadoop.dfs;

import java.io.InputStream;
import java.net.URL;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.zookeeper.common.IOUtils;

/**
 * Reading Data from a Hadoop URL
 * 
 * @version 1.0 
 * @date 2017年1月7日 下午2:57:28
 * @since 1.0
 */
public class URLCat {
	static {
		// 让Java程序识别Hadoop的HDFS URL
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
	}

	/**
	 * 命令如下：<br>
	 * hadoop jar xx.jar hdfs://192.168.162.111:8020/user/hadoop/ssp/input/a.txt
	 * 
	 * @param args 远程Hdfs地址
	 */
	public static void main(String[] args) {
		InputStream in = null;
		try{
			in = new URL(args[0]).openStream();
			IOUtils.copyBytes(in, System.out, 4096, false);
		}catch(Exception e){
		}finally{
			IOUtils.closeStream(in);
		}
	}

}
