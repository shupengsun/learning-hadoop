/*
 * Copyright(c) 2015-2017 Beijing Tech Co.,Ltd. All Rights Reserved.
 * 
 */
package com.rain.learning.hadoop.dfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.zookeeper.common.IOUtils;

/**
 * 读取HDFS Sequence文件.<br>
 * 
 * @version 1.0
 * @date 2017年1月7日 下午5:37:34
 * @since 1.0
 */
public class SequenceFileReader {

	/**
	 * hdfs://192.168.162.111:8020/user/hadoop/ssp/output/seq
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		String uri = args[0];
		SequenceFile.Reader reader = null;

		try {
			System.out.println(">>>操作开始！");
			Configuration conf = new Configuration();
			Path path = new Path(uri);

			Reader.Option optionPath = SequenceFile.Reader.file(path);
			reader = new SequenceFile.Reader(conf, optionPath);

			Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);

			long position = reader.getPosition();
			while (reader.next(key, value)) {
				System.out.printf("[%s]\t%s\t%s\n", position, key, value);
				position = reader.getPosition();
			}

			System.out.println(">>>操作成功！");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(reader);
		}

	}

}
