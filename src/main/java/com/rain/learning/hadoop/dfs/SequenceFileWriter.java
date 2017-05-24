/*
 * Copyright(c) 2015-2017 Beijing Tech Co.,Ltd. All Rights Reserved.
 * 
 */
package com.rain.learning.hadoop.dfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer.Option;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.common.IOUtils;

/**
 * HDFS写入Sequence文件.<br>
 * 
 * @version 1.0
 * @date 2017年1月7日 下午4:50:56
 * @since 1.0
 */
public class SequenceFileWriter {
	private static final String[] DATA = { "床前明月光", "疑是地上霜", "举头望明月", "低头思故乡" };

	/**
	 * hdfs://192.168.162.111:8020/user/hadoop/ssp/output/seq
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		String uri = args[0];
		SequenceFile.Writer writer = null;

		try {
			Configuration conf = new Configuration();
			Path path = new Path(uri);

			IntWritable key = new IntWritable();
			Text value = new Text();

			Option optionPath = SequenceFile.Writer.file(path);
			Option optionKey = SequenceFile.Writer.keyClass(key.getClass());
			Option optionValue = SequenceFile.Writer.valueClass(value.getClass());
			writer = SequenceFile.createWriter(conf, optionPath, optionKey, optionValue);

			for (int i = 0; i < 100; i++) {
				key.set(i + 1);
				value.set(DATA[i % DATA.length]);

				writer.append(key, value);
			}

			System.out.println(">>>操作成功！");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(writer);
		}

	}

}
