package cn.adsage.dc.dataPercent;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * 可与Mapreduce1和Mapreduce4一起使用
 */
public class Mapreduce3 {

	// 数据输入输出路径
	private static final String INPUT_PATH = "hdfs://172.16.3.151:9000/kinyi/data";
	private static final String OUTPUT_PATH = "hdfs://172.16.3.151:9000/kinyi/totalClick";

	public static void main(String[] args) {
		Configuration conf = new Configuration();
		try {
			FileSystem fileSystem = FileSystem.get(conf);
			if (fileSystem.exists(new Path(OUTPUT_PATH))) {
				fileSystem.delete(new Path(OUTPUT_PATH), true);
			}
			Job job = Job.getInstance(conf, Mapreduce3.class.getSimpleName());
			job.setJarByClass(Mapreduce3.class);
			job.setMapperClass(MyMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(LongWritable.class);
			job.setReducerClass(MyReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(LongWritable.class);
			FileInputFormat.setInputPaths(job, INPUT_PATH);
			FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
			job.waitForCompletion(true);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static class MyMapper extends
			Mapper<LongWritable, Text, Text, LongWritable> {
		Text text = new Text();

		@Override
		protected void map(LongWritable k1, Text v1,
				Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			String[] split = v1.toString().replaceAll("\\s+", ";").split(";");
			String k2 = split[0];
			text.set(k2);
			context.write(text, new LongWritable(1));
		}
	}

	public static class MyReducer extends
			Reducer<Text, LongWritable, Text, LongWritable> {

		@Override
		protected void reduce(Text k2, Iterable<LongWritable> v2s,
				Reducer<Text, LongWritable, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			long sum = 0;
			for (LongWritable v2 : v2s) {
				sum += v2.get();
			}
			context.write(k2, new LongWritable(sum));
		}
	}
}