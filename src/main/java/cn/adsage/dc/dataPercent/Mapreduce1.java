package cn.adsage.dc.dataPercent;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
 * 同一app下不同属性的点击量
 */
public class Mapreduce1 extends Configured implements Tool {
	private static final String INPUT_PATH = "hdfs://172.16.3.151:9000/kinyi/data";
	private static final String OUTPUT_PATH = "hdfs://172.16.3.151:9000/kinyi/step1";

	public int run(String[] args) throws Exception {
		if (args.length != 1) {
			System.err.println("Usage: hadoop jar xxx.jar <index of the count field>");
			System.exit(-1);
		}

		// 接收参数，记录需要统计字段的下标
		String field = args[0];
		Configuration conf = new Configuration();
		// 利用configuration来给map和reduce函数进行传参
		conf.set("field", field);
		try {
			FileSystem fileSystem = FileSystem.get(conf);
			if (fileSystem.exists(new Path(OUTPUT_PATH))) {
				fileSystem.delete(new Path(OUTPUT_PATH), true);
			}
			Job job = Job.getInstance(conf, Mapreduce1.class.getSimpleName());
			job.setJarByClass(Mapreduce1.class);
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
		return 0;
	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new Mapreduce1(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
		Text text = new Text();

		@Override
		protected void map(LongWritable k1, Text v1,
				Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			// 利用context来获取configuration对象
			Configuration conf = context.getConfiguration();
			// 利用configuration对象来获取参数
			String field = conf.get("field");
			// 把获取到的字符串参数转化为整型
			int fieldIndex = Integer.parseInt(field);
			String[] split = v1.toString().replaceAll("\\s+", ";").split(";");
			String k2 = split[0] + "\t" + split[fieldIndex];
			text.set(k2);
			context.write(text, new LongWritable(1L));
		}
	}

	public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		@Override
		protected void reduce(Text k2, Iterable<LongWritable> v2s,
				Reducer<Text, LongWritable, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			long sum = 0L;
			for (LongWritable v2 : v2s) {
				sum += v2.get();
			}
			context.write(k2, new LongWritable(sum));
		}
	}
}