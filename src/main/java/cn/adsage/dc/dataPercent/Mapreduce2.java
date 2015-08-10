package cn.adsage.dc.dataPercent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * 结合Mapreduce1，把data和step1两个文件连接起来
 */
public class Mapreduce2 {

	// 数据输入输出路径
	private static final String INPUT_PATH = "hdfs://172.16.3.151:9000/kinyi/data";
	private static final String INPUT_PATH2 = "hdfs://172.16.3.151:9000/kinyi/step1";
	private static final String OUTPUT_PATH = "hdfs://172.16.3.151:9000/kinyi/result";

	public static void main(String[] args) {
		Configuration conf = new Configuration();
		try {
			FileSystem fileSystem = FileSystem.get(conf);
			if (fileSystem.exists(new Path(OUTPUT_PATH))) {
				fileSystem.delete(new Path(OUTPUT_PATH), true);
			}
			Job job = Job.getInstance(conf, Mapreduce2.class.getSimpleName());
			job.setJarByClass(Mapreduce2.class);
			MultipleInputs.addInputPath(job, new Path(INPUT_PATH),TextInputFormat.class, MyMapper.class);
			MultipleInputs.addInputPath(job, new Path(INPUT_PATH2),TextInputFormat.class, MyMapper2.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setReducerClass(MyReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
			FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
			job.waitForCompletion(true);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		Text text = new Text();

		@Override
		protected void map(LongWritable k1, Text v1,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] split = v1.toString().replaceAll("\\s+", ";").split(";");
			String k2 = split[0];
			text.set(k2);
			context.write(text, new Text("1"));
		}
	}

	public static class MyMapper2 extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable k1, Text v1,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] split = v1.toString().split("\t");
			String k2 = split[0];
			String v2 = split[1] + "\t" + split[2];
			context.write(new Text(k2), new Text(v2));
		}
	}

	public static class MyReducer extends Reducer<Text, Text, Text, NullWritable> {

		@Override
		protected void reduce(Text k2, Iterable<Text> v2s,
				Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			ArrayList<String> arrayList = new ArrayList<String>();
			HashMap<String, String> map = new HashMap<String, String>();
			double sum = 0;
			for (Text text : v2s) {
				String v2 = text.toString();
				if (Mapreduce2.isNumeric(v2)) {
					sum += Double.parseDouble(v2);
				}else {
					arrayList.add(v2);
				}
			}
			for (String component : arrayList) {
				String[] split = component.split("\t");
				String attrName = split[0];
				double clickTimes = Double.parseDouble(split[1]);
				double rate = clickTimes / sum;
				map.put(attrName, rate + "");
			}
			String appid = k2.toString();
			String k3 = JsonUtils.generateJson(appid, map);
			context.write(new Text(k3), NullWritable.get());
		}
	}

	public static boolean isNumeric(String str) {
		Pattern pattern = Pattern.compile("[0-9]*");
		Matcher isNum = pattern.matcher(str);
		if (!isNum.matches()) {
			return false;
		}
		return true;
	}
}