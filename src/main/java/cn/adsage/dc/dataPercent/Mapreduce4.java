package cn.adsage.dc.dataPercent;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

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
 * 可与Mapreduce1和Mapreduce3一起使用
 */
public class Mapreduce4 {

	// 数据输入输出路径
	private static final String INPUT_PATH = "hdfs://172.16.3.151:9000/kinyi/step1";
	private static final String INPUT_PATH2 = "hdfs://172.16.3.151:9000/kinyi/totalClick";
	private static final String OUTPUT_PATH = "hdfs://172.16.3.151:9000/kinyi/result";

	public static void main(String[] args) {
		Configuration conf = new Configuration();
		try {
			FileSystem fileSystem = FileSystem.get(conf);
			if (fileSystem.exists(new Path(OUTPUT_PATH))) {
				fileSystem.delete(new Path(OUTPUT_PATH), true);
			}
			Job job = Job.getInstance(conf, Mapreduce4.class.getSimpleName());
			job.setJarByClass(Mapreduce4.class);
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

		@Override
		protected void map(LongWritable k1, Text v1,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] split = v1.toString().replaceAll("\\s+", ";").split(";");
			String key = split[0];
			String value = split[1] + ";" + split[2];
			context.write(new Text(key), new Text(value));
		}
	}

	public static class MyMapper2 extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable k1, Text v1,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] split = v1.toString().replaceAll("\\s+", ";").split(";");
			String key = split[0];
			String value = split[1];
			context.write(new Text(key), new Text(value));
		}
	}

	public static class MyReducer extends Reducer<Text, Text, Text, NullWritable> {

		@Override
		protected void reduce(Text k2, Iterable<Text> v2s,
				Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			//ArrayList<String> arrayList = new ArrayList<String>();
			HashMap<String, String> map = new HashMap<String, String>();
			HashMap<String, Double> map2 = new HashMap<String, Double>();
			double appClick =0;
			for (Text text : v2s) {
				String v2 = text.toString();
				if (v2.contains(";")) {
					String[] split = v2.split(";");
					String attrName = split[0];
					String clickTimes = split[1];
					double dclickTimes = Double.parseDouble(clickTimes);
					map2.put(attrName, dclickTimes);
				}else {
					appClick = Double.parseDouble(v2);
				}
			}
			for (Entry<String, Double> entry : map2.entrySet()) {
				double rate = entry.getValue() / appClick;
				map.put(entry.getKey(), rate+"");
			}
			String result = JsonUtils.generateJson(k2.toString(), map);
			context.write(new Text(result), NullWritable.get());
			
			/*String sum = "";
			for (Text text : v2s) {
//				sum += text.toString()+"|";
				String[] split = text.toString().split(";");
				if (split.length > 0) {
					
				}
			}
			context.write(new Text(k2.toString()+"|"+sum), NullWritable.get());*/
		}
	}
}