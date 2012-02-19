package preData;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import lib.Point;
import lib.ToolJob;
import lib.VectorWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.log4j.Logger;

public class CreatePoint extends ToolJob {

	private static Logger logger = Logger.getLogger(CreatePoint.class);

	public static class MyMapper extends Mapper<Text, Text, Text, Text> {
		@Override
		public void map(Text nid, Text line, Context context) {
			String[] tmp1 = line.toString().split(",");
			if (tmp1.length != 2) {
				return;
			}
			String uid = tmp1[0];
			try {
				context.write(new Text(uid), nid);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public static class MyReducer extends Reducer<Text, Text, Text, Point> {
		private Map<String,Integer> nidMap = new HashMap<String,Integer>();

		@Override
		public void reduce(Text uid, Iterable<Text> nidList, Context context) {
			Map<Integer,Double> data = new HashMap<Integer,Double>();
			
			for (Text nid : nidList) {
				if (!nidMap.containsKey(nid.toString())) {
					continue;
				}
				Integer position = nidMap.get(nid.toString());
				if (data.containsKey(position)) {
					Double clicktimes = data.get(position) + 1;
					data.put(position, clicktimes);
				}
				else {
					data.put(position, 1.0);
				}
			}
			
			VectorWritable vector = new VectorWritable(nidMap.size());
			vector.setData(data);
			
			Point point = new Point();
			point.setData(vector);
			point.setId(Integer.parseInt(uid.toString()));
			try {
				context.write(uid, point);
			} catch (IOException e) {

				e.printStackTrace();
			} catch (InterruptedException e) {

				e.printStackTrace();
			}
		}

		@Override
		protected void setup(Context context) {
			Configuration conf = context.getConfiguration();
			String nidFile = conf.get("nidFile");

			FileReader fr;
			Set<String> nidSet = new HashSet<String>();
			
			try {
				fr = new FileReader(nidFile);
				BufferedReader br = new BufferedReader(fr);
				String line = "";
				while ((line = br.readLine()) != null) {
					String tmp1[] = line.split("\t");
					if (tmp1.length == 0)
						continue;
					String nid = tmp1[0];
					nidSet.add(nid);
					
				}
				br.close();
				fr.close();
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}// 创建FileReader对象，用来读取字符流
			catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			int i=0;
			for (String nid : nidSet) {
				nidMap.put(nid, i);
				i++;
			}

		}
	}	

	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 * 
	 * 输入格式为SequenceFile格式的click日志nid->uid,time
	 */
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.out.println("wrong number of args");
			return 0;
		}
		String input = args[0];
		String output = args[1];
		String nidFile = args[2];

		Configuration conf = getConf();
		conf.set("nidFile", nidFile);
		Job job = new Job(conf, "createVector");
		job.setJarByClass(CreatePoint.class);
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(VectorWritable.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		if (job.waitForCompletion(true))
			return 1;
		else
			return 0;
	}

}
