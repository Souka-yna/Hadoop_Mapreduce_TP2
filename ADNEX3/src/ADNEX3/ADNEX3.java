package ADNEX3;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.util.*;

public class ADNEX3
{
	public static class MutationMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private IntWritable position = new IntWritable();
		private Text word = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString().trim();
			
			List<Integer> indices = null;
			
			if (lookforSequenceOfChar(line, 'A', 3).size() != 0) {
	            indices = lookforSequenceOfChar(line, 'A', 3);
	            for(Integer i : indices)
	            {
	            	word.set("AAA");
	    			position.set(i);
	    			context.write(word, position);
	            }
			}
			if (lookforSequenceOfChar(line, 'T', 3).size() != 0) {
	            indices = lookforSequenceOfChar(line, 'T', 3);
	            for(Integer i : indices)
	            {
	            	word.set("TTT");
	    			position.set(i);
	    			context.write(word, position);
	            }
			}
			if (lookforSequenceOfChar(line, 'C', 4).size() != 0) {
	            indices = lookforSequenceOfChar(line, 'C', 4);
	            for(Integer i : indices)
	            {
	            	word.set("CCCC");
	    			position.set(i);
	    			context.write(word, position);
	            }
			}
			if (lookforSequenceOfChar(line, 'G', 4).size() != 0) {
	            indices = lookforSequenceOfChar(line, 'G', 4);
	            for(Integer i : indices)
	            {
	            	word.set("GGGG");
	    			position.set(i);
	    			context.write(word, position);
	            }
			}
		}
		
		private List<Integer> lookforSequenceOfChar(String line , char c , int size)
		{
			List<Integer> indices = new ArrayList<Integer>();
			char [] seq = new char[size];
			Arrays.fill(seq, c);
			String sequence = new String(seq);
			int index = 0;
			String lineTemp = new String(line); // to insure the immutability
			while(lineTemp.contains(sequence))
			{
				index = lineTemp.indexOf(sequence);
				char [] lineChars = lineTemp.toString().toCharArray();
				for(int i = index ; i<index+size;i++)
				{
					lineChars[i] = '_';
					
				}
				lineTemp = new String(lineChars);
				indices.add(index+1);
			}
			return indices;
		}

	}
	
	public static class MutationReducer extends Reducer<Text, IntWritable, Text, Text>
	{
		private Text pos = new Text();
		private Text output = new Text();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			int count = 0;
			StringBuilder positions = new StringBuilder();
			for (IntWritable val : values)
			{

				positions.append(val.get());
				positions.append(",");
				count++;
			}
			if (positions.length() > 1)
				positions.setLength(positions.length() - 1);
			output.set(key.toString() + "\t" + count);
			pos.set(positions.toString());
			context.write(output, pos);
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"ADN Mutation sequence count");
		job.setJarByClass(ADNEX3.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setMapperClass(MutationMapper.class);
		job.setReducerClass(MutationReducer.class);
		//job.setCombinerClass(MutationReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}
}
