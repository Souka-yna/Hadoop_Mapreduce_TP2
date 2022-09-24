package ADNEX2;
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

public class ADNEX2
{
	public static class TripletMapper extends Mapper<LongWritable, Text, Text, Pair>
	{
		private IntWritable position = new IntWritable();
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String line = value.toString().trim();
			int i = 0;
			for (i = 0; i < line.length() - 3; i++) {
				String triplet = line.substring(i, i + 3);
				String newLine = null;
				if (i > 0)
				{
					newLine = line.substring(i + 1, line.length());
				} else
					newLine = line.substring(i + 1, line.length());
				int j = 0;
				for (j = 0; j < newLine.length() - 3; j++)
				{
					if (line.substring(j, j + 2).equals(triplet))
					{

						position.set(i + 1);
						word.set(triplet);
						Pair pair = new Pair();
						pair.setCount(one);
						pair.setPosition(position);
						context.write(word, pair);
					}

				}
				position.set(j + 1);
				word.set(line.substring(j, newLine.length()));
				Pair pair = new Pair();
				pair.setCount(one);
				pair.setPosition(position);
				context.write(word, pair);

			}
			position.set(i + 1);
			word.set(line.substring(i, line.length()));
			Pair pair = new Pair();
			pair.setCount(one);
			pair.setPosition(position);
			context.write(word, pair);
		}
	}

	public static class TripletReducer extends Reducer<Text, Pair, Text, IntWritable>
	{

		private IntWritable count = new IntWritable();
		private Text output = new Text();

		public void reduce(Text key, Iterable<Pair> pairs, Context context) throws IOException, InterruptedException
		{
			int sum = 0;
			StringBuilder positions = new StringBuilder();
			
				for (Pair val : pairs)
				{
					sum = sum + val.getCount().get();
					positions.append(val.getPosition().get());
					positions.append(",");

				}
				if (positions.length() > 1)
					positions.setLength(positions.length() - 1);
				output.set(key.toString() + "\t" + positions.toString());
				count.set(sum);
				context.write(output, count);
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"ADN triplet sequence count");
		job.setJarByClass(ADNEX2.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		 
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Pair.class);
		
		job.setMapperClass(TripletMapper.class);
		job.setReducerClass(TripletReducer.class);
		 //job.setCombinerClass(TripletReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}
}
