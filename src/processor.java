import java.io.*;
import java.util.*;
import java.util.StringTokenizer;
import java.util.HashMap;
import org.apache.hadoop.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

public class processor {

  public static void main(String[] args)
    throws IOException, InterruptedException, ClassNotFoundException {
    Job job = new Job();
    job.setJobName("processor");
		job.setJarByClass(processor.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(processMapper.class);
		job.setReducerClass(processReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);

  }

  static class processMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text word = new Text();
    
    public processMapper() {}

    public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

      String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

      String line = value.toString();
      String words[] = line.split(" ");
      StringTokenizer tok = new StringTokenizer(line);

      while(tok.hasMoreTokens()) {
        // remove any character thats not a letter from a-z & make everything lowercase
        word.set(tok.nextToken().replaceAll("[^a-zA-Z]", "").toLowerCase());
      }
    }

  }

  static class processReducer extends Reducer<Text, Text, Text, Text> {
	 
	public processReducer() {}
	
    public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

      HashMap map = new HashMap();

      int i = 0;
      for(Text text:values) {
        String s = text.toString();

        // iterate through all values
        if(map != null && map.get(s) != null) {
          i = (int)map.get(s);
          map.put(s,++i);
        } else {
          map.put(s,1);
        }
      }
      // display result as key + sum of values
      context.write(key, new Text(map.toString()));
    }
  }


}
