import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class processor {

  public static void main(String[] args)
    throws IOException, InterruptedException, ClassNotFoundException {
	  	Job job = new Job();
	  	job.setJarByClass(processor.class);
	  	job.setJobName("processor");

		job.setMapperClass(processMapper.class);
		job.setReducerClass(processReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

  }

  static class processMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text word = new Text();
    
    public processMapper() {}

    public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

      String file = ((FileSplit) context.getInputSplit()).getPath().getName();

      String line = value.toString();
      StringTokenizer tok = new StringTokenizer(line);

      while(tok.hasMoreTokens()) {
        // remove any character thats not a letter from a-z & make everything lowercase
        word.set(tok.nextToken().replaceAll("[^a-zA-Z]", "").toLowerCase());
        context.write(word, new Text(file));
      }
    }
    
    

  }

  static class processReducer extends Reducer<Text, Text, Text, Text> {
	
	private HashMap<String,Integer> map = new HashMap<String, Integer>();
	
	public processReducer() {}
	
    public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

     
      int i = 0;
      String str = key.toString();
      for(Text text:values) {
        String s = str + "\t(" + text.toString() + ")\t";

        // iterate through all values
        if(map != null && map.get(s) != null) {
          if(str.length() <= 3 || str.equals("there") || str.equals("their") || str.equals("here") ) {
        	  break;
          }
          i = (int)map.get(s);
          map.put(s, ++i);
        } else {
          map.put(s, 1);
        }
      }

    }
    
    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
    	LinkedHashMap<String, Integer> sorted = new LinkedHashMap<>();
    	
    	map.entrySet()
    		.stream()
    		.sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
    		.forEachOrdered(x -> sorted.put(x.getKey(), x.getValue()));
    			
		String temp = "";
        String temp2 = "";
		for(Map.Entry<String, Integer> e : sorted.entrySet()) {
			temp = e.getKey();
			temp2 = String.valueOf(e.getValue());
			
			context.write(new Text(temp2), new Text(temp));
		}
    } 
  }


}
