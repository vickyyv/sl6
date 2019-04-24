import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class mr {
public static void main(String [] args) throws IOException, ClassNotFoundException, InterruptedException
{
    Configuration c=new Configuration();
    Job j= Job.getInstance(c,"driver");
    j.setJarByClass(mr.class);
    j.setMapperClass(mapper.class);
    j.setReducerClass(reducer.class);
    j.setOutputKeyClass(Text.class);
    j.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(j,new Path (args[0]));
    FileOutputFormat.setOutputPath(j, new Path(args[1]));
    System.exit(j.waitForCompletion(true)? 0:1 );
}
}