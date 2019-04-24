
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
public void map(LongWritable key,Text value, Context con) throws IOException, InterruptedException
{
	int temp;
	int missing=9999;
	  String line=value.toString();
	  String year= line.substring(15,19);
	 if(line.charAt(87)=='+')
	 {
		 temp=Integer.parseInt(line.substring(88,92));
	 }
	 else
		 temp=Integer.parseInt(line.substring(87,92));
	 String quality=line.substring(92,93);
	 if(temp!=missing && quality.matches("[01451]")){
	 con.write(new Text(year), new IntWritable(temp));
	 }
}
}