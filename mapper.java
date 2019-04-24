import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;




public class mapper extends Mapper<LongWritable,Text,Text,IntWritable>{
public static IntWritable one = new IntWritable(1) ;
public  void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException{
    String s=val.toString();
    String [] st=s.split("-");
    context.write(new Text(st[0]),one);
}
}