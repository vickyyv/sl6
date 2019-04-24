import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class reducer extends Reducer<Text, IntWritable, Text, IntWritable>{
    public void reduce(Text key, Iterable<IntWritable> value, Context con) throws IOException, InterruptedException
    {
        Text k=key;
        int freq=0;
        for(IntWritable v :value)
        {
            freq+=v.get();
        }
        con.write(k, new IntWritable(freq));
    }
}