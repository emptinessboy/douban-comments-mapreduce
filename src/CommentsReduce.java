import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CommentsReduce extends Reducer<Text, IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();
    public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
        int sum = 0;
        for(IntWritable val : values)
            sum += val.get();
        // ci pin da yv 1 cai shu chu
        if (sum>1){
            result.set(sum);
            context.write(key, result);
        }
    }
}