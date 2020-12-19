import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CommentsReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        // 中文字
        Pattern p = Pattern.compile("[\u4e00-\u9fa5]");
        Matcher m = p.matcher(String.valueOf(key));
        if (m.find()) {
            int sum = 0;
            for (IntWritable val : values)
                sum += val.get();
            // ci pin da yv 1 cai shu chu
            if (sum > 1) {
                result.set(sum);
                context.write(key, result);
            }
        } else {
            return;
        }
    }
}