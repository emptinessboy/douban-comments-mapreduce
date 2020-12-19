import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class CommentsMR extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new CommentsMR(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] arg0) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "nseg.jar");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(new Path("hdfs://localhost:9000/user/huxiaofan/douban-out/comment")))
            fs.delete(new Path("hdfs://localhost:9000/user/huxiaofan/douban-out/comment"), true);
        job.setJarByClass(CommentsMR.class);
        job.setMapperClass(CommentsMAP.class);
        job.setCombinerClass(CommentsReduce.class);
        job.setReducerClass(CommentsReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/user/huxiaofan/douban-in/comments"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/user/huxiaofan/douban-out/comment"));

        return job.waitForCompletion(true) ? 0 : 1;
    }


}