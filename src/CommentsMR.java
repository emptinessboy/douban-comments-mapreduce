import java.io.*;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.wltea.analyzer.IKSegmentation;
import org.wltea.analyzer.Lexeme;



public class SegmentTool extends Configured implements Tool{
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SegmentTool(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] arg0) throws Exception {
        Configuration conf = new Configuration();
        String[] args = new GenericOptionsParser(conf,arg0).getRemainingArgs();
        if(args.length != 2){
            System.err.println("Usage:seg.SegmentTool <input> <output>");
            System.exit(2);
        }
        Job job = new Job(conf,"nseg.jar");
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(new Path(args[1])))
            fs.delete(new Path(args[1]),true);
        job.setJarByClass(SegmentTool.class);
        job.setMapperClass(SegmentMapper.class);
        job.setCombinerClass(SegReducer.class);
        job.setReducerClass(SegReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class SegmentMapper extends Mapper<LongWritable,Text,Text,IntWritable>{

//        private IKSegmenter iks = new IKSegmenter(true);



        private Text word = new Text();
        private final static IntWritable one = new IntWritable(1);
        public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
            String line = value.toString().trim();
            String[] str = line.split("\t");
            for(int i=1;i<str.length;i+=2){
                String tmp = str[i];
                if(tmp.startsWith("http"))
                    continue;
                List<String> list = segment(tmp);
                for(String s : list){
                    word.set(s);
                    context.write(word, one);
                }
            }
        }
        private List<String> segment(String str) throws IOException{

            IKSegmentation iks = new IKSegmentation(new StringReader(str), false);

            byte[] byt = str.getBytes();
            InputStream is = new ByteArrayInputStream(byt);
            Reader reader = new InputStreamReader(is);
            iks.reset(reader);
            Lexeme lexeme;
            List<String> list = new ArrayList<String>();
            while((lexeme = iks.next()) != null){
                String text = lexeme.getLexemeText();
                list.add(text);
            }
            return list;
        }
    }
    public static class SegReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        private IntWritable result = new IntWritable();
        public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
            int sum = 0;
            for(IntWritable val : values)
                sum += val.get();
            result.set(sum);
            context.write(key, result);
        }
    }

}