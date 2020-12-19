import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.wltea.analyzer.IKSegmentation;
import org.wltea.analyzer.Lexeme;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

// 参考代码 https://blog.csdn.net/laozhaokun/article/details/38614765

public class CommentsMAP extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text word = new Text();
    private final static IntWritable one = new IntWritable(1);

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        获取文件名字
//        InputSplit inputSplit = (InputSplit) context.getInputSplit();
//        String filename = ((FileSplit) inputSplit).getPath().getName();
//        System.out.println("当前读取的文件名为：" + filename);

        String line = value.toString().trim();
        List<String> list = segment(line);
        for (String s : list) {
            word.set(s);
            context.write(word, one);
        }

    }

    private List<String> segment(String str) throws IOException {

        IKSegmentation iks = new IKSegmentation(new StringReader(str), false);

        byte[] byt = str.getBytes();
        InputStream is = new ByteArrayInputStream(byt);
        Reader reader = new InputStreamReader(is);
        iks.reset(reader);
        Lexeme lexeme;
        List<String> list = new ArrayList<String>();
        while ((lexeme = iks.next()) != null) {
            String text = lexeme.getLexemeText();
            list.add(text);
        }
        return list;
    }
}