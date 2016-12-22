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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by huan on 16-11-5.
 */
public class WordCount extends Configured implements Tool{
    /**
     * Mapper区: WordCount程序 Map 类
     * Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>:
     *        |       |           |       |
     *  输入key类型  输入value类型  输出key类型 输出value类型
     */
    public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        // 输出结果
        private Text word = new Text();                             // KEYOUT
        // 因为若每个单词出现后，就置为 1，并将其作为一个<key,value>对，因此可以声明为常量，值为 1
        private final static IntWritable one = new IntWritable(1);  // VALUEOUT

        /**
         * value 是文本每一行的值
         * context 是上下文对象
         */
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 获取每行数据的值
            String lineValue = value.toString();
            // 分词：将每行的单词进行分割,按照"  \t\n\r\f"(空格、制表符、换行符、回车符、换页)进行分割
            StringTokenizer tokenizer = new StringTokenizer(lineValue);
            // 遍历
            while (tokenizer.hasMoreTokens()) {
                // 获取每个值
                String wordValue = tokenizer.nextToken();
                // 设置 map 输出的 key 值
                word.set(wordValue);
                // 上下文输出 map 处理结果
                context.write(word, one);
            }
        }
    }

    /**
     * Reducer 区域：WordCount 程序 Reduce 类
     * Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>:Map 的输出类型，就是Reduce 的输入类型
     */
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        // 输出结果：总次数
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;                        // 累加器，累加每个单词出现的总次数
            // 遍历values
            for (IntWritable val : values) {
                sum += val.get();               // 累加
            }
            // 设置输出 value
            result.set(sum);
            // 上下文输出 reduce 结果
            context.write(key, result);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(WordCount.class);
        job.setJobName("word count");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // 判断output文件夹是否存在，如果存在则删除
        Path path = new Path(args[1]);// 取第1个表示输出目录参数（第0个参数是输入目录）
        FileSystem fileSystem = path.getFileSystem(conf);// 根据path找到这个文件
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);// true的意思是，就算output有东西，也一带删除
        }

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        boolean sucess = job.waitForCompletion(true);
        return sucess ? 0 : 1;
    }

    // Driver 区：客户端
//    public static void main(String[] args) throws Exception {
//        // 获取配置信息
//        Configuration conf = new Configuration();
//        // 创建一个 Job
//        Job job = Job.getInstance(conf, "word count");      // 设置 job name 为 word count
//        //job = new Job(conf, "word count");                  // 过时的方式
//        // 1. 设置 Job 运行的类
//        job.setJarByClass(WordCount.class);
//
//        // 2. 设置Mapper类和Reducer类
//        job.setMapperClass(TokenizerMapper.class);
//        job.setReducerClass(IntSumReducer.class);
//
//        // 3. 获取输入参数，设置输入文件目录和输出文件目录
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//
//        // 4. 设置输出结果 key 和 value 的类型
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);
//        //job.setCombinerClass(IntSumReducer.class);中间合并过程，写法跟reducer类似
//
//        // 5. 提交 job，等待运行结果，并在客户端显示运行信息，最后结束程序
//        boolean isSuccess = job.waitForCompletion(true);
//
//        // 结束程序
//        System.exit(isSuccess ? 0 : 1);
//    }
    public static void main(String[] args) throws Exception{
        int ret = ToolRunner.run(new WordCount(), args);
        System.exit(ret);
    }

}
