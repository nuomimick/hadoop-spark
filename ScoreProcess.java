import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
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
public class ScoreProcess extends Configured implements Tool {

    public static class Map extends Mapper<LongWritable, Text, Text, FloatWritable>{
        public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizerArticle = new StringTokenizer(line,"\n");
            while(tokenizerArticle.hasMoreTokens()){
                StringTokenizer tokenizerLine = new StringTokenizer(tokenizerArticle.nextToken());
                String strName =  tokenizerLine.nextToken();
                String strScore = tokenizerLine.nextToken();
                Text name = new Text(strName);
                float score = Float.parseFloat(strScore);
                context.write(name,new FloatWritable(score));
            }
        }
    }

    public static class Reduce extends Reducer<Text,FloatWritable,Text,FloatWritable>{
        public void reduce(Text key,Iterable<FloatWritable> values,Context context) throws IOException, InterruptedException {
            float sum = 0;
            int count = 0;
            for(FloatWritable var : values){
                sum += var.get();
                count++;
            }
            float average = sum / count;
            context.write(key, new FloatWritable(average));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance();
        job.setJarByClass(ScoreProcess.class);
        job.setJobName("Score Process");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
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

    public static void main(String[] args) throws Exception{
        int ret = ToolRunner.run(new ScoreProcess(),args);
        System.exit(ret);
    }
}
