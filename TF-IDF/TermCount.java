package com.mowar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//mapreduce系统库
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//hadoop 通用库
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.StringTokenizer;

public class TermCount {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        //单词的数量为1
        private final static IntWritable one = new IntWritable(1);
        //当前切分的单词
        private Text word = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            //            获取map切分文件的文件名
            InputSplit inputSplit = context.getInputSplit();
            String filename = ((FileSplit) inputSplit).getPath().getName();
            String name = filename.substring(0, filename.lastIndexOf("."));

            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {

                //单词@文件名    组合键
                word.set(itr.nextToken() + "@" + name);

                //写入到文件中
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> vlaues, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : vlaues) {
                sum += val.get();
            }
            //转换数据类型
            result.set(sum);
            //输出
            context.write(key, result);
        }
    }

    //判断输出目录是否存在,如果目录存在,则删除该目录
    public static void OutDir_Is_Exis(String dir, Configuration conf) throws IOException {
//        dir 表示输出目录的参数
        Path path = new Path(dir);
        FileSystem fileSystem = path.getFileSystem(conf);
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: Term counting<in> [<in> ...] <out>");
            System.exit(2); //退出状态码
        }

        Job job = Job.getInstance(conf, "Term counting");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //输出文件和输出文件的设置
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }

        //判读输出目录是否存在，如果该目录存在,则删除该目录
        OutDir_Is_Exis(otherArgs[otherArgs.length - 1], conf);

        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
