package com.mowar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//mapreduce系统库
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//hadoop 通用库
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.lang.Math;

public class TfIdf {

    //暴力版mapper，读取的数据依旧是<行偏移量,当前行的内容>，比较优秀的方法，
    // 重写输入文件读入格式,使其恰好输入的内容是<word@filename,数量@单词总数>
    // 修改写入key,value之间的符号(空白符号,逗号,分号等自定义符号)
    // 输出的格式为<word,filename=数量@单词的总数>
    public static class TokenizerMapper
            extends Mapper<Object,Text, Text, Text>{

        //当前切分的单词
        private Text word = new Text();

        //组合值
        private Text result = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException{

            StringTokenizer itr = new StringTokenizer(value.toString());

            // key
            String spectial_key = itr.nextToken();
            String[] split = spectial_key.split("@");
            // 设置键
            word.set(split[0]);

            // value
            String spectial_value = itr.nextToken();
            //设置值
            result.set(split[1] + "=" + spectial_value);
            context.write(word,result);

        }
    }

    // 输入格式<word,[filename=数量@单词总数]>
    // 输出格式<word@filename,tf-idf>
    public static class IntSumReducer
            extends Reducer<Text,Text,Text,FloatWritable>{

        private Text spectial_key = new Text();
        private FloatWritable tfidf = new FloatWritable();

        public void reduce(Text key, Iterable<Text> vlaues, Context context)
                throws IOException, InterruptedException{

            List<String> vlaues_list = new ArrayList<String>();
            vlaues_list.clear();

            for (Text val : vlaues){
                vlaues_list.add(val.toString());
            }

            //获取该单词在多少篇文档中出现过
            int sum = vlaues_list.size();

            for (String val : vlaues_list){
                String[] split = val.split("[=|@]");
                spectial_key.set(key + "@"+split[0]);
                //假设有5篇文章
                tfidf.set((float) ((Integer.parseInt(split[1]) / Float.parseFloat(split[2])) *Math.log10((float) (4* 1.0 / (sum*1.0)))));
                context.write(spectial_key,tfidf);
            }
        }
    }

    //判断输出目录是否存在,如果目录存在,则删除该目录
    public static void OutDir_Is_Exis(String dir, Configuration conf) throws IOException {
//        dir 表示输出目录的参数
        Path path = new Path(dir);
        FileSystem fileSystem = path.getFileSystem(conf);
        if (fileSystem.exists(path)){
            fileSystem.delete(path,true);
        }
    }

    public static void main(String[] args) throws  Exception{

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
        if (otherArgs.length < 2){
            System.err.println("Usage: tf-idf<in> [<in> ...] <out>");
            System.exit(2); //退出状态码
        }

        Job job = Job.getInstance(conf,"tf-idf");
        job.setJarByClass(TfIdf.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);

        //设置map端的输出格式
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //设置reduce端的输出格式
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        //输出文件和输出文件的设置
        for(int i = 0; i < otherArgs.length -1; ++i){
            FileInputFormat.addInputPath(job,new Path(otherArgs[i]));
        }
        FileInputFormat.setInputPathFilter(job,TextPathFilter.class);
        //判读输出目录是否存在，如果该目录存在,则删除该目录
        OutDir_Is_Exis(otherArgs[otherArgs.length -1],conf);

        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
