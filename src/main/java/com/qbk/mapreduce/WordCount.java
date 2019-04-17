package com.qbk.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * MapReduce 官网demo：统计文件中每个单词出现的次数
 *
 * TextInputFormat是hadoop默认的输入格式，这个类继承自FileInputFormat,使用这种输入格式，每个文件都会单独作为Map的输入，每行数据都会生成一条记录，每条记录会表示成<key，value>的形式。
 * key的值是每条数据记录在数据分片中的字节偏移量，数据类型是LongWritable.
 * value的值为每行的内容，数据类型为Text。
 *
 * 实际上InputFormat（）是用来生成可供Map处理的<key，value>的。
 * InputSplit是hadoop中用来把输入数据传送给每个单独的Map(也就是我们常说的一个split对应一个Map),
 * InputSplit存储的并非数据本身，而是一个分片长度和一个记录数据位置的数组。
 * 生成InputSplit的方法可以通过InputFormat（）来设置。
 * 当数据传给Map时，Map会将输入分片传送给InputFormat（），InputFormat()则调用getRecordReader()生成RecordReader,RecordReader则再通过creatKey()和creatValue()创建可供Map处理的<key，value>对。
 *
 * OutputFormat()
 * 默认的输出格式为TextOutputFormat。它和默认输入格式类似，会将每条记录以一行的形式存入文本文件。它的键和值可以是任意形式的，因为程序内部会调用toString()将键和值转化为String类型再输出。
 */
public class WordCount {
    /**
     * 建立Mapper类TokenizerMapper继承自泛型类Mapper
     * Mapper类:实现了Map功能基类
     * Mapper接口：
     * Mapper接口一个泛型类型，它有四个形参类型，分别指定map函数的输入键、输入值、输出键、输出值的类型。hadoop没有直接使用Java内嵌的类型，而是自己开发了一套可以优化网络序列化传输的基本类型。
     * WritableComparable接口：实现WritableComparable的类可以相互比较。所有被用作key的类应该实现此接口。
     * Reporter 则可用于报告整个应用的运行进度，本例中未使用。
     */
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
        /**
         * IntWritable, Text 均是 Hadoop 中实现的用于封装 Java 数据类型的类，这些类实现了WritableComparable接口，
         * 都能够被串行化从而便于在分布式环境中进行数据交换，你可以将它们分别视为int,String 的替代品。
         * 声明one常量和word用于存放单词的变量
         * 这个1表示每个单词出现一次，map的输出value就是1.
         */
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        /**
         * Mapper中的map方法：
         * void map(K1 key, V1 value, Context context)
         * 映射一个单个的输入k/v对到一个中间的k/v对
         * 输出对不需要和输入对是相同的类型，输入对可以映射到0个或多个输出对。
         * Context：收集Mapper输出的<k,v>对。
         * Context的write(k, v)方法:增加一个(k,v)对到context
         * 程序员主要编写Map和Reduce函数.这个Map函数使用StringTokenizer函数对字符串进行分隔,通过write方法把单词存入word中
         * write方法存入(单词,1)这样的二元组到context中
         */
        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }
    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();
        /**
         * Reducer类中的reduce方法：
         * void reduce(Text key, Iterable<IntWritable> values, Context context)
         * 中k/v来自于map函数中的context,可能经过了进一步处理(combiner),同样通过context输出
         */
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
    /**
     *  程序入口 main
     */
    public static void main(String[]  args) throws Exception {
        //
        /**
         * 程序里，只需写这么一句话，就会加载到hadoop的配置文件了
         * Configuration类代表作业的配置，该类会加载mapred-site.xml、hdfs-site.xml、core-site.xml等配置文件。
         * Configuration：map/reduce的j配置类，向hadoop框架描述map-reduce执行的工作
         */
        Configuration conf = new Configuration();

        //FileSystem fileSystem = FileSystem.get( URI.create("hdfs://192.168.11.234:9000"),conf,"root");

        //设置一个用户定义的job名称
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        //为job设置Mapper类
        job.setMapperClass(TokenizerMapper.class);
        //为job设置Combiner类
        job.setCombinerClass(IntSumReducer.class);
        //为job设置Reducer类
        job.setReducerClass(IntSumReducer.class);
        //为job的输出数据设置Key类
        job.setOutputKeyClass(Text.class);
        //为job输出设置value类
        job.setOutputValueClass(IntWritable.class);
        //为job设置输入路径
       FileInputFormat.addInputPath(job, new Path(args[0]));
        //为job设置输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //指定的这个路径可以是单个文件、一个目录或符合特定文件模式的一系列文件。
//        FileInputFormat.addInputPath(job, new Path("hdfs://192.168.1.234:9000/qbk/test/hello.txt"));
        //只能有一个输出路径，该路径指定的就是reduce函数输出文件的写入目录。 特别注意：输出目录不能提前存在，否则hadoop会报错并拒绝执行作业
//        FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.1.234:9000/output/test"));
        //运行job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
