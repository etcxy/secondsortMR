package com.etcxy.servicesort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author etcxy
 */
public class OrderApp {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {


        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        conf.set("TOPN", "2");

        Path path = new Path("data/output");

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(path)) {
            fs.delete(path, true);
        }

        Job job = Job.getInstance(conf, "topn");
        job.setJarByClass(OrderApp.class);

        job.setInputFormatClass(TextInputFormat.class);

        job.setNumReduceTasks(3);
        job.setOutputKeyClass(ComboKey.class);
        job.setOutputValueClass(ComboKey.class);

        job.setMapperClass(ComboSortMapper.class);
        job.setReducerClass(ComboSortReducer.class);

        FileInputFormat.addInputPath(job, new Path("data/order.log"));
        FileOutputFormat.setOutputPath(job, new Path("data/output"));

        //设置map输出类型
        job.setMapOutputKeyClass(ComboKey.class);
        job.setMapOutputValueClass(NullWritable.class);

        //设置最终输出类型
        job.setOutputKeyClass(ComboKey.class);
        job.setOutputValueClass(NullWritable.class);

        //设置分区类
        job.setPartitionerClass(ComboPartition.class);
        //设置分组类
        job.setGroupingComparatorClass(UserIDGroupComparator.class);
        //设置对比类
//        job.setSortComparatorClass(ComboKeyComparator.class);


        job.waitForCompletion(true);
    }
}
