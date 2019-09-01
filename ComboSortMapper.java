package com.etcxy.servicesort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author etcxy
 */
public class ComboSortMapper extends Mapper<LongWritable, Text, ComboKey, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        ComboKey cKey = new ComboKey();

        String[] line = value.toString().split(",");
        cKey.setUserID(Integer.parseInt(line[0]));
        cKey.setServerName(line[1]);
        cKey.setCharge(Integer.parseInt(line[2]));

        context.write(cKey, NullWritable.get());

    }
}
