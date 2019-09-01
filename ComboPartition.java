package com.etcxy.servicesort;


import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class ComboPartition extends Partitioner<ComboKey, NullWritable> {

    @Override
    public int getPartition(ComboKey comboKey, NullWritable nullWritable, int numPartitions) {
        return comboKey.getUserID()%numPartitions;
    }
}
