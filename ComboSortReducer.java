package com.etcxy.servicesort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer
 *
 * @author etcxy
 */
public class ComboSortReducer extends Reducer<ComboKey, NullWritable, ComboKey, NullWritable> {

    int TOPN = 1;

    @Override
    protected void setup(Context context) {
        //从环境中去的需要求的topn值
        Configuration conf = context.getConfiguration();
        TOPN = Integer.parseInt(conf.get("TOPN"));
    }


    @Override
    protected void reduce(ComboKey key, Iterable<NullWritable> values, Context context)
            throws IOException, InterruptedException {

//        ComboKey cKey = new ComboKey(-1, "1", -1);
//
//        for (NullWritable v : values) {
//            if (key.getCharge() > cKey.getCharge()) {
//                ComboKey tmp = new ComboKey(key.getUserID(), key.getServerName(), key.getCharge());
//                cKey = tmp;
//            }
//        }
//        context.write(cKey, NullWritable.get());

        int flag = 0;
        for (NullWritable v : values) {
            if (flag++ < TOPN)
                context.write(key, NullWritable.get());

        }

    }
}
