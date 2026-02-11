
package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {

        double sum = 0.0;
        long cnt = 0;

        for (DoubleWritable v : values) {
            sum += v.get();
            cnt++;
        }

        double avg = (cnt == 0) ? 0.0 : (sum / cnt);
        context.write(key, new DoubleWritable(avg));
    }
}



