// package edu.cs.utexas.HadoopEx;

// import java.io.IOException;
// import java.util.StringTokenizer;

// import org.apache.hadoop.io.IntWritable;
// import org.apache.hadoop.io.Text;
// import org.apache.hadoop.mapreduce.Mapper;

// public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {

// 	// Create a counter and initialize with 1
// 	private final IntWritable counter = new IntWritable(1);
// 	// Create a hadoop text object to store words
// 	private Text word = new Text();

// 	@Override
// 	public void map(Object key, Text value, Context context) 
// 		throws IOException, InterruptedException {

// 			String curr = value.toString();
// 			if(curr.startsWith("YEAR")){
// 				return;
// 			}

// 			String[] p = curr.split(",");


// 			if(p.length > 7){
// 				word.set(p[7]);
// 				context.write(word, counter);
// 			}
		
// 	}
// }


//part 2 code
package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<Object, Text, Text, DoubleWritable> {

    private final DoubleWritable outVal = new DoubleWritable();
    private final Text outKey = new Text();

    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String currLine = value.toString();
        if (currLine.startsWith("YEAR")) return;

        String[] parts = currLine.split(",");

        // airline = parts[4], delay = parts[11]
        if (parts.length > 11 && !parts[11].isEmpty()) {
            outKey.set(parts[4]);
            outVal.set(Double.parseDouble(parts[11]));
            context.write(outKey, outVal);
        }
    }
}

