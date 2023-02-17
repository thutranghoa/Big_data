package Average;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

public static class MyMapper extends Mapper<LongWritable, Text, IntWritable, FloatWritable> {

    @Override
    public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] rating = line.split("\t");

        if(rating.length >= 4) {
    
            int movie_id = Integer.parseInt(rating[1]);
            int rate = Integer.parseInt(rating[2]);
            

            IntWritable mapKey = new IntWritable(movie_id);
            FloatWritable mapValue = new FloatWritable(rate);

            context.write(mapKey, mapValue);
        }
    }
}