import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job ;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class AverageRating {

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

    public static class MyReducer extends Reducer <IntWritable, FloatWritable, IntWritable, FloatWritable>{
        @Override
    
        public void reduce (IntWritable key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            float count = 0;
            float sum = 0;
            String title = "";

            for (FloatWritable value: values) {
                sum += value.get();
                count = count + 1;
            }
    
            FloatWritable  value = new FloatWritable(sum/count);
    
            context.write(key, value);
        }
        
    }

    
    public static void main (String[] args) throws Exception {
        if (args.length != 2 ){
            System.err.println("Syntax : MovieRating <input path> <output path>");
            System.exit(-1);
        }

        Job job = new Job();
        job.setJarByClass(AverageRating.class);
        job.setJobName("Average Rating");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(FloatWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}