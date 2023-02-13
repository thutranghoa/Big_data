
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job ;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class RatingCount {

    public static class MyMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

        @Override
        public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    
            String line = value.toString();
            String[] rating = line.split("\t");
    
            if(rating.length >= 4) {
    
                int rate = Integer.parseInt(rating[2]);
    
                IntWritable mapKey = new IntWritable(rate);
                IntWritable mapValue = new IntWritable(1);
    
                context.write(mapKey, mapValue);
            }
        }
    }

    public static class MyReducer extends Reducer <IntWritable, IntWritable, IntWritable, IntWritable>{
        @Override
    
        public void reduce (IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            String title = "";
            for (IntWritable value: values) {
                count += value.get();
            }
    
            IntWritable  value = new IntWritable(count);
    
            context.write(key, value);
        }
        
    }

    
    public static void main (String[] args) throws Exception {
        if (args.length != 2 ){
            System.err.println("Syntax : MovieRating <input path> <output path>");
            System.exit(-1);
        }

        Job job = new Job();
        job.setJarByClass(RatingCount.class);
        job.setJobName("Rating count");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}