package Average;

import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PrimeAvgRating {
    public static void main (String[] args) throws Exception {
        if (args.length != 2 ){
            System.err.println("Syntax : MovieRating <input path> <output path>");
            System.exit(-1);
        }

        Job job = new Job();
        job.setJarByClass(AverageRating.class);
        job.setJobName("Prime Average Rating ");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(FloatWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}