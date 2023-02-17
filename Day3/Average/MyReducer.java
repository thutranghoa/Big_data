package Average;

import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public static class MyReducer extends Reducer <IntWritable, FloatWritable, IntWritable, FloatWritable>{
        @Override
    
        public void reduce (IntWritable key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            float count = 0;
            float sum = 0;
            String title = "";

            for (FloatWritable value: values) {
                if (value == 2 || value == 3 || value == 5){
                    sum += value.get();
                count = count + 1;
                }
            }
    
            FloatWritable  value = new FloatWritable(sum/count);
    
            context.write(key, value);
        }
        
    }