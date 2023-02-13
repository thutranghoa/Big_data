package Day2;

import java.io.IOException;

public static class MyReducer extends Reducer <IntWritable, IntWritable, IntWritable, IntWritable>{
    @Override

    public void reduce (IntWritable key, Iterable<IntWritable> values, Contex contex) throws IOException, InterruptedException {
        int count = 0;
        String title = "";
        for (IntWritable value: values) {
            count += value.get();
        }

        IntWritable  value = new IntWritable(count);

        contex.write(key, value);
    }
    
}
