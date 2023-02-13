package Day2;

import java.io.IOException;

import javax.xml.soap.Text;

public static class MyMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

    @Override
    public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] rating = line.split("\t");

        if(rating.length >= 4) {

            int rate = Integer.parseInt(rating[2]);

            IntWritable mapKey = new IntWritable(rate);
            IntWritable mapValue = new IntWritable(1);

            contex.write(mapKey, mapValue);
        }
    }
}
