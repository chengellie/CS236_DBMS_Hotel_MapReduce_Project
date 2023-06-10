package org.myorg;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class RevenueCount {

   public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, FloatWritable> {
    //  private final static IntWritable one = new IntWritable(1);
     private FloatWritable cost = new FloatWritable();
     private Text year_month = new Text();

     public void map(LongWritable key, Text value, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException {
       String line = value.toString();
       StringTokenizer tokenizer = new StringTokenizer(line, ",");
       while (tokenizer.hasMoreTokens()) {
         cost.set(tokenizer.nextToken());
         year_month.set(tokenizer.nextToken());
         output.collect(year_month, cost);
       }
     }
   }

   public static class Reduce extends MapReduceBase implements Reducer<Text, FloatWritable, Text, FloatWritable> {
     public void reduce(Text key, Iterator<FloatWritable> values, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException {
       float sum = 0;
       while (values.hasNext()) {
         sum += values.next().get();
       }
       output.collect(key, new FloatWritable(sum));
     }
   }

   public static void main(String[] args) throws Exception {
     JobConf conf = new JobConf(RevenueCount.class);
     conf.setJobName("revenuecount");

     conf.setOutputKeyClass(Text.class);
     conf.setOutputValueClass(FloatWritable.class);

     conf.setMapperClass(Map.class);
     conf.setCombinerClass(Reduce.class);
     conf.setReducerClass(Reduce.class);

     conf.setInputFormat(TextInputFormat.class);
     conf.setOutputFormat(TextOutputFormat.class);

     FileInputFormat.setInputPaths(conf, new Path(args[0]));
     FileOutputFormat.setOutputPath(conf, new Path(args[1]));

     JobClient.runJob(conf);
   }
}