import java.io.IOException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import javax.sound.midi.SysexMessage;

public class MutualFriends {

    public static class Map
            extends Mapper<LongWritable, Text, Text, Text>{

        private Text word = new Text(); // type of output key
        private final static IntWritable one = new IntWritable(1);
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //System.out.println(value.toString());
            StringTokenizer tokenizer = new StringTokenizer(value.toString(), "\\r?\\n");
            String line = null;
            String lineArray[] = null;
            String[] friendArray = null;
            String name = null;
            String friends ="";

                //System.out.print("inside loop");
                line = value.toString();
                //System.out.println(line);
                //line = ;
                lineArray = line.split("\\t");
                name = lineArray[0];
                // System.out.println(lineArray.length);
                if(lineArray.length > 1) {
                    friends = lineArray[1];
                    friendArray = friends.split(",");
                    for (String s : friendArray) {

                        if (Integer.parseInt(name) > Integer.parseInt(s)) {
                            word.set(s + "," + name);
                        } else {
                            word.set(name + "," + s);
                        }
                        context.write(word, new Text(friends));
                        word = new Text();
                        // output.collect(word, friends);

                    }
                }
                else
                {
                    context.write(new Text(name), new Text());
                }
        }
    }


    public static class Reduce
            extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //System.out.println("key:" + key);
            if(key.toString().equals("0,4") || key.toString().equals("20,22939") || key.toString().equals("1,29826") || key.toString().equals("6222,19272") || key.toString().equals("28041,28056"))
            {
                //System.out.println("key:" + key);
                int count = 0;
                    HashSet<String> set1 = set1 = new HashSet<String>();
                    HashSet<String> set2 = null;

                    for (Text value : values) {
                        //System.out.println("values:"+value);
                        count++;
                        if(count == 1)
                        {
                            set1 = new HashSet<String>(Arrays.asList(value.toString().split(",")));
                        }
                        else
                        {
                            set2 = new HashSet<String>(Arrays.asList(value.toString().split(",")));
                            set1.retainAll(set2);
                        }
                    }
                   // mutualFriend.put(set1.size(), key.toString());
                    context.write(new Text(key), new Text(set1.toString()));
            }
        }
    }


    // Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // get all args
        if (otherArgs.length != 2) {
            System.err.println("Usage: MutualFriends <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "Mutual Friends");
        job.setJarByClass(MutualFriends.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        // set output key type
        job.setOutputKeyClass(Text.class);
        // set output value type
        job.setOutputValueClass(Text.class);
        //set the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        // set the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        //Wait till job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }



}
