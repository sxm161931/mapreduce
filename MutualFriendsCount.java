import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.*;

public class MutualFriendsCount {

    public static class Map
            extends Mapper<LongWritable, Text, Text, Text>{

       // private final static IntWritable one = new IntWritable(1);
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


    public static class Map2
            extends Mapper<LongWritable, Text, IntWritable, Text>{

        // private final static IntWritable one = new IntWritable(1);
        private Text word = new Text(); // type of output key
        private final static IntWritable one = new IntWritable(1);
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] lineArray = value.toString().split("\\t");
            context.write(new IntWritable(Integer.parseInt(lineArray[0])), new Text(lineArray[1]));


        }
    }
    public static class Reducer1
    extends Reducer<Text,Text,IntWritable,Text> {

        //   private IntWritable result = new IntWritable();


        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0; // initialize the sum for each keyword
            ArrayList<String[]> arr = new ArrayList<String[]>();
            Text s = new Text();
            //System.out.println("key:" + key);


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

                context.write(new IntWritable(set1.size()),new Text(key));


        }
    }


    public static class Reduce2
            extends  Reducer<IntWritable,Text,Text,IntWritable> {

     //   private IntWritable result = new IntWritable();
        int count =0;
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0; // initialize the sum for each keyword
            if(count < 10)
            {

            for(Text t : values)
            {
                if(count<10) {
                    context.write(new Text(t), key);
                    count++;
                }
            }

            }
            else
            {
                return;
            }

        }
    }


    public static class SortComparator extends WritableComparator {

        public SortComparator() {
            super(IntWritable.class);
        }


        @Override
        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {

            Integer v1 = ByteBuffer.wrap(b1, s1, l1).getInt();
            Integer v2 = ByteBuffer.wrap(b2, s2, l2).getInt();

            return v1.compareTo(v2) * (-1);
        }
    }


    // Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // get all args
        if (otherArgs.length != 2) {
            System.err.println("Usage: MutualFriendsCount <in> <out>");
            System.exit(2);
        }


        Job job = new Job(conf, "friendcount");
        job.setJarByClass(MutualFriendsCount.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reducer1.class);
        String intermediate ="intermediate_output";


        FileSystem fs = FileSystem.get(new URI(intermediate.toString()), conf);
        //It will delete the output directory if it already exists. don't need to delete it  manually
        fs.delete(new Path(intermediate));
        // uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);
        fs = FileSystem.get(new URI(otherArgs[1]), conf);
        fs.delete(new Path(otherArgs[1]));

        // set output key type
        job.setOutputKeyClass(Text.class);
        // set output value type
        job.setOutputValueClass(Text.class);
        //set the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        // set the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(intermediate));
        job.waitForCompletion(true);


        Job job2 = new Job(conf, "friendcount 2");
        job2.setJarByClass(MutualFriendsCount.class);
        job2.setMapperClass(Map2.class);
        //job.setCombinerClass(CombineClass.class);
        job2.setReducerClass(Reduce2.class);
        job2.setNumReduceTasks(1);
        //String intermediate ="intermediate_output";
        job2.setSortComparatorClass(MutualFriendsCount.SortComparator.class);
        // uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);

        // set output key type
        job2.setOutputKeyClass(IntWritable.class);
        // set output value type
        job2.setOutputValueClass(Text.class);
        //set the HDFS path of the input data

        job2.setSortComparatorClass(SortComparator.class);

        FileInputFormat.addInputPath(job2, new Path(intermediate));
        // set the HDFS path for the output
        //FileOutputFormat.setOutputPath(job, new Path(intermediate));
        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]));
        //Wait till job completion
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }



}
