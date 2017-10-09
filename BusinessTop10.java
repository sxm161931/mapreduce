import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.*;

public class BusinessTop10 {

    public static class Map
            extends Mapper<LongWritable, Text, Text, FloatWritable>{

        private final static IntWritable one = new IntWritable(1);
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //System.out.println(value.toString());
            StringTokenizer tokenizer = new StringTokenizer(value.toString(), "\\r?\\n");
            String line = null;
            String lineArray[] = null;
            //String[] friendArray = null;
            String business = null;
            float rating =0.0f;

                //System.out.print("inside loop");
                line = value.toString();
                //System.out.println(line);
                //line = ;
                lineArray = line.split("::");

                // System.out.println(lineArray.length);
                if(lineArray.length > 1) {
                    business = lineArray[2];
                    rating = Float.parseFloat(lineArray[3]);
                    context.write(new Text(business), new FloatWritable(rating));

                }
                else
                {
                    context.write(new Text(business), new FloatWritable(rating));
                }

        }
    }


    public static class Map2
            extends Mapper<LongWritable, Text, Text, Text>{

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] lineArray = value.toString().split("\\t");
            if(lineArray.length > 1) {
                context.write(new Text(lineArray[0]), new Text("rate" + "\t" + lineArray[1]));
            }

        }
    }

    public static class Map3
            extends Mapper<LongWritable, Text, Text, Text>{

        private Text word = new Text(); // type of output key
        private final static IntWritable one = new IntWritable(1);
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String business = "";
            String address = "";
            String categories = "";
            String[] lineArray = value.toString().split("::");

            if(lineArray.length > 1)
            {
                business = lineArray[0];
                address = lineArray[1];
                categories = lineArray[2];

            }

            context.write(new Text(business), new Text("buss" + "\t" + address + "\t" + categories));



        }
    }


    public static class Map4
            extends Mapper<LongWritable, Text, FloatWritable, Text>{
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] lineArray = value.toString().split("\\t");
            if(lineArray.length > 1) {
                context.write(new FloatWritable(Float.parseFloat(lineArray[0])), new Text(lineArray[1] + "\t" + lineArray[2] + "\t"+lineArray[3]));
            }

        }
    }


    public static class Reducer3
            extends Reducer<FloatWritable,Text,Text,FloatWritable> {
        private TreeMap countMap = new TreeMap<>();
    int count =0;
        public void reduce(FloatWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String details="";

                for (Text t : values) {
                    details = t.toString();
                }
                //context.write(new Text(details) ,(key));
                countMap.put(details,key);

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

            Set set = countMap.entrySet();
            Iterator i = set.iterator();
            // Display elements
            while(i.hasNext() && count < 10 ) {
                HashMap.Entry me = (HashMap.Entry)i.next();
                context.write((new Text((String)me.getKey())) ,(FloatWritable)(me.getValue()) );
                count++;
            }

            }

    }

    public static class Reducer1
    extends Reducer<Text,FloatWritable,Text,FloatWritable> {

        public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            float avg = 0.0f; // initialize the sum for each keyword
            int size = 0;

                for(FloatWritable rate : values)
                {
                    avg += rate.get();
                    size++;
                }

               if(size > 0)
               {
                   avg = avg / size;
               }

                context.write(new Text(key) ,new FloatWritable(avg));

        }
    }


    public static class Reduce2
            extends  Reducer<Text,Text,FloatWritable,Text> {


        int count =0;
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String business ="";
            float rating = 0.0f;
            //System.out.println(key);
            for(Text t : values)
            {
               // System.out.println(t.toString());
                String params[] = t.toString().split("\\t");
                if(params[0].equals("buss"))
                {
                    business = params[1] + "\t" + params[2];
                }
                else
                {
                    rating = Float.parseFloat(params[1]);
                }

            }


            context.write(new FloatWritable(rating), new Text(key + "\t" + business));

        }
    }


    public static class SortComparator extends WritableComparator {

        public SortComparator() {
            super(FloatWritable.class);
            // TODO Auto-generated constructor stub
            //System.out.println("gen");
        }

        @Override
        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {

            Float v1 = ByteBuffer.wrap(b1, s1, l1).getFloat();
            Float v2 = ByteBuffer.wrap(b2, s2, l2).getFloat();

            return v1.compareTo(v2) * (-1);
        }

    }


    // Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // get all args


        if (otherArgs.length != 3) {
            System.err.println("Usage: Top 10 businesses <review.csv path> <business.csv path> <output path>");
            System.exit(2);
        }

        Job job = new Job(conf, "top10bussiness");
        job.setJarByClass(BusinessTop10.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reducer1.class);
        String intermediate ="intermediate_output";


        FileSystem fs = FileSystem.get(new URI(intermediate.toString()), conf);
        //It will delete the output directory if it already exists. don't need to delete it  manually
        fs.delete(new Path(intermediate));
        // uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);
        fs = FileSystem.get(new URI(otherArgs[2]), conf);
        fs.delete(new Path(otherArgs[2]));

        // set output key type
        job.setOutputKeyClass(Text.class);
        // set output value type
        job.setOutputValueClass(FloatWritable.class);
        //set the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        // set the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(intermediate));
        job.waitForCompletion(true);


        Job job2 = new Job(conf, "friendcount 2");
        job2.setJarByClass(BusinessTop10.class);
        job2.setReducerClass(Reduce2.class);
        MultipleInputs.addInputPath(job2, new Path(intermediate),TextInputFormat.class, Map2.class);
        MultipleInputs.addInputPath(job2, new Path(args[1]),TextInputFormat.class, Map3.class);

        // set output key type
        job2.setOutputKeyClass(Text.class);
        // set output value type
        job2.setOutputValueClass(Text.class);

        String intermediate2 ="intermediate_output2";
        fs = FileSystem.get(new URI(intermediate2.toString()), conf);
        //It will delete the output directory if it already exists. don't need to delete it  manually
        fs.delete(new Path(intermediate2));
        FileOutputFormat.setOutputPath(job2, new Path(intermediate2));
        //Wait till job completion
        job2.waitForCompletion(true);

        Job job3 = new Job(conf, "top10bussiness");
        job3.setJarByClass(BusinessTop10.class);
        job3.setMapperClass(Map4.class);
        job3.setReducerClass(Reducer3.class);

        // set output key type
        job3.setOutputKeyClass(FloatWritable.class);

        // set output value type
        job3.setOutputValueClass(Text.class);
        //set the HDFS path of the input data
        FileInputFormat.addInputPath(job3, new Path(intermediate2));
        // set the HDFS path for the output
        FileOutputFormat.setOutputPath(job3, new Path((otherArgs[2])));



        System.exit(job3.waitForCompletion(true) ? 0 : 1);
    }



}
