
import java.io.*;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class UserRating extends Configured implements Tool {

    // Mapper
    public static class DistributedMap extends
            Mapper<LongWritable, Text, Text, Text> {
        private Text rate  = new Text();
        private Text user = new Text(); // type of output key
        private HashMap<String,String> stopWordsSet = new HashMap<String,String>();

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            try {
                Path[] stopWordsFiles = DistributedCache
                        .getLocalCacheFiles(context.getConfiguration());
                //Path[] stopWordsFiles =  context.getLocalCacheFiles();
                if (stopWordsFiles != null && stopWordsFiles.length > 0) {
                    for (Path stopWordsItem : stopWordsFiles) {
                        System.out.println(stopWordsItem);
                        readFile(stopWordsItem);
                    }
                }

            } catch (Exception ex) {
                System.err.println("Exception in mapper setup: "
                        + ex.getMessage());
            }
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] line = value.toString().split("::");
            String reviewId = "",userId ="",businessId = "",stars ="";

            if(line.length > 1)
            {

                reviewId = line[0];
                userId = line[1];
                businessId = line[2];
                stars = line[3];
                //System.out.println(userId);

                if(stopWordsSet.containsKey(businessId.toLowerCase()))
                {
                    //System.out.println("matched");
                    rate.set(stars);
                    user.set(userId);
                    context.write(user,rate);
                }
            }


        }

        private void readFile(Path filePath) {
            try {
                System.out.println(filePath.getName());
                String subpath[] = filePath.toString().split(":");
                String path =(  subpath[1]);

                FileSystem fs = FileSystem.get(new Configuration());
                BufferedReader bufferedReader = new BufferedReader(
                        new FileReader(path));
                String stopwords = null;
                while ((stopwords = bufferedReader.readLine()) != null) {
                    String[] line = stopwords.toLowerCase().split("::");

                    if(stopwords.toLowerCase().contains("palo alto")) {
                        //System.out.println(line[0] + "\t" + line[1]);
                        stopWordsSet.put(line[0], line[1]);
                    }

                }

            } catch (Exception e) {
                System.err.println("Exception while reading stop words file: "
                        + e.getMessage());
            }

        }

    }

    // Reducer
    public static class DistributedReduce extends
            Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            Float rate = 0.0f; // initialize the rate for each keyword
            int count =0;
            for (Text t : values) {
                rate += Float.parseFloat(t.toString());
                count++;
            }
            if(count > 1)
            {
                rate = rate / count;
                //System.out.println(rate);
            }
            result.set(rate.toString());
            context.write(key, result);// create a pair <keyword, number of
            // occurences>
        }
    }

    // Driver
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new UserRating(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.printf("Usage: %s needs two arguments <input> <output> <stopwordsfile> files\n",
                    getClass().getSimpleName());
            return -1;
        }

        // Initialize the Hadoop job and set the jar as well as the name of the
        // Job
        Configuration conf = new Configuration();
        Job job = new Job(conf);
        job.setJarByClass(UserRating.class);
        job.setJobName("Word Counter With Stop Words Removal");

        // Add input and output file paths to job based on the arguments passed
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);


        // Set the MapClass and ReduceClass in the job
        job.setMapperClass(DistributedMap.class);
        job.setReducerClass(DistributedReduce.class);
        //job.addCacheFile(new URI(args[2]));
        //DistributedCache.createSymlink(conf);
        DistributedCache.addCacheFile(new URI("hdfs://localhost:9000"+args[2]),job.getConfiguration());

        // Wait for the job to complete and print if the job was successful or
        // not
        int returnValue = job.waitForCompletion(true) ? 0 : 1;

        if (job.isSuccessful()) {
            System.out.println("Job was successful");
        } else if (!job.isSuccessful()) {
            System.out.println("Job was not successful");
        }

        return returnValue;
    }

}