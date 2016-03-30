package biddings;

import com.epam.hadoop3hw.tags.TagsMapper;
import com.epam.hadoop3hw.tags.TagsReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by root on 3/30/16.
 */
public class Driver extends Configured implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(Driver.class);

    public static void main(String[] args) throws Exception {
        int code = ToolRunner.run(new Driver(), args);
        System.exit(code);
    }

    public int run(String[] args) throws Exception {
        LOG.info("Start!");

        Configuration conf = getConf();
//        conf.set(TextOutputFormat.SEPERATOR, ",");

        Job job = Job.getInstance(conf, "Hadoop HW3 Biddings");
        job.setJarByClass(Driver.class);
        job.setMapperClass(TagsMapper.class);
        job.setCombinerClass(TagsReducer.class);
        job.setReducerClass(TagsReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
//
//    job.setOutputKeyClass(Text.class);
//    job.setOutputValueClass(LogResultWritable.class);

//    job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
