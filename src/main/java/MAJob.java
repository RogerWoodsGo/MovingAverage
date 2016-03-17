import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by beyondwu on 2016/3/16.
 */
public class MAJob extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        if(strings.length < 2){
            System.out.println("please add input and output path");
            System.exit(-1);
        }
        conf.setInt("QueueSize", 10);
        Job job = Job.getInstance(conf);
        job.setJobName("TimeAverage");
        job.setJarByClass(MAJob.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(FloatWritable.class);
        job.setPartitionerClass(CustomPartitioner.class);

        job.setMapperClass(MAMapper.class);
        job.setReducerClass(MAReducer.class);
        job.setNumReduceTasks(3);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(strings[0]));
        System.out.println("++++++++++" + strings[0]);
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));
        return (job.waitForCompletion(true) ? 0 : 1);
    }
    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new Configuration(), new MAJob(), args);
        System.exit(ret);
    }
}
