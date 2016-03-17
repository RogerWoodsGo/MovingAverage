import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by beyondwu on 2016/3/16.
 */
public class CustomPartitioner extends Partitioner<LongWritable, FloatWritable> implements Configurable{
    private Configuration conf;
    @Override
    public void setConf(Configuration configuration) {
        this.conf = configuration;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public int getPartition(LongWritable intWritable, FloatWritable floatWritable, int i) {
        return (int)(intWritable.get()) % i;
    }
}
