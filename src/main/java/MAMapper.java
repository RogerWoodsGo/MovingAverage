import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.Queue;

/**
 * Created by beyondwu on 2016/3/16.
 */
public class MAMapper extends Mapper<LongWritable, Text, LongWritable, FloatWritable> {
    private LinkedList<Float> seriesQueue;
    private int currentLine = 0;
    public static final int N = 10;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        seriesQueue = new LinkedList<Float>();
        Configuration conf = context.getConfiguration();
        conf.setInt("QueueSize", N);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //super.map(key, value, context);
        Iterator<Float> iter;
        seriesQueue.push(Float.valueOf(value.toString()));
        currentLine++;

        if (seriesQueue.size() > N) {
            iter = seriesQueue.iterator();
            while (iter.hasNext()) {
                context.write(new LongWritable(currentLine), new FloatWritable(iter.next()));
            }
            seriesQueue.poll();
        }
    }
}
