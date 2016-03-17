import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by beyondwu on 2016/3/16.
 */
public class MAReducer  extends Reducer<LongWritable, FloatWritable, LongWritable, FloatWritable> {
    private int queueSize;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //super.setup(context);
        queueSize = context.getConfiguration().getInt("QueueSize",5);
        System.out.println("queue size is: " + queueSize);
    }

    @Override
    protected void reduce(LongWritable key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<FloatWritable> iter = values.iterator();
        float sum = 0;
        while(iter.hasNext()){
            sum += iter.next().get();
        }
        context.write(key, new FloatWritable(sum/queueSize));
    }
}
