package biddings;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Vitaliy on 3/30/2016.
 */
public class BiddingsReducer extends Reducer<Text, BiddingsWritable, Text, BiddingsWritable> {
    @Override
    protected void reduce(Text key, Iterable<BiddingsWritable> values, Context context) throws IOException, InterruptedException {
        BiddingsWritable sum = new BiddingsWritable();
        for(BiddingsWritable bidding: values) {
            long visits = sum.getVisits().get() + bidding.getVisits().get();
            sum.setVisits(new LongWritable(visits));
            long spends = sum.getSpends().get() + bidding.getVisits().get();
            sum.setSpends(new LongWritable(spends));
        }
        context.write(key, sum);
    }
}
