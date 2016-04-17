import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by Vitaliy on 4/3/2016.
 */
public class Main {
    public static void main(String[] args) {
        Text qwe = new Text("qweqwe");
        qwe.set(new Text("qwe"));
        System.out.println(qwe.getBytes().length);


    }
}
