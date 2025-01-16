import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class YellowTaxiPreprocessReducer
        extends Reducer<Text, Text, NullWritable, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        // reducer to combine all records
        StringBuilder combinedOutput = new StringBuilder();

        // each line has key, value pair
        for (Text value : values) {
            context.write(NullWritable.get(), new Text(key+ "," + value));
        }
    }
}

