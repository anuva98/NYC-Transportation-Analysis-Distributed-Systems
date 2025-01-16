import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class YellowTaxiPreprocessReducer
        extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        // reducer to combine all records
        StringBuilder combinedOutput = new StringBuilder();

        // Concatenate all values for this key
        for (Text value : values) {
            combinedOutput.append(value.toString()).append("\n");
        }

        // Write the combined output to the context
        context.write(key, new Text(combinedOutput.toString()));
    }
}

