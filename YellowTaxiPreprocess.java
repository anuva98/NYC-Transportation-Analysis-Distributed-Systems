import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class YellowTaxiPreprocess{

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: YellowTaxiPreprocess <input path directory> " +
                    "<output path>");
            System.exit(-1);
        }

        String inputPathBase = args[0];
        String outputPathBase = args[1];

            Job job = Job.getInstance();
            job.setJarByClass(YellowTaxiPreprocess.class);
            job.setJobName("Yellow Taxi Preprocessor");

            FileInputFormat.setInputPaths(job, new Path(inputPathBase));
            FileOutputFormat.setOutputPath(job, new Path(outputPathBase));

            job.setMapperClass(YellowTaxiPreprocessMapper.class);
            job.setCombinerClass(YellowTaxiPreprocessReducer.class);
            job.setReducerClass(YellowTaxiPreprocessReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);


            job.setNumReduceTasks(3);

            // Wait for job completion
            boolean success = job.waitForCompletion(true);

            // If job fails, exit with an error code
            if (!success) {
                System.exit(1);
            }
        System.exit(0);

    }
}
