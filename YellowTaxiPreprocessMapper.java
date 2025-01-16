import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class YellowTaxiPreprocessMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        /*
         group by PU location id
         keep tpep_pickup_datetime, passenger_count maybe to calculate averages later
         drop 1st column (is just csv ids), RatecodeID, fare_amount, extra, mta_tax, tolls_amount, improvement_surcharge, congestion_surcharge,Airport_fee as
         all is in total_fare but keep tip separately as well: might be useful to see what boroughs tip how much?,
         drop DO location ID, tpep_dropoff_datetime, vendorID, trip_distance (not required as drop-off doesn't matter), store_and_fwd_flag
         output will be (PU location id[8], Text with format: tpep_pickup_datetime[2], passenger_count[4], payment_type[10], tip_amount[14], total_amount[17])
         */
        // assume 0/null passenger count to be 1. Statistics will be affected.
        if(key.get() != 0){
            String puLocationID = "";
            String line = value.toString();
            String[] tokens = line.split(",");
            StringBuilder textLine = new StringBuilder("");
            // invalid negative amounts will be filtered out
            if(Float.parseFloat(tokens[17]) >= 0 && Float.parseFloat(tokens[14]) >= 0){
                for(int i=1; i< tokens.length; i++){
                    if(i==8) puLocationID = tokens[i].trim();
                    else{
                        if(i==2 || i==10 || i==14){
                            textLine.append(tokens[i].trim());
                            textLine.append(",");
                            continue;
                        }
                        if(i==4){
                            if(tokens[i].trim().equals("")){
                                textLine.append("1.0");
                            }
                            else{
                                textLine.append(tokens[i].trim());
                            }

                            textLine.append(",");
                            continue;
                        }
                        if(i==17){
                            textLine.append(tokens[i].trim());
                            continue;
                        }
                    }
                }

                context.write(new Text(puLocationID), new Text(textLine.toString()));
            }

        }

    }
}
