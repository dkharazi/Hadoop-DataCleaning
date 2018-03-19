import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DataClean1 {

        public static class TokenizerMapper
                         extends Mapper<Object, Text, Text, Text>{

                private Text dateKey = new Text();
		private Text remEntryVal = new Text();

                public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String[] dataEntries = value.toString().split("\n");

			for (String str: dataEntries) {
				String dateStr = str.substring(0,7);
				String remStr = str.substring(7);
				dateKey.set(dateStr);
				remEntryVal.set(remStr);
				if (dateKey.charAt(0) != 'D') {
					context.write(dateKey, remEntryVal);
				}
			}
                }
        }

	public static class IntSumReducer
                         extends Reducer<Text,Text,Text,Text> {

		private int[] daysPerMonth = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

                public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			// Sort values
			ArrayList<String> valList = new ArrayList<String>();
			for (Text val :values) {
				valList.add(val.toString());
			}
			Collections.sort(valList);

			// Find day in month
			Text keyEmit = new Text();
			Text valEmit = new Text();
			int monthIndex = Integer.parseInt(key.toString().substring(5, 7)) - 1;
			int numDaysPerMonth = daysPerMonth[monthIndex];

			// Find missing days
			String prevLine = valList.get(0);
			for (int dayIndex = 1; dayIndex < valList.size(); dayIndex++) {
				Integer curEntryDay = Integer.parseInt(valList.get(dayIndex).substring(1,3));
				Integer prevEntryDay = Integer.parseInt(prevLine.substring(1,3));

				if (prevEntryDay + 1 == curEntryDay) {
					keyEmit.set(key);
					valEmit.set(key + prevLine);
					context.write(keyEmit, valEmit);
				} else {
					while (prevEntryDay != curEntryDay && prevEntryDay < numDaysPerMonth) {
						String prevDay = prevEntryDay.toString();
						if (prevEntryDay < 10) {
							prevDay = "0" + prevEntryDay.toString();
						}
						keyEmit.set(key);
						valEmit.set(key.toString() + "-" + prevDay + prevLine.substring(3));
						context.write(keyEmit, valEmit);
						prevEntryDay++;
					}
				}
				prevLine = valList.get(dayIndex);
			}

			// Fill in final day
                        prevLine = key.toString() + "-" + Integer.parseInt(prevLine.substring(1,3)) + prevLine.substring(3);
                        keyEmit.set(key);
                        valEmit.set(prevLine);
                        context.write(keyEmit, valEmit);
                }
        }

	public static void main(String[] args) throws Exception {
                Configuration conf = new Configuration();
                Job job = Job.getInstance(conf, "data clean");

                // Specifying the class that contains the Mapper and Reducer.
                //
                job.setJarByClass(DataClean1.class);

                // Specifying the classes that implement Map and Reduce.
                //
                job.setMapperClass(TokenizerMapper.class);
                job.setReducerClass(IntSumReducer.class);

                // Specifying the type of the input and the output.
                //
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);

                // Setting input and output paths, starting the job,
                // and waiting for the job to finish.
                //
                FileInputFormat.addInputPath(job, new Path(args[0]));
                FileOutputFormat.setOutputPath(job, new Path(args[1]));
                System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
}

