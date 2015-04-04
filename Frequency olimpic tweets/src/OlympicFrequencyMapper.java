import java.io.IOException;
import java.sql.Time;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;



import org.apache.hadoop.mapreduce.Mapper;

import com.google.common.util.concurrent.SimpleTimeLimiter;

public class OlympicFrequencyMapper extends Mapper<Text, Olympic, Text, IntWritable> { 


	private Text mapKey =new Text();
	private IntWritable one =new IntWritable(1);

	public void map(Text key,  Olympic value, Context context) throws IOException, InterruptedException 
	{
		// Sets the key as morning,afternoon,evening or night depending on the time interval				
		int hr = Integer.parseInt(value.getTweetTimeHour());

		if (hr >= 6 && hr < 12) {
			mapKey.set("Morning");
		}
		else if (hr >= 12 && hr < 18) {
			mapKey.set("Afternoon");
		}
		else if (hr >= 18 && hr < 24) {
			mapKey.set("Evening");
		}
		else{			
			mapKey.set("Night");
		}
		//Sends the appropriate key 
		context.write(mapKey, one);		
	}

}
