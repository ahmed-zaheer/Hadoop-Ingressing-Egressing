import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class OlympicFrequencyReducer extends Reducer<Text, IntWritable, Text, IntWritable> 
{	
	
	IntWritable result = new IntWritable();
	

	public void reduce(Text key, Iterable<IntWritable> values, Context context)throws IOException, InterruptedException 
	{
		//Calculate the number of occurence for each key sent from the mapper
		int count = 0;
		for( IntWritable val : values)
		{
			count++;
		}

		result.set(count);
		//Sends the appropriate key with the total number of occurence
		context.write(key, result);
		
	}


}


