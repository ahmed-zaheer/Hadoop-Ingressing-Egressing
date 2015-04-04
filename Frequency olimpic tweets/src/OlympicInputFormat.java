

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;


public class OlympicInputFormat extends FileInputFormat<Text,Olympic> {


	public RecordReader<Text, Olympic> createRecordReader(InputSplit inputSplit, TaskAttemptContext context) {
		return new OlympicRecordReader();
	}


	public class OlympicRecordReader extends RecordReader<Text,Olympic> {
	
		private CompressionCodecFactory compressionCodecs = null;
		private long start;
		private long pos;
		private long end;
		private LineReader in;
		private int maxLineLength;

		private Olympic value = null;
		private Text line = new Text();

		
		private Text id = new Text(); 
		private Text creatredAt = new Text();
		private DoubleWritable geoLocationLat = new DoubleWritable();
		private DoubleWritable geoLocationLong = new DoubleWritable();
		private Text placeInfo = new Text();		
		private Text source  = new Text();
		private Text lang = new Text();
		private Text screenName = new Text();
		private Text replyTo = new Text();
		private IntWritable rtCount = new IntWritable();
		private Text hashtags = new Text();	
		

		public void initialize(InputSplit genericSplit,	TaskAttemptContext context) throws IOException 
		{
			FileSplit split = (FileSplit) genericSplit;
			Configuration job = context.getConfiguration();
			this.maxLineLength = job.getInt(
					"mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
			start = split.getStart();
			end = start + split.getLength();
			final Path file = split.getPath();
			compressionCodecs = new CompressionCodecFactory(job);
			final CompressionCodec codec = compressionCodecs.getCodec(file);

			// open the file and seek to the start of the split
			FileSystem fs = file.getFileSystem(job);
			FSDataInputStream fileIn = fs.open(split.getPath());
			boolean skipFirstLine = false;
			if (codec != null) {
				in = new LineReader(codec.createInputStream(fileIn), job);
				end = Long.MAX_VALUE;
			} else 
			{
				if (start != 0) 
				{
					skipFirstLine = true;
					--start;
					fileIn.seek(start);
				}
				in = new LineReader(fileIn, job);
			}
			if (skipFirstLine) { // skip first line and re-establish "start".
				start += in.readLine(new Text(), 0,
						(int) Math.min((long) Integer.MAX_VALUE, end - start));
			}
			this.pos = start;
		}

		public boolean nextKeyValue() throws IOException {
			
			if (id == null) {
				id = new Text();
			}

			if (value == null) {
				value = new Olympic();
			}
			int newSize = 0;

			while (pos < end) {
				newSize = in.readLine(line, maxLineLength, Math.max(
						(int) Math.min(Integer.MAX_VALUE, end - pos),
						maxLineLength));
				if (newSize == 0) {
					break;
				}
				
				if(line.toString().endsWith("¿")) line.set(line.toString() + " ");				
				String[] fields = line.toString().split("¿");
							
				
				if(fields[0]!=null )   id.set(fields[0]);
				if(fields[1]!=null  )	lang.set(fields[1]);
				if(fields[2]!=null )   creatredAt.set(fields[2]);
				if(fields[3]!=null )   geoLocationLat.set(Double.parseDouble(fields[3]));
				if(fields[4]!=null  )	geoLocationLong.set(Double.parseDouble(fields[4]));				
				if(fields[5]!=null )	placeInfo.set(fields[5]);
				if(fields[6]!=null )	source.set(fields[6]);				
				if(fields[7]!=null  )	screenName.set(fields[7]);
				if(fields[8]!=null )	replyTo.set(fields[8]);
				if(fields[9]!=null )	rtCount.set(Integer.parseInt(fields[9].toString()));
				if(fields[10]!=null  )	hashtags.set(fields[10]);
					
				value.set(id, creatredAt, geoLocationLat, geoLocationLong, placeInfo, source, lang, screenName, replyTo, rtCount, hashtags);
					
				pos += newSize;
				if (newSize < maxLineLength) {
					break;
				}

				}
			if (newSize == 0) {
				id = null;
				value = null;
				return false;
			} else {
				return true;
			}
		}

		@Override
		public Text getCurrentKey() {
			return id;
		}

		@Override
		public Olympic getCurrentValue() {
			return value;
		}

		
		public float getProgress() {
			if (start == end) {
				return 0.0f;
			} else {
				return Math.min(1.0f, (pos - start) / (float) (end - start));
			}
		}

		public synchronized void close() throws IOException {
			if (in != null) {
				in.close();
			}
		}
	}
	
	@Override
	  protected boolean isSplitable(JobContext context, Path file) {
	    CompressionCodec codec = 
	      new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
	    return codec == null;
	  }

}
