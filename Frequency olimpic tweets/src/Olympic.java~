
import java.io.*;
import org.apache.hadoop.io.*;

public class Olympic implements WritableComparable<Olympic> {
	private Text id; 
	private Text creatredAt;
	private DoubleWritable geoLocationLat;
	private DoubleWritable geoLocationLong;
	private Text placeInfo;
	private Text source;
	private Text lang;
	private Text screenName;
	private Text replyTo;
	private IntWritable rtCount;
	private Text hashtags;

	public Olympic() {

		set(new Text(),new Text(),new DoubleWritable(),new DoubleWritable(),new Text(),new Text(),new Text(),new Text(),new Text(),new IntWritable(),new Text());
	}


	public void set(Text id,Text creatredAt, DoubleWritable geoLocationLat, DoubleWritable geoLocationLong, Text placeInfo, 
					Text source, Text lang,Text screenName,Text replyTo, IntWritable rtCount,Text hashtags) 
	{
		this.id=id;
		this.creatredAt = creatredAt;
		this.geoLocationLat = geoLocationLat;
		this.geoLocationLong = geoLocationLong;
		this.placeInfo = placeInfo;
		this.source = source;
		this.lang = lang;
		this.screenName=screenName;
		this.replyTo=replyTo;
		this.rtCount=rtCount;
		this.hashtags=hashtags;

	}

	public Text getId() {
		return id;
	}



	public Text getCreatredAt() {
		return creatredAt;
	}


	public String getTweetDate() {		
		return creatredAt.toString().split(",")[0];
	}

	public String getTweetTime() {
		return creatredAt.toString().split(",")[1];
	}


	public String getTweetTimeHour() {
		return getTweetTime().split(":")[0].trim();
	}

	public DoubleWritable getGeoLocationLat() {
		return geoLocationLat;
	}

	public DoubleWritable getGeoLocationLong() {
		return geoLocationLong;
	}

	public Text getPlaceInfo() {
		return placeInfo;
	}



	public Text getSource() {
		return source;
	}


	public Text getLang() {
		return lang;
	}


	public Text getScreenName() {
		return screenName;
	}

	public void setScreenName(Text screenName) {
		this.screenName = screenName;
	}

	public Text getReplyTo() {
		return replyTo;
	}


	public IntWritable getRtCount() {
		return rtCount;
	}

	public Text getHashtags() {
		return hashtags;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		id.write(out);
		creatredAt.write(out);
		geoLocationLat.write(out);
		geoLocationLong.write(out);
		placeInfo.write(out);
		source.write(out);
		lang.write(out);
		screenName.write(out);
		replyTo.write(out);
		rtCount.write(out);
		hashtags.write(out);

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		id.readFields(in);
		creatredAt.readFields(in);
		geoLocationLat.readFields(in);
		geoLocationLong.readFields(in);
		placeInfo.readFields(in);
		source.readFields(in);
		lang.readFields(in);
		screenName.readFields(in);

		replyTo.readFields(in);
		rtCount.readFields(in);
		hashtags.readFields(in);

	}

	@Override
	public int compareTo(Olympic o) {

		return id.compareTo(o.getId());				

	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((creatredAt == null) ? 0 : creatredAt.hashCode());
		result = prime * result + ((geoLocationLat == null) ? 0 : geoLocationLat.hashCode());
		result = prime * result + ((geoLocationLong == null) ? 0 : geoLocationLong.hashCode());
		result = prime * result + ((placeInfo == null) ? 0 : placeInfo.hashCode());		
		result = prime * result + ((source == null) ? 0 : source.hashCode());
		result = prime * result + ((lang == null) ? 0 : lang.hashCode());
		result = prime * result + ((screenName == null) ? 0 : screenName.hashCode());
		result = prime * result + ((replyTo == null) ? 0 : replyTo.hashCode());
		result = prime * result + ((rtCount == null) ? 0 : rtCount.hashCode());
		result = prime * result + ((hashtags == null) ? 0 : hashtags.hashCode());
		return result;
	}


}
