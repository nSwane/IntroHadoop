import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class CountryTag implements Writable, WritableComparable<CountryTag> {

	private Text country;
	private Text tag;
	

	public CountryTag() {
		this.country = new Text();
		this.tag = new Text();
	}

	public CountryTag(String country, String tag) {
		this.country = new Text(country);
		this.tag = new Text(tag);
	}
	
	public Text getCountry() {
		return country;
	}

	public void setCountry(Text country) {
		this.country = country;
	}
	
	public void setTag(Text tag) {
		this.tag = tag;
	}

	public String getTag() {
		return tag.toString();
	}

	public void setTag(String tag) {
		this.tag = new Text(tag);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.country.readFields(in);
		this.tag.readFields(in);

	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.country.write(out);
		this.tag.write(out);
	}

	@Override
	public int compareTo(CountryTag o) {
		
		// We want descending order!! That's why multiply by (-1)
		int returnedC = this.country.toString().compareTo(o.getCountry().toString())*(-1);		
		
		if(returnedC == 0){
			int returnedT = this.tag.toString().compareTo(o.getTag().toString())*(-1);
			return returnedT;
		}
		else{
			return returnedC;
		}
	}
	
	public String toString(){
		return this.country.toString()+" "+this.tag.toString();
	}

}
