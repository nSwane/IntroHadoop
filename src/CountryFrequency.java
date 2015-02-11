import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;


public class CountryFrequency implements Writable, WritableComparable<CountryFrequency> {
	
		private Text country;
		private long frequency;
	
		public CountryFrequency() {
			this.country = new Text(); // !!!!!!! POUR HADOOP
		}
	
		public CountryFrequency(String country, long frequency) {
			this.country = new Text(country);
			this.frequency = frequency;
		}
	
		public Text getCountry() {
			return country;
		}
	
		public void setCountry(Text country) {
			this.country = country;
		}
	
		public long getFrequency() {
			return frequency;
		}

		public void setFrequency(int frequency) {
			this.frequency = frequency;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			this.country.readFields(in);
			this.frequency = in.readLong();
	
		}
	
		@Override
		public void write(DataOutput out) throws IOException {
			this.country.write(out);
			out.writeLong(this.frequency);
		}
	
		@Override
		public int compareTo(CountryFrequency o) {
			if(this.country.toString().compareTo(o.getCountry().toString()) < 0)
				return -1;
			else
				if(this.country.toString().compareTo(o.getCountry().toString()) > 0)
					return 1;
				else
					if(this.frequency < o.getFrequency())
						return -1;
					else
						if(this.frequency > o.getFrequency())
							return 1;
						else
							return 0;
		}

	}
