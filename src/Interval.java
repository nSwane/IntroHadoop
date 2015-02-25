import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;


public class Interval implements WritableComparable<Interval> {
	private long min;
	private long max;
	
	public Interval(){
		
	}
	
	public Interval(long min, long max){
		this.min = min;
		this.max = max;
	}

	@Override
	public int compareTo(Interval i) {
		if(this.min > i.min){
			return  1;
		}
		else{
			if(this.min < i.min){
				return -1;
			}
			else{
				return 0;
			}
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.min = in.readLong();
		this.max = in.readLong();
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(this.min);
		out.writeLong(this.max);
	}
	
	public String toString(){
		return "["+min+", "+max+"]";
	}
}
