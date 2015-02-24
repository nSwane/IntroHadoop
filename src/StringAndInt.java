import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class StringAndInt implements WritableComparable<StringAndInt>, Writable {

	private Text tag;
	private int numberOcc;
	

	public StringAndInt() {
		this.tag = new Text();
		this.numberOcc = 0;
	}

	public StringAndInt(String tag, int numberOcc) {
		this.tag = new Text(tag);
		this.numberOcc = numberOcc;
	}

	public String getTag() {
		return tag.toString();
	}

	public void setTag(String tag) {
		this.tag = new Text(tag);
	}

	public int getNumberOcc() {
		return numberOcc;
	}

	public void setNumberOcc(int numberOcc) {
		this.numberOcc = numberOcc;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.tag.readFields(in);
		this.numberOcc = in.readInt();

	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.tag.write(out);
		out.writeInt(this.numberOcc);
	}

	@Override
	public int compareTo(StringAndInt o) {
		// We want descending order!! That's why multiply by (-1)
		int returnedT = this.tag.toString().compareTo(o.getTag().toString())*(-1);
		
		if(returnedT == 0){
			if(this.numberOcc < o.getNumberOcc()){
				return 1;
			}
			else{
				if(this.numberOcc > o.getNumberOcc()){
					return -1;
				}
				else{
					return 0;
				}
			}
		}
		else{
			return returnedT;
		}
	}

}
