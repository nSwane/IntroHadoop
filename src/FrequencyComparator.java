import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Used for the second job to sort the keys by frequency.
 * 
 * @author nawaouis
 *
 */
public class FrequencyComparator extends WritableComparator {

	public FrequencyComparator(){
		
	}
	
	public int compare(WritableComparable w1, WritableComparable w2){
		CountryFrequency cf1 = (CountryFrequency) w1;
		CountryFrequency cf2 = (CountryFrequency) w2;
		
		long f1 = cf1.getFrequency();
		long f2 = cf2.getFrequency();
		if(f1 < f2){
			return -1;
		}
		else{
			if(f1 > f2){
				return 1;
			}
			else{
				return 0;
			}
		}
	}
}
