import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Used for the second job to sort the keys by {Country, frequency}.
 * 
 * @author nawaouis
 *
 */
public class FrequencyComparator extends WritableComparator {

	public FrequencyComparator(){
		super(CountryFrequency.class, true);
	}
	
	public int compare(WritableComparable w1, WritableComparable w2){
		CountryFrequency cf1 = (CountryFrequency) w1;
		CountryFrequency cf2 = (CountryFrequency) w2;
		
		return cf1.compareTo(cf2);
	}
}
