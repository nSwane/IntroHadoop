import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Used for the second job to group the keys by country.
 * 
 * @author nawaouis
 *
 */
public class CountryComparator extends WritableComparator{

	public CountryComparator(){
	}
	
	public int compare(WritableComparable w1, WritableComparable w2){
		CountryFrequency cf1 = (CountryFrequency) w1;
		CountryFrequency cf2 = (CountryFrequency) w2;
		
		return cf1.getCountry().toString().compareTo(cf2.getCountry().toString());
	}
}
