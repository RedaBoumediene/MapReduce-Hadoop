import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class Pair implements WritableComparable<Pair> {
	Text first , second;
	
	public Pair() {
		first = new Text();
		second = new Text();
	}

	public Pair(String key1, String key2) {
		this.first = new Text(key1);
		this.second = new Text(key2);
	}
	
	@Override
	public boolean equals(Object b) {
		Pair p = (Pair) b;
		return p.first.toString().equals(this.first.toString()) && p.second.toString().equals(this.second.toString());
	}
	@Override
	public int hashCode() {
		return first.hashCode()+second.hashCode();
	}
	@Override
	public void readFields(DataInput arg0) throws IOException {
		first.readFields(arg0);
		second.readFields(arg0);
		
	}
	@Override
	public void write(DataOutput arg0) throws IOException {
		first.write(arg0);
		second.write(arg0);
	}
	@Override
	public int compareTo(Pair o) {
		
		int k = this.first.toString().compareTo(o.first.toString());

		if (k != 0) 
			return k;
		else
			return this.second.toString().compareTo(o.second.toString());

	}
}
