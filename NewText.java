package predictionNewAC;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class NewText implements WritableComparable<NewText> {

	  private Text value;

	  

	  public void NewText(Text first) {
	   this.value = first;
	  }

	
	
	  public Text getText() {
	    return value;
	  }

	  @Override
	  public void write(DataOutput out) throws IOException {
	    value.write(out);
	  }

	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int compareTo(NewText o) {
		// TODO Auto-generated method stub
		if(o.toString().contains("Agelaius_phoeniceus")){
			return -1;
		}
		return 0;
	}
}