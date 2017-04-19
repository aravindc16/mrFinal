package predictionNewAC;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class sortHM {

}


class SortMap11 extends Mapper<LongWritable, Text, Text, Text>{
	String fileName;
	long lineno = 0;

	public void map(LongWritable _k, Text val, Context context) throws IOException, InterruptedException{

		context.write(new Text("Dummy"), val);


	}
}



class SortReducer11 extends Reducer <Text, Text, NullWritable, Text>{
	//	String fileName;
	LinkedList<String>  Lines;

	HashMap<String, String> HMValues;

	long lineno;



	public void setup(Context context) throws IOException, InterruptedException{

		Lines = new LinkedList<String>();
		HMValues = new HashMap<String, String>();
		lineno = 0;


	}

	public void reduce(Text k, Iterable<Text> val, Context context) throws IOException, InterruptedException{
		LongWritable k1 = null;
		Text v1 = null;

		for(Text val1 : val){

			String ip = val.toString();
			

			if(ip.contains(",")){
				String ip1[] = ip.split(",");
				lineno = lineno + 1;
				Lines.add(ip1[1]);

			}

			else{
				String ip1[] = ip.split("\t");
				HMValues.put(ip1[0],  ip);

			}
		}
		//context.write(k1, v1);
	}
	
	
	public void cleanup(Context context) throws IOException, InterruptedException{
		for(String s: Lines)

		{


		context.write(NullWritable.get(), new Text(HMValues.get(s)));


		} 


	}
}