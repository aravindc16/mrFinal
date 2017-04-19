package predictionNewAC;
//package prediction;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import weka.core.Instances;
import weka.core.converters.ArffSaver;
import weka.core.converters.CSVLoader;


public class PreProcessing {

}

class PreProcessingMapper extends Mapper<LongWritable, Text, NullWritable, Text>{
	private Random rands = new Random();
	//	int i;
	int k;
	MultipleOutputs mos;
	public void setup(Context ctx){
		//i = 0;
		k=ctx.getConfiguration().getInt("k", 1);
		mos = new MultipleOutputs(ctx);
	}

	public void map(LongWritable _k, Text val, Context context) throws IOException, InterruptedException{

		List<String> ip = Arrays.asList(val.toString().split(","));
		int size = ip.size();

		List<String> ip1 = ip.subList(0, 19);
		List<String> ip2 = ip.subList(26, 27);
		List<String> ip3 = ip.subList(955, 1015);
		List<String> ip4 = ip.subList(1018, size);


		List<String> listFinal = new ArrayList<String>();
		listFinal.addAll(ip2);
		listFinal.addAll(ip1);
		listFinal.addAll(ip3);
		listFinal.addAll(ip4);

		List<String> inputs5 = listFinal.subList(0, 60);
		
		
		List<String> ip11 = listFinal.subList(0, 6);
		List<String> ip22 = listFinal.subList(8,12);
		List<String> ip33 = listFinal.subList(15, 16);
		List<String> ip44 = listFinal.subList(17, 18);
		List<String> ip55 = listFinal.subList(19, 20);
		List<String> ip66 = listFinal.subList(21, 22);
		List<String> ip77 = listFinal.subList(28, 34);
		List<String> ip88 = listFinal.subList(80, 81);
		List<String> ip99 = listFinal.subList(89, 93);		
		List<String> ip110 = listFinal.subList(102, 106);		
		List<String> ip220 = listFinal.subList(115, 119);	
		List<String> ip330 = listFinal.subList(128, 132);	
		List<String> ip440 = listFinal.subList(137, 139);	
		List<String> ip550 = listFinal.subList(152, 157);	
		
		
		List<String> listFinal1 = new ArrayList<String>();
		listFinal1.addAll(ip11);
		listFinal1.addAll(ip22);
		listFinal1.addAll(ip33);
		listFinal1.addAll(ip44);
		listFinal1.addAll(ip55);
		listFinal1.addAll(ip66);
		listFinal1.addAll(ip77);
		listFinal1.addAll(ip88);
		listFinal1.addAll(ip99);
		listFinal1.addAll(ip110);
		listFinal1.addAll(ip220);
		listFinal1.addAll(ip330);
		listFinal1.addAll(ip440);
		listFinal1.addAll(ip550);
		

		String a = StringUtils.join(listFinal1, ",");
		
		Random r = new Random();
		int rand = r.nextInt((100)) ;
		
		if(listFinal1.get(0).contains("?")){
			
			mos.write(NullWritable.get(), new Text(a), "test");
		}
		else{
		
			mos.write(NullWritable.get(), new Text(a), "train");;
			
		}
		
		
	//	context.write(NullWritable.get(),new Text(a));
	}
	
	public void cleanup(Context context) throws IOException, InterruptedException{
		mos.close();
	}
}