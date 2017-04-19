package predictionNewAC;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.SequenceInputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Random;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.classifiers.trees.RandomForest;
import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.SerializationHelper;
import weka.core.converters.ArffLoader;
import weka.core.converters.ArffSaver;
import weka.core.converters.CSVLoader;
import weka.core.converters.CSVSaver;
import weka.core.converters.Loader;
import weka.filters.Filter;
import weka.filters.unsupervised.attribute.NumericToNominal;

public class PredictParallel {
	//	public static void main(String[] args) throws IOException{
	//
	//		ArffLoader testLoader = new ArffLoader();
	//		testLoader.setSource(new File("/Users/aravindchinta/Documents/workspace/mrFinal/data/999.arff"));
	//		testLoader.setRetrieval(Loader.BATCH);
	//		Instances testDataSet = testLoader.getDataSet();
	//
	//		Attribute testAttribute = testDataSet.attribute(0);
	//		testDataSet.setClassIndex(0);
	//
	//
	//		try {
	//			Classifier cls = (Classifier) weka.core.SerializationHelper.read("/Users/aravindchinta/Documents/workspace/mrFinal/data/0.model");
	////			NumericToNominal convert= new NumericToNominal();
	////		     String[] options= new String[2];
	////		     options[0]="-R";
	////		     options[1]="1";  //range of variables to make numeric
	////		     convert.setOptions(options);
	////		     convert.setInputFormat(testDataSet);
	////		     Instances labeled=Filter.useFilter(testDataSet, convert);
	//			Enumeration testInstances = testDataSet.enumerateInstances();
	//			FastVector predictions = new FastVector();
	//			while (testInstances.hasMoreElements()) {
	//				Instance instance = (Instance) testInstances.nextElement();
	//				double classification = Math.round(cls.classifyInstance(instance));
	//				instance.setClassValue(classification);
	//				//System.out.println(classification + " " + instance.value(1));
	//
	//			}
	//			Evaluation evaluation = new Evaluation(testDataSet);
	//			evaluation.evaluateModel(cls, testDataSet, new Object[] {});
	//
	//			System.out.println(evaluation.toSummaryString());
	//		} catch (Exception e) {
	//			e.printStackTrace();
	//		}
	//
	//		//		Evaluation evaluation = new Evaluation(trainDataSet);
	//		//		evaluation.evaluateModel(cls, testDataSet, new Object[] {});
	//
	//		CSVSaver predictedCsvSaver = new CSVSaver();
	//		predictedCsvSaver.setFile(new File("predict.csv"));
	//		predictedCsvSaver.setInstances(testDataSet);
	//		predictedCsvSaver.writeBatch();
	//
	//
	//	}
}

class PredictMapper extends Mapper<Object, Text, Text, DoubleWritable>{
	Classifier cls;
	List<Path> pList;
	String header;
	public void setup(Context c) throws IOException{
		pList = new ArrayList<Path>();
		URI[] u = c.getCacheFiles();	//Fetching the file from the DistributedCache
		for(int i=0;i<c.getConfiguration().getInt("k", 1);i++){
			pList.add(new Path(u[i]));
		}
		header = c.getConfiguration().get("header");

	}

	public void map(Object _k, Text val, Context context) throws IOException, InterruptedException{
		double sum = 0;
		int count = 0;
		//		System.out.println(val.toString());
		//		System.out.println(val.toString());
		//		if(!val.toString().startsWith("@")){
		InputStream h = new ByteArrayInputStream(header.getBytes("us-ascii"));
		InputStream l = new ByteArrayInputStream(val.toString().getBytes("us-ascii"));
		//		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		InputStream in = new SequenceInputStream(h, l);
		CSVLoader csv = new CSVLoader();
		csv.setSource(in);
		Instances ins = csv.getDataSet();
		Attribute testAttribute = ins.attribute(0);
		ins.setClassIndex(0);
		System.out.println("Size "+pList.size());
		for(Path p : pList){
			try {

				cls = (Classifier) weka.core.SerializationHelper.read(p.toString());

				Enumeration testInstances = ins.enumerateInstances();
				//FastVector predictions = new FastVector();
				//				while (testInstances.hasMoreElements()) {
				//					Instance instance = (Instance) testInstances.nextElement();
				double classification = cls.classifyInstance(ins.instance(0));
				ins.instance(0).setClassValue(classification);
				sum = sum + classification;

				//System.out.println(classification + " " + instance.value(1));
				//				}	
				count++;
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		context.write(new Text(val.toString().split(",")[1]),new DoubleWritable(sum/count));
		//		}
	}
}