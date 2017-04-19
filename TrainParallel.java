package predictionNewAC;
//package prediction;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.classifiers.evaluation.NominalPrediction;
import weka.classifiers.evaluation.NumericPrediction;
import weka.classifiers.trees.J48;
import weka.classifiers.trees.RandomForest;
import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instances;
import weka.core.SerializationHelper;
import weka.core.converters.ArffLoader;
import weka.core.converters.ArffSaver;
import weka.core.converters.CSVLoader;
import weka.core.converters.Loader;


public class TrainParallel {

}

class TrainParallelMapper extends Mapper<Object, Text, IntWritable, Text>{
	int k;
	String header;
	private Random rands = new Random();
	String fileName;
	int i = 0;
	public void setup(Context context) throws IOException, InterruptedException{
		k = context.getConfiguration().getInt("k", 1);

		context.getInputSplit();
		fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
		//System.out.println(fileName);

		header = context.getConfiguration().get("header");
		System.out.println("get header");
		System.out.println(header);

		//		for(int i=0; i<k; i++){
		//			context.write(new IntWritable(i), new Text(header));
		//		}

	}

	public void map(Object _key, Text val, Context context) throws IOException, InterruptedException{
		if(val.toString().split(",")[0].contains("?")){
			context.write(new IntWritable(999), val);
		}
		else{
			for(int i=0; i<k; i++){
				if(rands.nextDouble() <= 0.1)
					context.write(new IntWritable(i), val);
			}
		}
	}
}

class PreProcessPartitioner extends Partitioner<IntWritable, Text>{

	@Override
	public int getPartition(IntWritable key, Text value, int nrt) {
		// TODO Auto-generated method stub
		return key.get()%nrt;
	}
}

class TrainParallelReducer extends Reducer<IntWritable, Text, NullWritable, NullWritable>{
	PrintWriter pw ;
	StringBuilder sb;
	String filename;
	String previousFilename;
	List<String> files;
	String dir;

	public void setup(Context context) throws FileNotFoundException{

		filename = "";
		previousFilename = "";
		pw = new PrintWriter(new File(filename + ".csv"));
		sb = new StringBuilder();
		files = new ArrayList<String>();
		dir = context.getConfiguration().get("dir");
		System.out.println("IN TRAIN MAPPER"+dir);
	}

	public void reduce(IntWritable key, Iterable<Text> val, Context context) throws IOException, InterruptedException{
		if(!key.toString().equals(filename)){
			previousFilename = filename;
			filename = key.toString();
			pw.close();
			FileSystem fileSystem = FileSystem.get(URI.create(dir),context.getConfiguration());
			FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path(dir+filename + ".csv"));  
			pw = new PrintWriter(fsDataOutputStream);
			//			pw = new PrintWriter(new File(dir+filename + ".csv"));
			files.add(filename);		//Adding the filenames to the list so that converting to .arff is easier later.
			pw.println(context.getConfiguration().get("header"));
		}

		sb.append('\n');
		for(Text v : val){
			String line = v.toString();
			String[] split = line.split(",");
			if(line.contains("Agelaius") || split[0].contains("X")){

			}
			else{
				int ang = 0;
				String ans = "";

				if(!split[0].contains("?")){
					if(Integer.parseInt(split[0]) > 0){
						ang = 1;
					}

				}
				ans = ans + ang;

				for(int i=1; i< split.length; i++){

					ans = ans + "," + Test.Encode(split[i]) ;
					//					ans = ans + "," + Test.Encode(split[i]) ;

				}

				pw.println(ans);
			}
		}
		context.write(NullWritable.get(), NullWritable.get());
	}

	public void cleanup(Context context) throws IOException, InterruptedException{
		//		pw.write(sb.toString());
		pw.close();

		System.out.println("INSIDE THE CLEANUP ");
		for(String f : files){
			if(!f.contains("999")){
				FileSystem fileSystem = FileSystem.get(URI.create(dir),context.getConfiguration());
				FSDataInputStream fsDataInputStream = fileSystem.open(new Path(dir+f + ".csv"));  
				CSVLoader csv = new CSVLoader();
				csv.setSource(fsDataInputStream);
				Instances data = csv.getDataSet();

				FileSystem fs = FileSystem.get(URI.create(dir),context.getConfiguration());
				FSDataOutputStream fsDataOutputStream = fs.create(new Path(dir+f + ".arff"));  
				ArffSaver saver = new ArffSaver();
				saver.setInstances(data);
				PrintWriter writer = new PrintWriter(fsDataOutputStream);
				writer.println(data.toString());
				writer.close();

				//				File file = new File(dir + f+".arff");
				//				saver.setFile(file);


				//				saver.setFile(new File(dir+f + ".arff"));
				//				saver.setFile(o);
				//				saver.writeBatch();
				//				File file = new File(dir+f+".csv");
				//				file.delete();
				fsDataOutputStream.close();

				//				fileSystem =FileSystem.get(URI.create(dir),context.getConfiguration());
				fsDataInputStream = fileSystem.open(new Path(dir  + f +".arff"));
				ArffLoader ar = new ArffLoader();
				ar.setSource(new File(dir + f + ".arff"));
				ar.setSource(fsDataInputStream);
				ar.setRetrieval(Loader.BATCH);
				Instances dataSet = ar.getDataSet();
				dataSet.setClassIndex(0);

				// cross validation new

				Classifier cls = new RandomForest();
				Evaluation eval;
				try {
					eval = new Evaluation(dataSet);
					Random rand = new Random(1);  // using seed = 1
					int folds = 9;
					eval.crossValidateModel(cls, dataSet, folds, rand);
					System.out.println(eval.toSummaryString());
				} catch (Exception e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}


				// end cross validation’
				//Attribute to classify on.
				Attribute trainAtt = dataSet.attribute(0);
				dataSet.setClass(trainAtt);
				FileSystem f1 = FileSystem.get(URI.create(dir),context.getConfiguration());
				FSDataOutputStream fsDOS = f1.create(new Path(dir+f + ".model"));
				RandomForest classifier = new RandomForest();
				//				classifier.setNumFeatures(60);

				try {
					classifier.buildClassifier(dataSet);
					//					SerializationHelper.write(dir+f+".model", classifier);
					SerializationHelper.write(fsDOS, classifier);
					File arf = new File(dir+f + ".arff");
					arf.delete();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
}