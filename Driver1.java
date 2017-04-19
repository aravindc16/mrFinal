package predictionNewAC;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

public class Driver1 {
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException{
		Configuration conf = new Configuration();
		int k =2;
//		PreProcess(conf, args[0],args[2]);
		Train(conf, k, args[2]);
		Predict(conf,k, args[1],args[2]);
		sort(conf, args[2],args[1]);
	}

	public static void PreProcess(Configuration conf, String input, String preset) throws IOException, ClassNotFoundException, InterruptedException{
		Job job = new Job(conf, "Bird Prediction");
		job.setJarByClass(Driver1.class);
		job.setNumReduceTasks(0);
		job.setMapperClass(PreProcessingMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		// multiple input
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job,new Path(preset + "/out"));
		MultipleOutputs.addNamedOutput(job, "train", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "test", TextOutputFormat.class, NullWritable.class, Text.class);
		job.waitForCompletion(true);
	}


	public static String getHeader(String preset, Configuration conf) throws IOException{
		String addline = "";
		FileSystem fileSystem = FileSystem.get(URI.create(preset),conf);
		FSDataInputStream fsDataInputStream = fileSystem.open(new Path(preset + "/out/" + "train-m-00000"));  
		return fsDataInputStream.readLine();
	}

	public static void Train(Configuration conf, int k, String preset) throws IOException, ClassNotFoundException, InterruptedException{
		conf.setInt("k", k);
		conf.set("dir", preset + "/trainOut/");
		conf.set("header", getHeader(preset,conf));
		Job job = new Job(conf, "Train");
		job.setJarByClass(Driver1.class);
		job.setNumReduceTasks(k);
		job.setPartitionerClass(PreProcessPartitioner.class);
		job.setMapperClass(TrainParallelMapper.class);
		job.setReducerClass(TrainParallelReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(NullWritable.class);
		//		FileInputFormat.addInputPath(job, new Path(preset +"/out"));
		FileInputFormat.addInputPath(job, new Path(preset + "/out/t*"));
		FileOutputFormat.setOutputPath(job, new Path("trainOut"));
		job.waitForCompletion(true);
	}

	public static void Predict(Configuration conf, int k, String output,String preset) throws IOException, ClassNotFoundException, InterruptedException{
		conf.setInt("k", k);
		Job job = new Job(conf, "Predict");
		job.setJarByClass(Driver1.class);
		for(int i=0;i<k;i++){
			job.addCacheFile(new Path(preset + "/trainOut/"+i+".model").toUri());
		}
		job.setNumReduceTasks(0);
		job.setMapperClass(PredictMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(preset + "/trainOut/999.csv"));
		FileOutputFormat.setOutputPath(job, new Path(preset + "/predict"));
		job.waitForCompletion(true);
	}


	public static void sort(Configuration conf, String preset, String output) throws IOException, ClassNotFoundException, InterruptedException{
		Job job = new Job(conf, "Bird Prediction");
		job.setJarByClass(Driver1.class);
		job.setNumReduceTasks(1);
		job.setMapperClass(SortMap11.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		//
		//job.setReducerClass(Reducer.class);
		job.setReducerClass(SortReducer11.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		// multiple input
		MultipleInputs.addInputPath(job, new Path(preset + "/trainOut/999.csv"), TextInputFormat.class, SortMap11.class);
		MultipleInputs.addInputPath(job, new Path(preset + "/predict"), TextInputFormat.class, SortMap11.class);
		//FileInputFormat.addInputPath(job, new Path(input));

		FileOutputFormat.setOutputPath(job,new Path(output));


		//FileOutputFormat.setOutputPath(job, new Path("out"));
		job.waitForCompletion(true);
	}
}

