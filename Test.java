package predictionNewAC;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;

import weka.attributeSelection.AttributeSelection;
import weka.attributeSelection.CfsSubsetEval;
import weka.attributeSelection.GreedyStepwise;
import weka.core.Instances;
import weka.core.Utils;
import weka.core.converters.ArffLoader;
import weka.core.converters.Loader;

public class Test {
	public static void main(String args[]) throws Exception{
		System.out.println(Encode("?"));
//		ArffLoader ar = new ArffLoader();
//		ar.setSource(new File("outReduce.arff"));
//		ar.setRetrieval(Loader.BATCH);
//		Instances data = ar.getDataSet();
//	    if (data.classIndex() == -1)
//	      data.setClassIndex(data.numAttributes() - 1);
//
//	    // 3. low-level
//	    useLowLevel(data);
		
	}
	
	public static void useLowLevel(Instances data) throws Exception {
	    System.out.println("\n3. Low-level");
	    AttributeSelection attsel = new AttributeSelection();
	    CfsSubsetEval eval = new CfsSubsetEval();
	    GreedyStepwise search = new GreedyStepwise();
	    search.setSearchBackwards(true);
	    attsel.setEvaluator(eval);
	    attsel.setSearch(search);
	    System.out.println("I am here.");
	    attsel.SelectAttributes(data);
	    
	    int[] indices = attsel.selectedAttributes();
	    System.out.println("selected attribute indices (starting with 0):\n" + Utils.arrayToString(indices));
	  }
	
	public static Double Encode(String s){
		if( isDouble(s)){
			return Double.parseDouble(s);
		}
//		if(s.trim().equals("?")){
//			return(s.trim());
//		}
		
		int size = s.length();
		double ascii=0;
		for(int i=0;i<size;i++){
			char c = s.charAt(i);
			ascii += (double)c;
		}
		return ascii;
	}



	public static boolean isDouble(String s) {
		try{
		double t = Double.parseDouble(s);
			return true;
		
		}catch (Exception E){
			return false;
		}
	}
}