import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.w3c.dom.Text;

public class CountingIndexer {

	public static class WordWritable implements WritableComparable{
		
		private static class ChapFreq implements Comparable{
			
			private String chapter;
			private int frequency;
			
			public ChapFreq(String c, int f) {
				chapter = c;
				frequency = f;
			}
			
			@Override
			public String toString(){
				return "<"+chapter+", "+frequency+">";
			}
			
			//-1 if this has greater frequency than other, or equal frequency but lesser chapter
			//0 if they are the same frequency AND same chapter
			//1 if this has a lesser frequency, or equal frequency but greater chapter
			@Override
			public int compareTo(Object o) {
				
				ChapFreq other = (ChapFreq) o;
				
				if(this.frequency > other.frequency) {
					return -1;
				}
				
				if(this.frequency < other.frequency) {
					return 1;
				}
				
				String subThis = this.chapter.substring(4);
				String subOther = other.chapter.substring(4);
				
				int chapThis = Integer.parseInt(subThis);
				int chapOther = Integer.parseInt(subOther);
				
				if (chapThis > chapOther){ return 1; }
				if (chapThis < chapOther){ return -1; }
				
				// TODO Auto-generated method stub
				return 0;
			}
			
		}
		
		
	   private Hashtable<String, Integer> ht;
	   
	   public WordWritable() {
		   ht = new Hashtable<String, Integer>();
	   }
	   
	   @Override
	   public String toString(){
		   Set<Entry<String, Integer>> set = ht.entrySet();
		   
		   ArrayList<ChapFreq> list = new ArrayList<ChapFreq>();
		   
		   for(Entry<String,Integer> e : set) {
			   list.add(new ChapFreq(e.getKey(), e.getValue()));
		   }
		   
		   Collections.sort(list);
		   
		   String res ="\n";
		   for (ChapFreq c: list){
			   res += c+"\n";
		   }

		   return res;
		   //return ht.toString(); //TODO by order 
	   }
	   
	   @Override
	   public int compareTo(Object o){
		   return 0;
	   }
	   
	   public void addEntry(String s, int i) {
		   Integer prevVal = ht.remove(s);
		   
		   if(prevVal == null) {
			   prevVal = 0;
		   }
		   
		   ht.put(s,  prevVal + i);
		   
	   }
	   
	   public void addWW(WordWritable ww) {
		     Set<Entry<String, Integer>> set = ww.ht.entrySet();
		     
		     for(Entry<String, Integer> e : set) {
		    	 this.addEntry(e.getKey(),  e.getValue());
		     }		   
	   }
	   
	   public void write(DataOutput out) throws IOException {
	     //out.writeInt(counter);
	     //out.writeLong(timestamp);
	     int size = ht.size();
	     out.writeInt(size);
	     
	     if(size < 1) {
	    	 return;
	     }
	     
	     Set<Entry<String, Integer>> set = ht.entrySet();
	     
	     for(Entry<String, Integer> e : set) {
	    	 out.writeUTF(e.getKey());
	    	 out.writeInt(e.getValue());
	     }
	     
	     
	   }
	   
	   public void readFields(DataInput in) throws IOException {
	     //counter = in.readInt();
	     //timestamp = in.readLong();
		 int size = in.readInt();
		 
		 ht = new Hashtable<String, Integer>();
		 
		 for(int i = 0; i < size; i++) {
			 String string = in.readUTF();
			 Integer integer = in.readInt();
			 
			 ht.put(string,  integer);
		 }
		 
	   }
	   
	   public int compareTo(WordWritable o) {
	     //int thisValue = this.value;
	     //int thatValue = o.value;
	     return 0;//(thisValue < thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
	   }
	
	} 
	
	public static class TokenizerMapper extends Mapper<Object, Text, Text, WordWritable>{
		private WordWritable one;// = new WordWritable();
	    private Text word = new Text();

	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	        one = new WordWritable();
	    	one.addEntry(((FileSplit)context.getInputSplit()).getPath().getName(), 1);
	    	StringTokenizer itr = new StringTokenizer(value.toString(), " \t\n\r\f,.:;?![]'");
	    	while (itr.hasMoreTokens()) {
	    		word.set(itr.nextToken().toLowerCase()); //+" "+((FileSplit)context.getInputSplit()).getPath().getName()
	    		if (!word.toString().equals(""))
	    			context.write(word, one);
	      }
	    }
	}

    public static class IntSumReducer extends Reducer<Text,WordWritable,Text,WordWritable> {
    private WordWritable result = new WordWritable();

    public void reduce(Text key, Iterable<WordWritable> values, Context context) throws IOException, InterruptedException {
      //int sum = 0;
      for (WordWritable val : values) {
        //sum += val.get();
    	result.addWW(val);
      }
      //result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
	  
	  /* 
	   * word1
	   * <file-name1, occurrence-frequency1> 
	   * <file-name2, occurrence-frequency2>
	   */

	Configuration conf = new Configuration();
	Job job = Job.getInstance(conf, "Counting Indexer");
	job.setJarByClass(CountingIndexer.class);
	job.setMapperClass(TokenizerMapper.class);
	job.setCombinerClass(IntSumReducer.class);
	job.setReducerClass(IntSumReducer.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(WordWritable.class);
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}