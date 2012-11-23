package mapred.hashtagsim;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class nGramArrReducer extends Reducer<Text, Text, Text, Text>{
    @Override
        protected void reduce(Text key, Iterable<Text> value, Context context)
        throws IOException, InterruptedException {
        
    	long[] freqArr = new long[6];
    	
    	  for(Text t : value){
              /*split the input array into number array*/
              String[] afterSplit = t.toString().split(" ");
              for(int i = 0; i< afterSplit.length; i++){
                  if (Integer.parseInt(afterSplit[i]) != 0)
                      freqArr[i] += Integer.parseInt(afterSplit[i]);
              } 
          }
    	  StringBuffer sb = new StringBuffer();
    	  for(int i = 0 ; i<6;i++)
    		  sb.append(freqArr[i]+" ");
    	  context.write(key,new Text(sb.toString().trim()));  
    }
}
