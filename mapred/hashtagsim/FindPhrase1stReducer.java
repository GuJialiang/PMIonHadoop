package mapred.hashtagsim;

import java.io.IOException;
import java.util.Hashtable;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * @input format firstWord, prefixTable and their frequency: "         1;  I:1"
 *                                                           "I        4;I like:3;I need:1"
 *                                                           "like     3;like I:3"
 *                                                           "need     1;need  :1" // last one with tailing space
 *                                                           "I        1;I hate:1"
 *                                                           "hate     1;hate  :1" // last one with tailing space

 * @output format merged input:                              "         2;  I:2"
 *                                                           "I        5;I like:3;I need:1;I hate:1;
 *                                                           "like     3;like I:3"
 *                                                           "need     1;need  :1" // last one with tailing space
 *                                                           "hate     1;hate  :1" // last one with tailing space
*/
 

public class FindPhrase1stReducer extends Reducer<Text, Text, Text, Text>{
    @Override
        protected void reduce(Text key, Iterable<Text> value, Context context)
        throws IOException, InterruptedException {
        
    	Hashtable<String,Integer> prefixTable = new Hashtable<String,Integer>();
        Integer wordCount = 0;
        String stream = null;

        Hashtable<String,Integer> currentPrefixTable = new Hashtable<String,Integer>();
        Integer currentWordCount = 0;

        for(Text t : value){
            stream = t.toString();
            currentPrefixTable.clear();
            currentWordCount = KeyUtil.streamToCountHash(stream,currentPrefixTable);
            wordCount += currentWordCount;
/*
            for(String s : currentPrefixTable.keySet()){
                if(s.equals("I were")){
                    System.out.println("##################################");
                    System.out.println("wordCount: "+ currentWordCount);
                    System.out.println(s+" : "+currentPrefixTable.get(s));
                    System.out.println("##################################");
                }
            }
*/                
            for(Map.Entry<String,Integer> entry : currentPrefixTable.entrySet()){
                if(prefixTable.get(entry.getKey()) == null)
                    prefixTable.put(entry.getKey(),entry.getValue());
                else{
                    Integer curr = prefixTable.get(entry.getKey());                    
                    prefixTable.put(entry.getKey(),entry.getValue()+curr);
                }
            } 
        }
        String outputStream = KeyUtil.countHashToStream(wordCount,prefixTable);
        context.write(key,new Text(outputStream));
    }
}
