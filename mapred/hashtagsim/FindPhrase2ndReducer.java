package mapred.hashtagsim;

import java.io.IOException;
import java.util.Hashtable;
import java.util.Map;
import java.util.Map.Entry;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
/**
 *@input format secondWord firstWord and their frequency            "I        2; :2   ;  I:2"    // first one with leading space
 *                                                                  "like     3;I:5   ;I like:3"
 *                                                                  "need     1;I:5   ;I need:1"
 *                                                                  "hate     1;I:5   ;I hate:1"
 *                                                                  "I        3;like:3;like I:3"
 *
 *@output format phrase firstWord secondWord                        "I like:3   I:   5;like:3"
 *                                                                  "I need:1   I:   5;need:1"
 *                                                                  "I hate:1   I:   5;hate:1"
 *                                                                  "like I:3   like:3;I:5"   
 */
public class FindPhrase2ndReducer extends Reducer<Text, Text, Text, Text>{
    @Override
        protected void reduce(Text key, Iterable<Text> value, Context context)
        throws IOException, InterruptedException {
   
        Configuration conf = context.getConfiguration();
        long totalOccur = Long.parseLong(conf.get("totalOccur"));
        System.out.println("%%%%%%%%%%%%%%%%%%%%%% in find phrase 2nd reducer : totalOccur is "+totalOccur); 

        Integer secondWordCount = 0;Integer currSecondWordCount = 0;
        Double phrasePro,firstWordPro,secondWordPro;

        /*hadoop will clear iterable list, so buff them in an arraylist */
        ArrayList<String> inputBuffList = new ArrayList<String>(); 

        /*traverse and accumulate count to the second word*/
        for(Text t : value){
            inputBuffList.add(t.toString());
        }
    
        /*accumulate global count for secondWord*/
        for(String s : inputBuffList){
            currSecondWordCount = Integer.parseInt(s.split(";")[0]);
            secondWordCount += currSecondWordCount;
            // System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%"+currSecondWordCount);
        }
              
        String secondWord = key.toString();
 
        Hashtable<String,Integer> currFirstPhraseTable = new Hashtable<String,Integer>();
        ArrayList<Map.Entry<String,Integer>> currFirstPhraseList = new ArrayList<Map.Entry<String,Integer>>();
        String currFirstWord; Integer currFirstWordCount;
        String currPhrase;    Integer currPhraseCount;
        /*emit in the format: phrase:phrase count       firstword:firstwordcount  secondword:secondwordcount*/
        //System.out.println("before another iteration through value!\n\n\n\n\n\n\n");
        //System.out.println(value == null);
        for(String s : inputBuffList){
            //System.out.println("##############in the iteration, s from inputBuffList = "+s);
            String stream = s;

            currFirstPhraseTable.clear();
            KeyUtil.streamToCountHash(stream, currFirstPhraseTable);
 
            currFirstPhraseList.clear();           
            currFirstPhraseList.addAll(currFirstPhraseTable.entrySet());
           
            String firstKey = currFirstPhraseList.get(0).getKey();
            /*extract first word and phrase*/
            if(!firstKey.contains(" ")){
                currFirstWord     = firstKey;
                currFirstWordCount= currFirstPhraseList.get(0).getValue();
                currPhrase        = currFirstPhraseList.get(1).getKey();
                currPhraseCount   = currFirstPhraseList.get(1).getValue();    
            }else{
                currPhrase        = firstKey;
                currPhraseCount   = currFirstPhraseList.get(0).getValue();
                currFirstWord     = currFirstPhraseList.get(1).getKey();
                currFirstWordCount= currFirstPhraseList.get(1).getValue();     
            }
            /*some words begins with space because they are just padding, don't emit as real phrase*/
            if(currPhrase.charAt(0) == ' ') continue;
 
            phrasePro    = (double)currPhraseCount    / (double)totalOccur;
            firstWordPro = (double)currFirstWordCount / (double)totalOccur;
            secondWordPro= (double)secondWordCount    / (double)totalOccur;
            if(phrasePro/firstWordPro/secondWordPro  >= 1000 && phrasePro >= 0.00005)
                context.write(new Text(currPhrase + ":" + currPhraseCount.toString()), new Text(currFirstWord + ":" + currFirstWordCount.toString() + " | " +  secondWord +":"+ secondWordCount.toString()));    
    

            

	    }         
    
   }
}
