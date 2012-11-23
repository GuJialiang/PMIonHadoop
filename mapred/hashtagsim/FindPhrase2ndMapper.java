package mapred.hashtagsim;

import java.io.IOException;
import java.util.List;
import java.util.Hashtable;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;

import mapred.util.Tokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
/**
 * @input format: firstWord prefixTable and their frequency:        "         2;  I:2"
 *                                                                  "I        5;I like:3;I need:1;I hate:1;
 *                                                                  "like     3;like I:3"
 *                                                                  "need     1;need  :1" // last one with tailing space
 *                                                                  "hate     1;hate  :1" // last one with tailing space
 *
 * @output format: secondWord, firstWord phrase and their frequency:"I        2; :2   ;  I:2"    // first one with leading space
 *                                                                  "like     3;I:5   ;I like:3"
 *                                                                  "need     1;I:5   ;I need:1"
 *                                                                  "hate     1;I:5   ;I hate:1"
 *                                                                  "I        3;like:3;like I:3"
*/                                                        
public class FindPhrase2ndMapper extends Mapper<LongWritable, Text, Text, Text>{
    @Override
        protected void map(LongWritable key, Text value,
                           Context context) throws IOException, InterruptedException{

	    Hashtable<String,Integer> prefixTable = new Hashtable<String,Integer>();
	    Integer firstWordCount = 0;
	    String line = value.toString();
        String stream = line.split("\t")[1];
	    firstWordCount = KeyUtil.streamToCountHash(stream,prefixTable);

        String currPhrase = null;
        Integer secondWordCount = 0;
        /*this table has only two entries in the format of X:Nx; XY:Nxy*/
        Hashtable<String,Integer> firstAndPhraseTable = new Hashtable<String,Integer>();
        /*every entry in the table will be parsed, the key will be split into firstWord and secondWord*/
        String[] firstSecondArr = null; String firstWord; String secondWord;
        String countHashStream = null; Integer secondOrPhraseCount = 0;
        for(Map.Entry<String,Integer> entry : prefixTable.entrySet()){
            currPhrase = entry.getKey();
            secondOrPhraseCount = entry.getValue();
            if(currPhrase.charAt(0) == ' '){//the padding space in the beginning , should emit
                firstWord = " ";
                secondWord = currPhrase.substring(2); 

            }else if(currPhrase.charAt(currPhrase.length()-1) == ' '){// the padding space in the end, don't emit
                continue;
            }else{
                firstSecondArr = currPhrase.split(" ");
                firstWord = firstSecondArr[0]; secondWord = firstSecondArr[1];
            }
            firstAndPhraseTable.clear();
            firstAndPhraseTable.put(firstWord,firstWordCount);
            firstAndPhraseTable.put(currPhrase,secondOrPhraseCount);
            
            countHashStream = KeyUtil.countHashToStream(secondOrPhraseCount,firstAndPhraseTable);
            context.write(new Text(secondWord), new Text(countHashStream));

        }
    }      
}
