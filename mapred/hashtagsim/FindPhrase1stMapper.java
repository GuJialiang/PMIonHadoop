package mapred.hashtagsim;

import java.io.IOException;
import java.util.List;
import java.util.Hashtable;
import java.util.HashSet;
import java.util.LinkedList;

import mapred.util.Tokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;


/**
 * @input  format: score and plain text Example: "I like I like I like I need"
 *                                               "I hate"
 * @output format: firstWord, prefixTable and their frequency, Example: "         1;  I:1"    // fist one with leading space
 *                                                                      "I        4;I like:3;I need:1"
 *                                                                      "like     3;like I:3"
 *                                                                      "need     1;need  :1" // last one with tailing space
 *                                                                      "         1;  I:1"
 *                                                                      "I        1;I hate:1"
 *                                                                      "hate     1;hate  :1" // last one with tailing space
 *                                                                      "   
*/
public class FindPhrase1stMapper extends Mapper<LongWritable, Text, Text, Text>{
    @Override
        protected void map(LongWritable key, Text value,
                           Context context) throws IOException, InterruptedException{
/*
        Configuration conf = context.getConfiguration();
        int N = Integer.parseInt(conf.get("N"));
*/
        String line = value.toString();
        Object[] words = KeyUtil.parseAndSanitizeOneLine(line);
        /*add a padding space to the tail*/
        String[] wordsWithPad = new String[words.length+1];
        for(int i = 0; i< words.length ; i++){
            wordsWithPad[i] = (String)(words[i]);
        }
        wordsWithPad[wordsWithPad.length-1] = " ";//the padding space

/*
        int score = Integer.parseInt((String)words[0]);
        String spaceValueString = "1 0 0 0 0 0";
*/
        /*empty tmp variable*/
    	String currentWord,currentPhrase;
    	StringBuffer sb = new StringBuffer();     
        Integer currentWordCount;String countHashString; 

        /*the table to store all the phrases sharing the same first word*/
        Hashtable<String,Integer> prefixTable = new Hashtable<String,Integer>();
  		Integer currentPhraseCount;
        HashSet<String> visitedWords = new HashSet<String>();

        /* scan the words in one review, the first is rating, last is padding space */
 	    for (int i = 1; i < wordsWithPad.length - 1; i++) {
	        currentWord = wordsWithPad[i];

            /*only visit different words*/
            if(visitedWords.contains(currentWord)) continue;
            else visitedWords.add(currentWord);
            currentWordCount = 0;
            prefixTable.clear();
            for(int j = i; j< wordsWithPad.length - 1; j++){
                if(wordsWithPad[j].equals(currentWord)){
                    currentWordCount++;

                    /*add current phrase to hashtable*/
                    currentPhrase = currentWord +" "+ wordsWithPad[j+1];
                    if(prefixTable.get(currentPhrase) == null){
                        prefixTable.put(currentPhrase,1);
                    }else{
                        currentPhraseCount = prefixTable.get(currentPhrase);
                        prefixTable.put(currentPhrase,currentPhraseCount+1);
                    }//if currentPhrase is in the table                
                }//if find another current word
            }//iterate j
            countHashString = KeyUtil.countHashToStream(currentWordCount,prefixTable);
            context.write(new Text(currentWord),new Text(countHashString));
        }//iterate i

        /* Emit a phrase of 'space and first word' in order to calculate the count of first word in the whole file
         * Will help in the FindPhrase2ndReducer
         */
        String space = " "; 
        String spaceAndFirstPhrase = space+" "+wordsWithPad[1];
        prefixTable.clear();
        prefixTable.put(spaceAndFirstPhrase,1);
        countHashString = KeyUtil.countHashToStream(1,prefixTable);
        context.write(new Text(space), new Text(countHashString));
        
    }      
}
