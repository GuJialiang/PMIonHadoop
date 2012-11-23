package mapred.hashtagsim;

import java.io.IOException;
import java.util.List;
import java.util.LinkedList;

import mapred.util.Tokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordArrMapper extends Mapper<LongWritable, Text, Text, Text>{
    @Override
        protected void map(LongWritable key, Text value,
                           Context context) throws IOException, InterruptedException{
        String line = value.toString();
//        String[] words = line.split(" ");
        Object[] words = KeyUtil.parseAndSanitizeOneLine(line);
	    Integer numWords = words.length;
        String spaceValueString = numWords.toString()+" 0 0 0 0 0";

       int score = Integer.parseInt((String)words[0]);

    	String currentWord;
    	StringBuffer sb = new StringBuffer();
		/* scan the words in one review */
		for (int i = 1; i < words.length; i++) {
			if(((String)words[i]).trim().length() == 0) continue;
            sb.delete(0,sb.length());
			currentWord = (String) words[i];
				long[] emptyArr = new long[6];
				emptyArr[score - 1]++;
				emptyArr[5]++; // increase total count
				for(int j = 0; j<6; j++){
					sb.append(emptyArr[j]+" ");
				}
			context.write(new Text(currentWord), new Text(sb.toString().trim()));
		}
	context.write(new Text(" "),new Text(spaceValueString));
    }      
}
