package mapred.hashtagsim;

import java.io.IOException;
import java.util.List;
import java.util.LinkedList;

import mapred.util.Tokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;

public class nGramArrMapper extends Mapper<LongWritable, Text, Text, Text>{
    @Override
        protected void map(LongWritable key, Text value,
                           Context context) throws IOException, InterruptedException{
        String line = value.toString();
        Object[] words = KeyUtil.parseAndSanitizeOneLine(line);

        int score = Integer.parseInt((String)words[0]);
        String spaceValueString = "1 0 0 0 0 0";
    	String currentWord,currentPhrase;
    	StringBuffer sb = new StringBuffer();      
        Configuration conf = context.getConfiguration();
        int N = Integer.parseInt(conf.get("N"));
 		/* scan the words in one review */
 	    for (int i = 1; i < words.length - N + 1; i++) {
			currentPhrase = "";
			/*clearing the stringbuffer */
    		sb.delete(0,sb.length());
	    	for (int j = i; j < i + N; j++)
				sb.append((String) words[j] + " ");
			currentPhrase = sb.toString().trim();

			long[] emptyArr = new long[6];
			emptyArr[score - 1]++;
			emptyArr[5]++; // increase the total count

			sb.delete(0,sb.length());
			for(int j = 0; j<6; j++){
					sb.append(emptyArr[j]+" ");
				}
			context.write(new Text(currentPhrase), new Text(sb.toString().trim()));			
		}
        context.write(new Text(" "),new Text(spaceValueString));
    }      
}
