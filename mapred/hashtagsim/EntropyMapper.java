package mapred.hashtagsim;

import java.io.IOException;
import java.util.List;
import java.util.LinkedList;

import mapred.util.Tokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EntropyMapper extends Mapper<LongWritable, Text, Text, Text>{
    @Override
        protected void map(LongWritable key, Text value,
                           Context context) throws IOException, InterruptedException{
        
        String line = value.toString();
        
        String[] keyWordPair = line.split("\t");
        String wordOrPhrase = keyWordPair[0];
        String arrString = keyWordPair[1];//in the format of "0 0 0 1 0 1"

        String[] valueArr = arrString.split(" ");
        long[] freqArr = new long[valueArr.length];
        for(int i = 0; i < freqArr.length ; i++)
            freqArr[i] = Integer.parseInt(valueArr[i]); 

        Double entropy = KeyUtil.computeEntropy(freqArr);

    	context.write(new Text(wordOrPhrase),new Text(entropy.toString()));
    }      
}
