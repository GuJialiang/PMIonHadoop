package mapred.ngramcount;

import java.io.IOException;

import mapred.util.Tokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NgramCountMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private static int count = 1;
    @Override
	protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = Tokenizer.tokenize(line);
        StringBuffer strBuf = new StringBuffer();

        System.out.println("printing in ngram");

        for(int i = 0; i < words.length - count + 1; i++){
            for(int j = 0; j < count; j++){
                strBuf.append(words[i + j] + " ");
            }
            if(strBuf.length() > 0){
                String ngram = strBuf.toString();
                context.write(new Text(ngram), NullWritable.get());
                strBuf.setLength(0);
            }
        }
    }

    public static void setNgramCount(int count){
        NgramCountMapper.count = count;
    }
}
