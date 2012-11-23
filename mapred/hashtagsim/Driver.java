package mapred.hashtagsim;

import java.io.IOException;
import mapred.job.Optimizedjob;
import mapred.util.FileUtil;
import mapred.util.SimpleParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;


public class Driver {

    static private Long totalOccur;

    public static void main(String args[]) throws Exception {
        SimpleParser parser = new SimpleParser(args);

        String input = parser.get("input");
        String wordTable = parser.get("wordTable");
        String gramTable = parser.get("gramTable");
        String wordEntropyList = parser.get("wordEntropyList");
        String gramEntropyList = parser.get("gramEntropyList");
        String findPhrase1st = parser.get("findPhrase1st");
        String findPhrase2nd = parser.get("findPhrase2nd");

        System.out.println("input:"+input);
        System.out.println("wordEntropyList:"+wordEntropyList);
        System.out.println("wordTable:"+wordTable);   
        System.out.println("gramTable: "+ gramTable);
        
        
        //System.exit(1);
        //System.out.println("in hashtagsim Driver\n\n\n\n\n\n\n\n\n");
   //     String output = parser.get("output");
   //     String tmpdir = parser.get("tmpdir");

        getWordArr(input,wordTable);
        


        /*read the output of wordTable, the first line will be total occurance of words in the input file*/
        try{
            BufferedReader br = new BufferedReader(new FileReader(wordTable+"/part-r-00000"));
            System.out.println("##########read path is : "+ wordTable+"/part-r-00000");
            String original = br.readLine();
            String[] afterSplitT     = original.split("\t");
            String[] afterSplitSpace = afterSplitT[1].split(" ");
            String answer = afterSplitSpace[0];
            Long l = Long.parseLong(answer);
            totalOccur = l;
            System.out.println("##########totalOccur has been set to : "+totalOccur);
            //System.exit(1);
        }catch(FileNotFoundException e){
            e.printStackTrace();
            System.out.println("##########wordTableNotFound");
        }


        getFindPhrase1st(input,findPhrase1st);
        getFindPhrase2nd(findPhrase1st,findPhrase2nd);

        //getEntropy(wordTable,wordEntropyList);
        //getNGramArr(input,gramTable,3);
        //getEntropy(gramTable,gramEntropyList);
        
        //getInvertFeatureVector(input, output);
        //computePairwiseSimilarities(tmpdir, output);
    }


    private static void getInvertFeatureVector(String input, String output)
        throws Exception{
        Optimizedjob job = new Optimizedjob(new Configuration(), input, output, 
                                                "Get invert feature vector for all words");
        job.setClasses(InvertMapper.class, InvertReducer.class, null);
        job.setMapOutputClasses(Text.class, Text.class);

        //System.out.println("before invert job runing \n\n\n\n\n\n\n\n\n\n");
        job.run();
    }

    
    private static void getWordArr(String input, String output)
        throws Exception{
        Optimizedjob job = new Optimizedjob(new Configuration(), input, output, 
                                                "Get word array in the format of word 0|0|0|1|0|1");
        job.setClasses(WordArrMapper.class, WordArrReducer.class, null);
        job.setMapOutputClasses(Text.class, Text.class);
        job.run();
    }

     private static void getPhraseArr(String input, String output)
        throws Exception{
        Optimizedjob job = new Optimizedjob(new Configuration(), input, output, 
                                                "Get phrase array in the format of word 0|0|0|1|0|1");
        job.setClasses(Phrase2ArrMapper.class, Phrase2ArrReducer.class, null);
        job.setMapOutputClasses(Text.class, Text.class);
        job.run();
    }

     private static void getNGramArr(String input, String output,Integer N)
        throws Exception{
        Configuration conf = new Configuration();
        conf.set("N",N.toString());
        Optimizedjob job = new Optimizedjob(conf, input, output, 
                                                "Get phrase array with a third argument N" );
        job.setClasses(nGramArrMapper.class, nGramArrReducer.class, null);
        job.setMapOutputClasses(Text.class, Text.class);
        job.run();
    }

     private static void getEntropy(String input, String output)
        throws Exception{
        Optimizedjob job = new Optimizedjob(new Configuration(), input, output, 
                                                "Compute Entropy, output in the format of 'word  entropy'");
        job.setClasses(EntropyMapper.class, null, null);
        job.setMapOutputClasses(Text.class, Text.class);
        job.run();
    }

     private static void getFindPhrase1st(String input, String output)
        throws Exception{
        Optimizedjob job = new Optimizedjob(new Configuration(), input, output, 
                                                "find phrase 1st stage");
        job.setClasses(FindPhrase1stMapper.class, FindPhrase1stReducer.class, null);
        //job.setClasses(FindPhrase1stMapper.class, null, null);
        job.setMapOutputClasses(Text.class, Text.class);
        job.run();
    }

     private static void getFindPhrase2nd(String input, String output)
        throws Exception{

        Configuration conf = new Configuration();
        conf.set("totalOccur",totalOccur.toString());

        Optimizedjob job = new Optimizedjob(conf, input, output, 
                                                "find phrase 2nd stage");
        job.setClasses(FindPhrase2ndMapper.class, FindPhrase2ndReducer.class, null);
        //job.setClasses(FindPhrase2ndMapper.class, null, null);
        job.setMapOutputClasses(Text.class, Text.class);
        job.run();
    }






    private static void computePairwiseSimilarities(String input, String output) throws IOException, ClassNotFoundException, InterruptedException{
        Configuration conf = new Configuration();
        
        conf.setInt("dfs.block.size",327680);
        conf.setInt("mapred.max.split.size",327680);
        conf.setInt("mapred.min.split.size",327680);
        conf.setInt("mapred.map.tasks",16);
        
        //conf.setInt("mapred.job.reuse.jvm.num.tasks",10);
        Optimizedjob job = new Optimizedjob(conf, input, output, "generate pairwise similarity");
        job.setClasses(PairwiseSimilarityMapper.class, PairwiseSimilarityReducer.class, PairwiseSimilarityCombiner.class);
        job.setMapOutputClasses(Text.class, IntWritable.class);
        job.run();
    }
}
