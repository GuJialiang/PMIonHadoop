package mapred.hashtagsim;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Map;
import java.util.Map.Entry;

public class KeyUtil{
	public static Object[] parseAndSanitizeOneLine(String oneLine) {

        oneLine = oneLine.replaceAll(";","");
        oneLine = oneLine.replaceAll(":","");

		String[] rawWords = oneLine.split(" ");

		ArrayList<String> list = new ArrayList<String>(Arrays.asList(rawWords));
		/* sanitize the words, remove empty strings */
		String original;
		String afterTrim;
		for (int i = 0; i < list.size();) {
			original = list.get(i);
			afterTrim = original.trim();
			/* if this word is empty, remove */
			if (afterTrim.length() == 0)
				list.remove(i);
			/* trim the leading and trailing space */
			else {
				list.set(i, afterTrim);
				i++;
			}
		}

		Object[] words = list.toArray();
		return words;
	}

	/* caculate the discriminant power of a word or phrase */
	public static double computeEntropy(long[] frequencyArr) {
		double entropy = 0.0;
		long total = 0;
		/* calculate total */
		for (int i = 0; i < frequencyArr.length; i++) {
			frequencyArr[i]++;// increase each count by one to eliminate rare
								// words
			if (i != 2)
				total += frequencyArr[i];
		}
		entropy = (double) (frequencyArr[3] + frequencyArr[4]
				- frequencyArr[0] - frequencyArr[1])
				/ (double) total;

		return entropy;
	}

    /*in FindPhrase1stMapper, stream the count and hashtable to a string*/
	public static String countHashToStream(Integer currentWordCount,Hashtable<String,Integer> prefixTable){
		StringBuffer sb = new StringBuffer();
		sb.append(currentWordCount);
		for(Map.Entry<String, Integer> entry : prefixTable.entrySet()){
			sb.append(";"+entry.getKey()+":"+entry.getValue());
		}
		return sb.toString();
	}

    /*return the count of key, build the hashtable from stream*/
	public static Integer streamToCountHash(String stream, Hashtable<String,Integer> prefixTable){
		String[] splitSemicolon = stream.split(";");
		Integer ret = Integer.parseInt(splitSemicolon[0]);
		//System.out.println("in helper: count = "+ countPointer[0]);
		String currPhrase;
		Integer currCount;
		for(int i=1 ; i<splitSemicolon.length; i++){
			String[] pair = splitSemicolon[i].split(":");
			currPhrase = pair[0];
			currCount = Integer.parseInt(pair[1]);
			prefixTable.put(currPhrase, currCount);
		}
		return ret;
	}
}
