package cs181;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Word Count Mapper 
 * Receives lines of text, splits each line into words, and generates key, value pairs. Where 
 * the key is the word, and the value is just 1. The counts for a given key will be aggregated in the reducer. 
 *
 * @param  Raw text
 * @return < Key , 1 >
 * 
 */

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	    private final IntWritable one = new IntWritable(1);
	    private Text word = new Text();
	    private String pattern= "^[a-z][a-z0-9]*$";
	    private List<String> stopWords = Arrays.asList("a", "about", "above", "above", "across", "after", "afterwards", "again", 
	                        "against", "all", "almost", "alone", "along", "already", "also","although",
	                        "always","am","among", "amongst", "amoungst", "amount", "an", "and", "another", 
	                        "any","anyhow","anyone","anything","anyway", "anywhere", "are", "around", "as", 
	                        "at", "back","be","became", "because","become","becomes", "becoming", "been", 
	                        "before", "beforehand", "behind", "being", "below", "beside", "besides", "between", 
	                        "beyond", "bill", "both", "bottom","but", "by", "call", "can", "cannot", "cant", 
	                        "co", "con", "could", "couldnt", "cry", "de", "describe", "detail", "do", "done", 
	                        "down", "due", "during", "each", "eg", "eight", "either", "eleven","else", "elsewhere", 
	                        "empty", "enough", "etc", "even", "ever", "every", "everyone", "everything", 
	                        "everywhere", "except", "few", "fifteen", "fify", "fill", "find", "fire", "first", 
	                        "five", "for", "former", "formerly", "forty", "found", "four", "from", "front", 
	                        "full", "further", "get", "give", "go", "had", "has", "hasnt", "have", "he", "hence", 
	                        "her", "here", "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "him", 
	                        "himself", "his", "how", "however", "hundred", "ie", "if", "in", "inc", "indeed", "interest", 
	                        "into", "is", "it", "its", "itself", "keep", "last", "latter", "latterly", "least", "less", 
	                        "ltd", "made", "many", "may", "me", "meanwhile", "might", "mill", "mine", "more", "moreover", 
	                        "most", "mostly", "move", "much", "must", "my", "myself", "name", "namely", "neither", 
	                        "never", "nevertheless", "next", "nine", "no", "nobody", "none", "noone", "nor", "not", 
	                        "nothing", "now", "nowhere", "of", "off", "often", "on", "once", "one", "only", "onto", 
	                        "or", "other", "others", "otherwise", "our", "ours", "ourselves", "out", "over", "own",
	                        "part", "per", "perhaps", "please", "put", "rather", "re", "same", "see", "seem", 
	                        "seemed", "seeming", "seems", "serious", "several", "she", "should", "show", "side", 
	                        "since", "sincere", "six", "sixty", "so", "some", "somehow", "someone", "something", 
	                        "sometime", "sometimes", "somewhere", "still", "such", "system", "take", "ten", "than", 
	                        "that", "the", "their", "them", "themselves", "then", "thence", "there", "thereafter", 
	                        "thereby", "therefore", "therein", "thereupon", "these", "they", "thickv", "thin", "third", 
	                        "this", "those", "though", "three", "through", "throughout", "thru", "thus", "to", "together", 
	                        "too", "top", "toward", "towards", "twelve", "twenty", "two", "un", "under", "until", "up", 
	                        "upon", "us", "very", "via", "was", "we", "well", "were", "what", "whatever", "when", "whence", 
	                        "whenever", "where", "whereafter", "whereas", "whereby", "wherein", "whereupon", "wherever", 
	                        "whether", "which", "while", "whither", "who", "whoever", "whole", "whom", "whose", "why", 
	                        "will", "with", "within", "without", "would", "yet", "you", "your", "yours", "yourself", 
	                        "yourselves", "the");
	    
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        
	    	String line = value.toString();  /* get line of text from variable 'value' and convert to string */
	    	
	    	/* Lets use a string tokenizer to split line by words using a pattern matcher */
	        StringTokenizer tokenizer = new StringTokenizer(line); 
	        
	        while (tokenizer.hasMoreTokens()) {
	            word.set(tokenizer.nextToken());
	            String stringWord = word.toString().toLowerCase();
	            
	            /* for each word, output the word as the key, and value as 1 */
	            if (stringWord.matches(pattern) && !(stopWords.contains(stringWord))){
	                context.write(new Text(stringWord), one);
	            }
	            
	        }
	    }
	}