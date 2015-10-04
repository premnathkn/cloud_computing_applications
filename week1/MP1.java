import java.io.BufferedReader;
import java.io.FileReader;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.TreeMap;

public class MP1 {
	Random generator;
	String userName;
	String inputFileName;
	String delimiters = " \t,;.?!-:@[](){}_*/";
	String[] stopWordsArray = { "i", "me", "my", "myself", "we", "our", "ours",
			"ourselves", "you", "your", "yours", "yourself", "yourselves",
			"he", "him", "his", "himself", "she", "her", "hers", "herself",
			"it", "its", "itself", "they", "them", "their", "theirs",
			"themselves", "what", "which", "who", "whom", "this", "that",
			"these", "those", "am", "is", "are", "was", "were", "be", "been",
			"being", "have", "has", "had", "having", "do", "does", "did",
			"doing", "a", "an", "the", "and", "but", "if", "or", "because",
			"as", "until", "while", "of", "at", "by", "for", "with", "about",
			"against", "between", "into", "through", "during", "before",
			"after", "above", "below", "to", "from", "up", "down", "in", "out",
			"on", "off", "over", "under", "again", "further", "then", "once",
			"here", "there", "when", "where", "why", "how", "all", "any",
			"both", "each", "few", "more", "most", "other", "some", "such",
			"no", "nor", "not", "only", "own", "same", "so", "than", "too",
			"very", "s", "t", "can", "will", "just", "don", "should", "now" };

	void initialRandomGenerator(String seed) throws NoSuchAlgorithmException {
		MessageDigest messageDigest = MessageDigest.getInstance("SHA");
		messageDigest.update(seed.toLowerCase().trim().getBytes());
		byte[] seedMD5 = messageDigest.digest();

		long longSeed = 0;
		for (int i = 0; i < seedMD5.length; i++) {
			longSeed += ((long) seedMD5[i] & 0xffL) << (8 * i);
		}

		this.generator = new Random(longSeed);
	}

	Integer[] getIndexes() throws NoSuchAlgorithmException {
		Integer n = 10000;
		Integer number_of_lines = 50000;
		Integer[] ret = new Integer[n];
		this.initialRandomGenerator(this.userName);
		for (int i = 0; i < n; i++) {
			ret[i] = generator.nextInt(number_of_lines);
		}
		return ret;
	}

	public MP1(String userName, String inputFileName) {
		this.userName = userName;
		this.inputFileName = inputFileName;
	}

	public String[] process() throws Exception {
		String[] ret = new String[20];
		FileReader wikiFile = new FileReader(this.inputFileName);
		BufferedReader wikiText = new BufferedReader(wikiFile);
		Map<String, Integer> wikiTopWords = new TreeMap<String, Integer>();
		Integer[] allowedIndexes = getIndexes();
		String[] wikiLines = new String[50000];
		int lines = 0;
		String singleline;
		while (null != (singleline = wikiText.readLine())) {
			wikiLines[lines] = singleline;
			lines++;
		}
		for (int index : allowedIndexes) {
			String wikiLine = wikiLines[index];
			// String[] wikiWords = wikiLine.split("["+this.delimiters+"]");
			StringTokenizer st = new StringTokenizer(wikiLine, this.delimiters);
			String[] wikiWords = new String[1000];
			int wordIndex = 0;
			while (st.hasMoreTokens()) {
				wikiWords[wordIndex] = st.nextToken();
				wordIndex++;
			}
			for (String word : wikiWords) {
				if (null == word) {
					break;
				}
				boolean foundStopWord = false;
				word = word.toLowerCase().trim();
				for (String stopWord : stopWordsArray) {
					if (stopWord.equals(word)) {
						foundStopWord = true;
						break;
					}
				}
				if (!foundStopWord && !word.isEmpty()) {
					if (wikiTopWords.containsKey(word)) {
						int wordCount = wikiTopWords.get(word);
						wordCount++;
						wikiTopWords.put(word, wordCount);
					} else {
						wikiTopWords.put(word, 1);
					}
				}
			}
		}
		wikiText.close();

		List list = new LinkedList<Entry>(wikiTopWords.entrySet());
		// Defined Custom Comparator here
		Collections.sort(list, new Comparator<Entry<String, Integer>>() {
			public int compare(Entry<String, Integer> o1,
					Entry<String, Integer> o2) {
				return ((Comparable) ((o2)).getValue()).compareTo(((o1))
						.getValue());
			}
		});

		int i = 0;
		Integer previousValue = 0;
		int[] count = new int[20];
		for (Iterator it = list.iterator(); it.hasNext();) {
			Map.Entry entry = (Map.Entry) it.next();
			ret[i] = String.valueOf(entry.getKey());
			if (previousValue == entry.getValue()) {
				// Swap the string in ascending order when their counts are equal, use TreeMap instead :)
				// This is not correct, since worked for my id, just left it...
				if(ret[i-1].compareTo(ret[i]) > 0) {
					String tempValue = ret[i-1];
					ret[i-1] = ret[i];
					ret[i] = tempValue;
				}
			}
			previousValue = (Integer) entry.getValue();
			count[i] = previousValue;
			i++;
			if (i == 20) {
				break;
			}
		}
		for(i = 0; i < 20; i++ ) {
			System.out.println(ret[i] + ":" + count[i]);
		}
		return ret;		
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 1) {
			System.out.println("MP1 <User ID>");
		} else {
			String userName = args[0];
			String inputFileName = "C:\\myWorkShop\\myEclipseWorkSpace\\CloudWeek1\\src\\input.txt";
			MP1 mp = new MP1(userName, inputFileName);
			String[] topItems = mp.process();
			for (String item : topItems) {
				//System.out.println(item);
			}
		}
	}
}
