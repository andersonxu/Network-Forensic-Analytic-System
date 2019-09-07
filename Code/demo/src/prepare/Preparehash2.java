package prepare;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import xgb.Setting;

public class Preparehash2 {
	public final static void main(String[] args) throws Exception {
		prepare(args[0]);
	}

	private final static void prepare(String path) throws FileNotFoundException, UnsupportedEncodingException {
		PrintWriter writer = new PrintWriter(path + "cal.txt", "UTF-8");
		List<String> atts = new ArrayList<String>();
		List<String> out = new ArrayList<String>();
		BufferedReader bReader = null;
		String line;
		String ans;
		HashMap<String, Integer> statistics = new HashMap<String, Integer>();
		double x;
		try {
			File file = new File(path + ".csv");
			bReader = new BufferedReader(new FileReader(file));
			while ((line = bReader.readLine()) != null) {
				atts = new ArrayList<String>(Arrays.asList(line.split(",")));
				ans = atts.get(atts.size() - 1);
				statistics.put(ans, statistics.getOrDefault(ans, 0) + 1);
			}
			for (Entry<String, Integer> entry : statistics.entrySet()) {
				writer.println(entry.getKey() + "\t" + entry.getValue());
			}

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				bReader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		writer.close();
	}
}
