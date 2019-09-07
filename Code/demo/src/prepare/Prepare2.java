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

import xgb.Setting;

public class Prepare2 {
	public final static void main(String[] args) throws Exception {
		prepare(args[0]);
		prepare(args[1]);
//		prepare(args[2]);
	}
	private final static void prepare(String path) throws FileNotFoundException, UnsupportedEncodingException {
		PrintWriter writer = new PrintWriter(path+".txt", "UTF-8");
		List<String> atts = new ArrayList<String>();
		List<String> out = new ArrayList<String>();
		BufferedReader bReader = null;
		String line;
		double x;
		String att;
		int hash=0;
		List<HashMap<String, Integer>> map = new ArrayList<HashMap<String, Integer>>();
		List<Integer> seq = new ArrayList<Integer>();
		for(int i = 0; i < 50; i++) {
			map.add(new HashMap<String, Integer>());
			seq.add(Integer.valueOf(0));
		}
		try {
			File file = new File(path+".csv");
//			File file = new File(path);
			bReader = new BufferedReader(new FileReader(file));
			while ((line = bReader.readLine()) != null) {
				atts = new ArrayList<String>(Arrays.asList(line.split(",")));
//				out.add(Integer.toString(Math.abs(atts.get(atts.size() - 1).hashCode())%1023));
				for (int i = 0; i < atts.size(); i++) {
					att = atts.get(i);
					try {
						// calculating PR
						x = Double.parseDouble(att);
						out.add(atts.get(i));
					} catch (NumberFormatException e) {
						try {
							hash = map.get(i).get(att);
						} catch (Exception e2) {
							map.get(i).put(att, seq.get(i));
							seq.set(i, seq.get(i) + 1);
							hash = map.get(i).get(att);
							if(i==atts.size()-1) {
								System.out.println(att+","+hash);
							}
						}
						out.add(Integer.toString(hash));
					}
				}
				writer.println(join(out));
				out = new ArrayList<String>();
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
	private final static String join(List<String> line) {
		String SEPARATOR = ",";
		StringBuilder csvBuilder = new StringBuilder();
		for (String str : line) {
			csvBuilder.append(str);
			csvBuilder.append(SEPARATOR);
		}
		// Remove last comma
		String csv = csvBuilder.toString();
		csv = csv.substring(0, csv.length() - SEPARATOR.length());
		return csv;
	}
}
