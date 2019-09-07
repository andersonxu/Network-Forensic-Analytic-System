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

public class Prepare {
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
		try {
			File file = new File(path+".csv");
			bReader = new BufferedReader(new FileReader(file));
			while ((line = bReader.readLine()) != null) {
				atts = new ArrayList<String>(Arrays.asList(line.split(",")));
//				out.add(Integer.toString(Math.abs(atts.get(atts.size() - 1).hashCode())%1023));
				for (int i = 0; i < atts.size(); i++) {
					try {
						// calculating PR
						x = Double.parseDouble(atts.get(i));
//						if (x != 0) {
							out.add(atts.get(i));
//						}
					} catch (NumberFormatException e) {
						
						out.add(Integer.toString((Math.abs(atts.get(i).hashCode())%4095)));
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
