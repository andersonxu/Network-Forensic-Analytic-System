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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PrepareDARPA {
	public static final int space = 5095;

	public final static void main(String[] args) throws Exception {
		prepare(args);
	}

	private final static void prepare(String[] args) throws FileNotFoundException, UnsupportedEncodingException {
		PrintWriter writer = new PrintWriter("list.txt", "UTF-8");
		List<String> atts = new ArrayList<String>();
		List<String> out = new ArrayList<String>();
		BufferedReader bReader = null;
		String line;
		Set<String> map = new HashSet<String>();
		Set<Integer> map2 = new HashSet<Integer>();
		List<String> dt;
		boolean miss;

		for (int j = 0; j < args.length; j++) {
			map = new HashSet<String>();
			map2 = new HashSet<Integer>();
			String path = args[j];
			try {
				File file = new File(path);
				bReader = new BufferedReader(new FileReader(file));
				while ((line = bReader.readLine()) != null) {
					miss = false;
					atts = new ArrayList<String>(Arrays.asList(line.split(" ")));
					for (int i = 0; i < atts.size() - 1; i++) {
						if (atts.get(i).equals("-")) {
							miss = true;
							break;
						}
					}
					if (miss) {
						continue;
					}
					dt = new ArrayList<String>(Arrays.asList(atts.get(4).split(":")));
					out.add("" + (Integer.parseInt(dt.get(0)) * 3600 + Integer.parseInt(dt.get(1)) * 60
							+ Integer.parseInt(dt.get(2))));
					try {
						// calculating PR
						Long.parseLong(atts.get(5));
						// if (x != 0) {
						out.add("" + atts.get(5));
						// }
					} catch (NumberFormatException e) {

						out.add("" + Math.abs(atts.get(5).hashCode()));
						map.add(atts.get(5));
						map2.add(Math.abs(atts.get(5).hashCode()));
					}
					out.add("" + atts.get(6));
					out.add("" + atts.get(7));
					dt = new ArrayList<String>(Arrays.asList(atts.get(8).split("\\.")));
					out.add("" + (Long.parseLong(dt.get(0)) * 16777216 + Long.parseLong(dt.get(1)) * 65536
							+ Long.parseLong(dt.get(2)) * 256 + Long.parseLong(dt.get(3))));
					dt = new ArrayList<String>(Arrays.asList(atts.get(9).split("\\.")));
					out.add("" + (Long.parseLong(dt.get(0)) * 16777216 + Long.parseLong(dt.get(1)) * 65536
							+ Long.parseLong(dt.get(2)) * 256 + Long.parseLong(dt.get(3))));
					out.add(atts.get(atts.size() - 2));
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
			System.out.println(map.size());
			System.out.println(map2.size());
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
