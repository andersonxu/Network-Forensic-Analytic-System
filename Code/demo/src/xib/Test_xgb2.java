package xgb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.io.StringReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

public class Test_xgb2 {
	public static final int normal = (int) Math.abs("normal.".hashCode()) % 4095;
	// public static final int normal = 0;
	public static final HashMap<Integer, String> hash;

	public static double accuracy_show_xg;
	public static double false_al_rate_xg;
	public static String response_time_xg;

	static {
		hash = new HashMap<Integer, String>();
		hash.put(1675, "xlock.");
		hash.put(984, "nmap.");
		hash.put(2539, "imap.");
		hash.put(196, "perl.");
		hash.put(658, "processtable.");
		hash.put(775, "mscan.");
		hash.put(678, "ps.");
		hash.put(4018, "smurf.");
		hash.put(1559, "httptunnel.");
		hash.put(3486, "guess_passwd.");
		hash.put(593, "snmpguess.");
		hash.put(195, "warezmaster.");
		hash.put(1731, "xsnoop.");
		hash.put(3094, "sendmail.");
		hash.put(1765, "xterm.");
		hash.put(648, "rootkit.");
		hash.put(2228, "portsweep.");
		hash.put(3291, "ipsweep.");
		hash.put(2305, "multihop.");
		hash.put(1647, "saint.");
		hash.put(960, "buffer_overflow.");
		hash.put(1988, "sqlattack.");
		hash.put(187, "teardrop.");
		hash.put(3128, "named.");
		hash.put(82, "udpstorm.");
		hash.put(3631, "ftp_write.");
		hash.put(1940, "satan.");
		hash.put(627, "neptune.");
		hash.put(4039, "phf.");
		hash.put(342, "back.");
		hash.put(3507, "normal.");
		hash.put(1939, "worm.");
		hash.put(1689, "loadmodule.");
		hash.put(3491, "land.");
		hash.put(2517, "snmpgetattack.");
		hash.put(2514, "pod.");
		hash.put(1308, "mailbomb.");
		hash.put(916, "apache2.");
		// hash.put(0, "normal");
		// hash.put(1, "a");
	}
	// public static final int normal = 0;
	public static final double range = 10;

	public final static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: mr/Test <input path> <model file path>");
			System.exit(-1);
		}

		FileSystem fs = FileSystem.get(new Configuration());
		fs.delete(new Path("output-" + args[0]), true);
		final long startTime = System.currentTimeMillis();
		String model_path = args[1];
		BufferedReader bReader = null;
		String line = "";
		File file;
		SAXReader reader = new SAXReader();
		try {
			Document document = reader.read(new File(model_path));
			String xmlText = document.asXML();
			System.out.println(xmlText);
			// root = document.getRootElement();
			Test_xgb2.mr(args[0], xmlText);
		} catch (DocumentException e) {
			e.printStackTrace();
		}
		String[] l;
		HashMap<String, Integer> statistics = new HashMap<String, Integer>();
		try {
			file = new File("output-" + args[0] + "/part-r-00000");
			bReader = new BufferedReader(new FileReader(file));
			while ((line = bReader.readLine()) != null) {
				l = line.split("\\s+");
				statistics.put(l[0], Integer.parseInt(l[1]));
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
		int FP, FN, TP, TN;
		try {
			FP = statistics.get("FP");
		} catch (NullPointerException e) {
			FP = 0;
		}
		try {
			FN = statistics.get("FN");
		} catch (NullPointerException e) {
			FN = 0;
		}
		try {
			TP = statistics.get("TP");
		} catch (NullPointerException e) {
			TP = 0;
		}
		try {
			TN = statistics.get("TN");
		} catch (NullPointerException e) {
			TN = 0;
		}
		double acc = (double) ((TP + TN) * 100) / (FP + FN + TP + TN);
		double fl_al_rate = (double) (FP * 100) / (FP + TN);
		PrintWriter writer = new PrintWriter("statistics.txt", "UTF-8");
		writer.println("testing file: " + args[0]);
		writer.println("accuracy = " + String.format("%.2f", acc) + "%\t" + (TP + TN) + " in " + (FP + FN + TP + TN));
		accuracy_show_xg = acc;
		writer.println("false alarm rate = " + String.format("%.2f", fl_al_rate) + "%\t" + FP + " in " + (FP + TN));
		false_al_rate_xg = fl_al_rate;
		System.out.println(statistics);
		System.out
				.println("accuracy = " + String.format("%.2f", acc) + "%\t" + (TP + TN) + " in " + (FP + FN + TP + TN));
		System.out.println("false alarm rate = " + String.format("%.2f", fl_al_rate) + "%\t" + FP + " in " + (FP + TN));
		final long totalTime = System.currentTimeMillis() - startTime;
		response_time_xg = "" + (double) totalTime / 1000;
		writer.println("Total time: " + (double) totalTime / 1000);
		System.out.println("Total time: " + (double) totalTime / 1000);
		writer.close();
	}

	public static void mr(String path, String xmlText) throws Exception {
		Configuration conf = new Configuration();
		conf.setStrings("xml", xmlText);
		Job job = Job.getInstance(conf);
		job.setJarByClass(Test_xgb2.class);
		job.setJobName("test");

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(path));
		FileOutputFormat.setOutputPath(job, new Path("output-" + path));

		job.waitForCompletion(true);
	}

	public static class Map extends Mapper<LongWritable, Text, IntWritable, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] testing;
			Double answer = 0.0;
			testing = value.toString().split(",");
			answer = Double.parseDouble(testing[testing.length - 1]);
			if (answer == normal) {
				context.write(new IntWritable(0), value);
			} else {
				context.write(new IntWritable(1), value);
			}

		}
	}

	public static class Reduce extends Reducer<IntWritable, Text, Text, IntWritable> {

		public void reduce(IntWritable type, Iterable<Text> data, Context context)
				throws IOException, InterruptedException {
			List<String> testing = new ArrayList<String>();
			Double prediction = 0.0;
			Double answer = 0.0;
			Configuration conf = context.getConfiguration();
			String[] xmlText = conf.getStrings("xml");
			SAXReader reader = new SAXReader();
			Document doc = DocumentHelper.createDocument();
			try {
				doc = reader.read(new StringReader(xmlText[0]));
			} catch (DocumentException e) {
				e.printStackTrace();
			}
			HashMap<String, HashMap<Integer, Integer>> statistics = new HashMap<String, HashMap<Integer, Integer>>();
			HashMap<Integer, Integer> tmp = new HashMap<Integer, Integer>();
			int sum;
			Element root = doc.getRootElement();
			if (type.get() == 0) {
				int FP = 0;
				int TN = 0;
				for (Text t : data) {
					testing = new LinkedList<String>(Arrays.asList(t.toString().split(",")));
					prediction = pred(root, testing);
					if (prediction > (normal + range) || prediction <= (normal - range)) {
						FP++;
						tmp = new HashMap<Integer, Integer>();
						tmp.put(new Integer(prediction.intValue()), new Integer(0));
						tmp = statistics.getOrDefault(Integer.toString(normal), tmp);
						tmp.put(prediction.intValue(), tmp.getOrDefault(prediction.intValue(), 0) + 1);
						statistics.put(Integer.toString(normal), tmp);
						// context.write(new Text(normal + ""), new IntWritable(prediction.intValue()));
					} else {
						TN++;
						tmp = new HashMap<Integer, Integer>();
						tmp.put(new Integer(normal), new Integer(0));
						tmp = statistics.getOrDefault(Integer.toString(normal), tmp);
						tmp.put((int) normal, tmp.getOrDefault((int) normal, 0) + 1);
						statistics.put(Integer.toString(normal), tmp);
						// context.write(new Text(normal + ""), new IntWritable(normal));
					}
					// statistics.put("A" + normal, statistics.getOrDefault("A" + normal, 1) + 1);

				}
				context.write(new Text("FP"), new IntWritable(FP));
				context.write(new Text("TN"), new IntWritable(TN));

			} else {
				int FN = 0;
				int TP = 0;
				for (Text t : data) {
					testing = new LinkedList<String>(Arrays.asList(t.toString().split(",")));
					answer = Double.parseDouble(testing.remove(testing.size() - 1));
					prediction = pred(root, testing);
					if (prediction > (answer + range) || prediction <= (answer - range)) {
						if ((prediction <= (normal + range)) && (prediction > (normal - range))) {
							FN++;
							tmp = new HashMap<Integer, Integer>();
							tmp.put(new Integer(prediction.intValue()), new Integer(0));
							tmp = statistics.getOrDefault(Integer.toString(answer.intValue()), tmp);
							tmp.put(prediction.intValue(), tmp.getOrDefault(prediction.intValue(), 0) + 1);
							statistics.put(Integer.toString(answer.intValue()), tmp);
							// context.write(new Text(answer + ""), new IntWritable(prediction.intValue()));
						} else {
							TP++;
							tmp = new HashMap<Integer, Integer>();
							tmp.put(new Integer(prediction.intValue()), new Integer(0));
							tmp = statistics.getOrDefault(Integer.toString(answer.intValue()), tmp);
							tmp.put(prediction.intValue(), tmp.getOrDefault(prediction.intValue(), 0) + 1);
							statistics.put(Integer.toString(answer.intValue()), tmp);
							// statistics.put(Double.toString(answer),
							// statistics.getOrDefault(Double.toString(answer), 1) + 1);
							// context.write(new Text(answer + ""), new IntWritable(prediction.intValue()));
						}
					} else {
						TP++;
						tmp = new HashMap<Integer, Integer>();
						tmp.put(new Integer(answer.intValue()), new Integer(0));
						tmp = statistics.getOrDefault(Integer.toString(answer.intValue()), tmp);
						tmp.put(answer.intValue(), tmp.getOrDefault(answer.intValue(), 0) + 1);
						statistics.put(Integer.toString(answer.intValue()), tmp);
						// statistics.put(Double.toString(answer),
						// statistics.getOrDefault(Double.toString(answer), 1) + 1);
						// context.write(new Text(answer + ""), new IntWritable(answer.intValue()));
					}
					// statistics.put("A" + answer, statistics.getOrDefault("A" + answer, 1) + 1);
				}
				context.write(new Text("FN"), new IntWritable(FN));
				context.write(new Text("TP"), new IntWritable(TP));

			}
			String r_key2 = "unknown";
			for (Entry<String, HashMap<Integer, Integer>> entry : statistics.entrySet()) {
				String key = entry.getKey();
				HashMap<Integer, Integer> value = entry.getValue();
				List<String> re = new ArrayList<String>();
				for (Entry<Integer, Integer> entry2 : value.entrySet()) {
					Integer key2 = entry2.getKey();
					Integer value2 = entry2.getValue();
					for (int i = 0; i <= range; i++) {
						r_key2 = hash.get(key2 + i);
						if (r_key2 == null) {
							r_key2 = hash.get(key2 - i);
						}
						if (r_key2 != null) {
							break;
						}
					}
					if (r_key2 == null) {
						r_key2 = "unknown";
					}
					re.add(r_key2 + ":" + value2);
					// context.write(key, value);
				}
				String r_key = hash.get(Integer.parseInt(key));
				System.out.println(r_key + "\t" + join(re));
				// context.write(key, value);
			}

		}
	}

	private final static double pred(Element parent, List<String> obs) {
		Element att_names_ele = parent.element("Att_names");
		List<String> att_names = new ArrayList<String>(Arrays.asList(att_names_ele.getText().split(":")));
		Element ini_ele = parent.element("Ini_value");
		double ini = Double.parseDouble(ini_ele.getText());
		Iterator<Element> it = parent.elementIterator("DecisionTree");
		double pred = 0f;
		while (it.hasNext()) {
			Element child = (Element) it.next();
			pred += decision(child, obs, att_names);
		}
		return pred + ini;
	}

	private final static double decision(Element parent, List<String> obs, List<String> att_names) {
		Iterator<Element> it = parent.elementIterator();
		String att_value = "";
		String flag = "";
		double value = 0f;
		String value_s = "";
		while (it.hasNext()) {
			Element child = (Element) it.next();
			try {
				att_value = obs.get(att_names.indexOf(child.getName()));
			} catch (IndexOutOfBoundsException e) {
				att_value = obs.get(Integer.parseInt(child.getName().substring(1)));
			}
			flag = child.attribute("flag").getValue();
			value_s = child.attribute("value").getValue();
			try {
				value = Double.parseDouble(value_s);
			} catch (NumberFormatException e) {

			}
			if ((flag.equals("m") && value_s.equals(att_value))
					|| (flag.equals("l") && Double.parseDouble(att_value) < value)
					|| (flag.equals("r") && Double.parseDouble(att_value) >= value)) {
				return decision(child, obs, att_names);
			}
		}
		String return_text = parent.getText();
		if (return_text.equals("")) {
			return 0f;
		}
		return Double.parseDouble(parent.getText());
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