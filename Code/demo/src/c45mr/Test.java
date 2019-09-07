package c45mr;

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

public class Test {

	public static double accuracy_show;
	public static double false_al_rate;
	public static String response_time;

	public static final String normal = "normal.";

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
			// root = document.getRootElement();
			Test.mr(args[0], xmlText);
		} catch (DocumentException e) {
			e.printStackTrace();
		}
		String[] l;
		int tmp = 0;
		HashMap<String, Integer> statistics = new HashMap<String, Integer>();
		try {
			file = new File("output-" + args[0] + "/part-r-00000");
			bReader = new BufferedReader(new FileReader(file));
			while ((line = bReader.readLine()) != null) {
				l = line.split("\\s+");
				try {
					tmp = statistics.get(l[0]);
				} catch (Exception e) {
					tmp = 0;
				}
				statistics.put(l[0], tmp + Integer.parseInt(l[1]));
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
		System.out.println(statistics);
		writer.println("accuracy = " + String.format("%.2f", acc) + "%\t" + (TP + TN) + " in " + (FP + FN + TP + TN));
		accuracy_show = acc;
		writer.println("false alarm rate = " + String.format("%.2f", fl_al_rate) + "%\t" + FP + " in " + (FP + TN));
		false_al_rate = fl_al_rate;
		System.out
				.println("accuracy = " + String.format("%.2f", acc) + "%\t" + (TP + TN) + " in " + (FP + FN + TP + TN));
		System.out.println("false alarm rate = " + String.format("%.2f", fl_al_rate) + "%\t" + FP + " in " + (FP + TN));
		final long totalTime = System.currentTimeMillis() - startTime;
		response_time = "" + (double) totalTime / 1000;
		writer.println("Total time: " + (double) totalTime / 1000);
		System.out.println("Total time: " + (double) totalTime / 1000);
		writer.close();
	}

	public static void mr(String path, String xmlText) throws Exception {
		Configuration conf = new Configuration();
		conf.setStrings("xml", xmlText);
		Job job = Job.getInstance(conf);
		job.setJarByClass(Test.class);
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
			String[] testing = value.toString().split(",");
			String answer = testing[testing.length - 1];
			if (answer.equals(normal)) {
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
			String prediction;
			String answer;
			Configuration conf = context.getConfiguration();
			String[] xmlText = conf.getStrings("xml");
			SAXReader reader = new SAXReader();
			Document doc = DocumentHelper.createDocument();
			try {
				doc = reader.read(new StringReader(xmlText[0]));
			} catch (DocumentException e) {
				e.printStackTrace();
			}
			HashMap<String, HashMap<String, Integer>> statistics = new HashMap<String, HashMap<String, Integer>>();
			HashMap<String, Integer> tmp = new HashMap<String, Integer>();
			Element root = doc.getRootElement();
			Element att_names_ele = root.element("Att_names");
			List<String> att_names = new ArrayList<String>(Arrays.asList(att_names_ele.getText().split("-")));
			Element child = root.element("DecisionTree");
			if (type.get() == 0) {
				int FP = 0;
				int TN = 0;
				for (Text t : data) {
					testing = new LinkedList<String>(Arrays.asList(t.toString().split(",")));
					answer = testing.remove(testing.size() - 1);
					prediction = decision(child, testing, att_names);
					if (!answer.equals(prediction)) {
						FP++;
						tmp = new HashMap<String, Integer>();
						tmp.put(prediction, new Integer(0));
						tmp = statistics.getOrDefault(normal, tmp);
						tmp.put(prediction, tmp.getOrDefault(prediction, 0) + 1);
						statistics.put(normal, tmp);
					} else {
						TN++;
						tmp = new HashMap<String, Integer>();
						tmp.put(normal, new Integer(0));
						tmp = statistics.getOrDefault(normal, tmp);
						tmp.put(normal, tmp.getOrDefault(normal, 0) + 1);
						statistics.put(normal, tmp);
					}
				}
				context.write(new Text("FP"), new IntWritable(FP));
				context.write(new Text("TN"), new IntWritable(TN));
			} else {
				int FN = 0;
				int TP = 0;
				for (Text t : data) {
					testing = new LinkedList<String>(Arrays.asList(t.toString().split(",")));
					answer = testing.remove(testing.size() - 1);
					prediction = decision(child, testing, att_names);
					if (!answer.equals(prediction)) {
						if (prediction.equals(normal)) {
							FN++;
							tmp = new HashMap<String, Integer>();
							tmp.put(prediction, new Integer(0));
							tmp = statistics.getOrDefault(answer, tmp);
							tmp.put(prediction, tmp.getOrDefault(prediction, 0) + 1);
							statistics.put(answer, tmp);
						} else {
							TP++;
							tmp = new HashMap<String, Integer>();
							tmp.put(prediction, new Integer(0));
							tmp = statistics.getOrDefault(answer, tmp);
							tmp.put(prediction, tmp.getOrDefault(prediction, 0) + 1);
							statistics.put(answer, tmp);
						}
					} else {
						TP++;
						tmp = new HashMap<String, Integer>();
						tmp.put(answer, new Integer(0));
						tmp = statistics.getOrDefault(answer, tmp);
						tmp.put(answer, tmp.getOrDefault(answer, 0) + 1);
						statistics.put(answer, tmp);
					}
				}
				context.write(new Text("FN"), new IntWritable(FN));
				context.write(new Text("TP"), new IntWritable(TP));
			}
			for (Entry<String, HashMap<String, Integer>> entry : statistics.entrySet()) {
				String key = entry.getKey();
				HashMap<String, Integer> value = entry.getValue();
				List<String> re = new ArrayList<String>();
				for (Entry<String, Integer> entry2 : value.entrySet()) {
					String key2 = entry2.getKey();
					Integer value2 = entry2.getValue();
					re.add(key2 + ":" + value2);
					// context.write(key, value);
				}
				System.out.println(key + "\t" + join(re));
				// context.write(key, value);
			}
		}
	}

	private final static String decision(Element parent, List<String> obs, List<String> att_names) {
		Iterator<Element> it = parent.elementIterator();
		String att_value = "";
		String flag = "";
		double value = 0f;
		String value_s = "";
		while (it.hasNext()) {
			Element child = (Element) it.next();
			att_value = obs.get(att_names.indexOf(child.getName()));
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
			return "unknown";
		}
		return parent.getText();
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