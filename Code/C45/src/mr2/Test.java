package mr2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
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
	public static final double normal = (double) Math.abs("normal.".hashCode()) % 4095;
	public static final double ini = 3507;
	public static final double range = 10;

	public final static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.err.println("Usage: mr/Test <input path> <attribute names file path> <model file path>");
			System.exit(-1);
		}

		FileSystem fs = FileSystem.get(new Configuration());
		fs.delete(new Path("output-" + args[0]), true);
		final long startTime = System.currentTimeMillis();
		String model_path = args[2];
		BufferedReader bReader = null;
		String line = "";
		String[] att_names = new String[0];
		File file;
		try {
			file = new File(args[1]);
			bReader = new BufferedReader(new FileReader(file));
			line = bReader.readLine();
			att_names = line.split(",");
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				bReader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		SAXReader reader = new SAXReader();
		try {
			Document document = reader.read(new File(model_path));
			String xmlText = document.asXML();
			// root = document.getRootElement();
			Test.mr(args[0], att_names, xmlText);
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
		int FP = statistics.get("FP"), FN = statistics.get("FN"), TP = statistics.get("TP"), TN = statistics.get("TN");
		double acc = (double) ((TP + TN) * 100) / (FP + FN + TP + TN);
		double dtc_rate = (double) (TP * 100) / (FP + FN + TP);
		double fl_al_rate = (double) (FP * 100) / (FP + TN);
		PrintWriter writer = new PrintWriter("statistics.txt", "UTF-8");
		writer.println("testing file: " + args[0]);
		writer.println("accuracy = " + String.format("%.2f", acc) + "%\t" + (TP + TN) + " in " + (FP + FN + TP + TN));
		writer.println("detection rate = " + String.format("%.2f", dtc_rate) + "%\t" + TP + " in " + (FP + FN + TP));
		writer.println("false alarm rate = " + String.format("%.2f", fl_al_rate) + "%\t" + FP + " in " + (FP + TN));
		System.out.println("FP " + FP);
		System.out.println("TP " + TP);
		System.out.println("FN " + FN);
		System.out.println("TN " + TN);
		System.out
				.println("accuracy = " + String.format("%.2f", acc) + "%\t" + (TP + TN) + " in " + (FP + FN + TP + TN));
		System.out
				.println("detection rate = " + String.format("%.2f", dtc_rate) + "%\t" + TP + " in " + (FP + FN + TP));
		System.out.println("false alarm rate = " + String.format("%.2f", fl_al_rate) + "%\t" + FP + " in " + (FP + TN));
		final long totalTime = System.currentTimeMillis() - startTime;
		writer.println("Total time: " + (double) totalTime / 1000);
		System.out.println("Total time: " + (double) totalTime / 1000);
		writer.close();
	}

	public static void mr(String path, String[] att_names, String xmlText) throws Exception {
		Configuration conf = new Configuration();
		conf.setStrings("att_names", att_names);
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
			List<String> att_names = new ArrayList<String>(Arrays.asList(conf.getStrings("att_names")));
			SAXReader reader = new SAXReader();
			Document doc = DocumentHelper.createDocument();
			try {
				doc = reader.read(new StringReader(xmlText[0]));
			} catch (DocumentException e) {
				e.printStackTrace();
			}
			Element root = doc.getRootElement();
			if (type.get() == 0) {
				int FP = 0;
				int TN = 0;
				for (Text t : data) {
					testing = new LinkedList<String>(Arrays.asList(t.toString().split(",")));
					answer = Double.parseDouble(testing.remove(testing.size() - 1));
					prediction = pred(root, testing, att_names);
					if (prediction > (answer + range) || prediction <= (answer - range)) {
						FP++;
					} else {
						TN++;
					}
				}
				context.write(new Text("FP"), new IntWritable(FP));
				context.write(new Text("TN"), new IntWritable(TN));
			} else {
				int FN = 0;
				int TP = 0;
				for (Text t : data) {
					testing = new LinkedList<String>(Arrays.asList(t.toString().split(",")));
					answer = Double.parseDouble(testing.remove(testing.size() - 1));
					prediction = pred(root, testing, att_names);
					if (prediction > (answer + range) || prediction <= (answer - range)) {
						if ((prediction <= (normal + range)) && (prediction > (normal - range))) {
							FN++;
						}else {
							TP++;
						}
					} else {
						TP++;
					}
				}
				context.write(new Text("FN"), new IntWritable(FN));
				context.write(new Text("TP"), new IntWritable(TP));
			}
		}
	}

	private final static double pred(Element parent, List<String> obs, List<String> att_names) {
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
			return 0f;
		}
		return Double.parseDouble(parent.getText());
	}
}