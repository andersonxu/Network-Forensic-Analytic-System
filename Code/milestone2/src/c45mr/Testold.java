package c45mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;

import org.apache.hadoop.conf.Configuration;
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
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

public class Testold {
	public static List<String> att_names;
	public static int FP;
	public static int FN;
	public static int TP;
	public static int TN;
	public static Element root;

	// public static String model_path;

	public final static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.err.println("Usage: mr/Test <input path> <attribute names file path> <model file path>");
			System.exit(-1);
		}
		PrintWriter writer = new PrintWriter("statistics.txt", "UTF-8");
		FileSystem fs = FileSystem.get(new Configuration());
		writer.println("testing file: " + args[0]);
		fs.delete(new Path("output-" + args[0]), true);
		final long startTime = System.currentTimeMillis();
		String model_path = args[2];
		BufferedReader bReader = null;
		try {
			File file = new File(args[1]);
			bReader = new BufferedReader(new FileReader(file));
			String line = bReader.readLine();
			att_names = new ArrayList<String>(Arrays.asList(line.split(",")));
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
			root = document.getRootElement();
			Testold.mr(args[0]);
		} catch (DocumentException e) {
			e.printStackTrace();
		}
		float acc = (float) ((TP + TN) * 100) / (FP + FN + TP + TN);
		float dtc_rate = (float) (TP * 100) / (FP + FN + TP);
		float fl_al_rate = (float) (FP * 100) / (FP + TN);
		writer.println("accuracy = " + String.format("%.2f", acc) + "%\t" + (TP + TN) + " in " + (FP + FN + TP + TN));
		writer.println("detection rate = " + String.format("%.2f", dtc_rate) + "%\t" + TP + " in " + (FP + FN + TP));
		writer.println("false alarm rate = " + String.format("%.2f", fl_al_rate) + "%\t" + FP + " in " + (FP + TN));
		final long totalTime = System.currentTimeMillis() - startTime;
		writer.println("Total time: " + (float) totalTime / 1000);
		writer.close();
	}

	public static void mr(String path) throws Exception {
		Job job = Job.getInstance();
		job.setJarByClass(Testold.class);
		job.setJobName("test");

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(path));
		FileOutputFormat.setOutputPath(job, new Path("output-" + path));

		job.waitForCompletion(true);
	}

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			List<String> testing = new ArrayList<String>();
			String answer = new String();
			String prediction = new String();
			testing = new LinkedList<String>(Arrays.asList(value.toString().split(",")));
			answer = testing.get(testing.size() - 1);
			testing.remove(testing.size() - 1);
			prediction = decision(root, testing);
			context.write(new Text(answer), new Text(prediction));
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text answer, Iterable<Text> prediction, Context context)
				throws IOException, InterruptedException {
			for (Text pred : prediction) {
				String ans = answer.toString();
				String pre = pred.toString();
				if (!ans.equals(pre)) {
					if (ans.equals("normal.")) {
						FP++;
					} else if (pre.equals("normal.")) {
						FN++;
					}
					context.write(new Text(ans), new Text(pre));
				} else {
					if (ans.equals("normal.")) {
						TN++;
					} else if (!ans.equals("normal.")) {
						TP++;
					}
				}
			}
		}
	}

	private final static String decision(Element parent, List<String> obs) {
		Iterator<Element> it = parent.elementIterator();
		String att_value = "";
		String flag = "";
		float value = 0f;
		String value_s = "";
		while (it.hasNext()) {
			Element child = (Element) it.next();
			att_value = obs.get(att_names.indexOf(child.getName()));
			flag = child.attribute("flag").getValue();
			value_s = child.attribute("value").getValue();
			try {
				value = Float.parseFloat(value_s);
			} catch (NumberFormatException e) {

			}
			if ((flag.equals("m") && value_s.equals(att_value))
					|| (flag.equals("l") && Float.parseFloat(att_value) < value)
					|| (flag.equals("r") && Float.parseFloat(att_value) >= value)) {
				return decision(child, obs);
			}
		}
		String return_text = parent.getText();
		if (return_text.equals("")) {
			return "none";
		}
		return parent.getText();
	}
}