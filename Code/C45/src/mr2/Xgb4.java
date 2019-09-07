package mr2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.dom4j.io.XMLWriter;

public class Xgb4 {
	public static List<String> att_names;
	public static final String divide = "-";
	public static final String comma = ",";
	public static final String space = "\\s+";
	public static FileSystem fs;

	public final static void main(String[] args) throws Exception {
		// check the number of arguments
		if (args.length != 2) {
			System.err.println("Usage: mr/C45 <input path> <attribute names file path>");
			System.exit(-1);
		}
		final long startTime = System.currentTimeMillis();
		float ini = 0.5f;
		float gamma = 0f;
		float eta = 0.5f;
		float lambda = 1f;
		int max_depth = 10;
		int round = 2;
		HashMap<String, Object> params = new HashMap<String, Object>();
		params.put("ini", ini);
		params.put("gamma", gamma);
		params.put("eta", eta);
		params.put("lambda", lambda);
		params.put("max_depth", max_depth);
		params.put("round", round);

		fs = FileSystem.get(new Configuration());
		// delete previous output files
		fs.delete(new Path("output"), true);
		fs.delete(new Path("res"), true);
		System.out.println(args[0]);
		// read in attribute names for print off the model
		BufferedReader bReader = null;
		try {
			bReader = new BufferedReader(new FileReader(new File(args[1])));
			String line = bReader.readLine();
			att_names = new ArrayList<String>(Arrays.asList(line.split(comma)));
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				bReader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		// train the model
		Xgb4.train(args[0], params);
		// print off the training time
		final long totalTime = System.currentTimeMillis() - startTime;
		System.out.println("Total time: " + (float) totalTime / 1000);
	}

	private final static void train(String input_path, HashMap<String, Object> params) throws Exception {
		File file = new File("models/");
		if (!file.exists())
			file.mkdirs();
		file = new File("res/");
		if (!file.exists())
			file.mkdirs();
		Document document;
		Element root;
		Document document_all = DocumentHelper.createDocument();
		Element root_all = document_all.addElement("All");
		XMLWriter writer;
		int depth = 0;

		String[] ini_set = new String[3];
		String orig_input_path = input_path;
		for (int c_round = 0; c_round < (int) params.get("round"); c_round++) {
			document = DocumentHelper.createDocument();
			root = document.addElement("DecisionTree");
			if (c_round > 0) {
				input_path = "res/" + (c_round - 1) + "/part-m-00000";
			}
			try {
				// train the model
				grow(input_path, root, depth, ini_set, params);
			} catch (OutOfMemoryError e) {
				System.out.println("Out of memory");
			}
			Element a = root.createCopy();
			root_all.add(a);
			// print off the model
			file = new File("models/" + orig_input_path + "model" + c_round + ".xml");
			try {
				writer = new XMLWriter(new FileOutputStream(file));
				writer.setEscapeText(false);
				writer.write(document);
				writer.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			String xmlText = document.asXML();
			Xgb4.res_mr(input_path, xmlText, c_round, (float) params.get("ini"));
		}
		file = new File("models/" + orig_input_path + "modelAll.xml");
		try {
			writer = new XMLWriter(new FileOutputStream(file));
			writer.setEscapeText(false);
			writer.write(document_all);
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private final static void grow(String input_path, Element parent, int depth, String[] set,
			HashMap<String, Object> params) throws Exception {
		// depth is the depth of the tree
		depth++;
		// summarise the data it needs
		Xgb4.sum_mr(input_path, depth, set, params);
		// calculate the gain ratio
		Xgb4.att_mr(input_path, depth, params);
		// find the maximum gain ratio
		Xgb4.cal_mr(input_path, depth, params);
		BufferedReader bReader = null;
		try {
			bReader = new BufferedReader(new FileReader(new File("output/cal-" + depth + "/part-r-00000")));
			String line = bReader.readLine();
			// System.out.println(line);
			// read in the result
			Setting settings = new Setting(line);
			// if can't go deeper terminate growing
			if (settings.getScore() <= 0f || depth > (int) params.get("max_depth")) {
				parent.setText(settings.getR());
			} else {
				String[] new_set = new String[3];
				// if this attribute is numeric go to left and right children
				new_set[0] = Integer.toString(settings.getAtt());
				new_set[1] = "l";
				new_set[2] = Float.toString(settings.getP());
				Element sonl = parent.addElement(att_names.get(settings.getAtt()));
				sonl.addAttribute("flag", "l");
				sonl.addAttribute("value", String.format("%.04f", settings.getP()));
				grow(input_path, sonl, depth, new_set, params);
				new_set[1] = "r";
				Element sonr = parent.addElement(att_names.get(settings.getAtt()));
				sonr.addAttribute("flag", "r");
				sonr.addAttribute("value", String.format("%.04f", settings.getP()));
				grow(input_path, sonr, depth, new_set, params);
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
		try {
			// delete used data
			if (fs.exists(new Path("output/att-" + depth))) {
				fs.delete(new Path("output/att-" + depth), true);
				fs.delete(new Path("output/sum-" + depth), true);
				fs.delete(new Path("output/cal-" + depth), true);
			}
		} catch (FileAlreadyExistsException e) {
			e.printStackTrace();
		}
	}

	public static void sum_mr(String input_path, int depth, String[] set, HashMap<String, Object> params)
			throws Exception {
		Configuration conf = new Configuration();
		conf.setStrings("conf", set);
		conf.setFloat("ini", (float) params.get("ini"));
		Job job = Job.getInstance(conf);
		job.setJarByClass(Xgb4.class);
		job.setJobName("sum_mr");

		job.setMapperClass(Map_sum.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setNumReduceTasks(0);

		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path("output/sum-" + depth));
		// read the original file at the first time
		if (depth > 1) {
			depth--;
			job.setMapperClass(Map_sum.class);
			job.setInputFormatClass(SequenceFileInputFormat.class);
			FileInputFormat.addInputPath(job, new Path("output/sum-" + depth));
		} else {
			job.setMapperClass(Map_sum_ini.class);
			job.setInputFormatClass(TextInputFormat.class);
			FileInputFormat.addInputPath(job, new Path(input_path));
		}
		job.waitForCompletion(true);
	}

	public static class Map_sum_ini extends Mapper<LongWritable, Text, NullWritable, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			float ini = conf.getFloat("ini", 0.5f);
			String[] line = value.toString().split(comma);
			String[] cat_pred = line[line.length - 1].split(divide);
			if (cat_pred.length > 1) {
				context.write(NullWritable.get(), value);
			} else {
				Text ini_value = new Text(value + divide + ini);
				context.write(NullWritable.get(), ini_value);
			}

		}
	}

	public static class Map_sum extends Mapper<NullWritable, Text, NullWritable, Text> {
		public void map(NullWritable key, Text value, Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String[] set = conf.getStrings("conf");
			String[] line = value.toString().split(comma);
			boolean l_r = Float.parseFloat(line[Integer.parseInt(set[0])]) < Float.parseFloat((set[2]));
			if (((set[1].equals("l") && l_r) || ((set[1].equals("r") && !l_r)))) {
				context.write(NullWritable.get(), value);
			}
		}
	}

	public static void att_mr(String input_path, int depth, HashMap<String, Object> params) throws Exception {
		Configuration conf = new Configuration();
		conf.setFloat("gamma", (float) params.get("gamma"));
		conf.setFloat("lambda", (float) params.get("lambda"));
		conf.setFloat("eta", (float) params.get("eta"));
		Job job = Job.getInstance(conf);
		job.setJarByClass(Xgb4.class);
		job.setJobName("att_mr");

		job.setMapperClass(Map_att.class);
		job.setReducerClass(Reduce_att.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(PairWritable.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(PairWritable.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path("output/sum-" + depth));
		FileOutputFormat.setOutputPath(job, new Path("output/att-" + depth));

		job.waitForCompletion(true);
	}

	public static class Map_att extends Mapper<NullWritable, Text, IntWritable, PairWritable> {
		public void map(NullWritable key, Text value, Context context) throws IOException, InterruptedException {
			List<String> line = new ArrayList<String>();
			line = new LinkedList<String>(Arrays.asList(value.toString().split(comma)));
			String cat_pred = line.get(line.size() - 1);
			PairWritable p;
			for (int i = 0; i < line.size() - 1; i++) {
				p = new PairWritable(new Text(line.get(i)), new Text(cat_pred));
				context.write(new IntWritable(i), p);
			}
		}
	}

	public static class Reduce_att extends Reducer<IntWritable, PairWritable, NullWritable, PairWritable> {
		public void reduce(IntWritable key, Iterable<PairWritable> ps, Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			float gamma = conf.getFloat("gamma", 0f);
			float lambda = conf.getFloat("lambda", 1f);
			float eta = conf.getFloat("eta", 1f);
			// preparing the data
			List<Pair<Float, Pair<Float, Float>>> data = new ArrayList<Pair<Float, Pair<Float, Float>>>();
			// List<String> all_att = new ArrayList<String>();
			for (PairWritable p : ps) {
				// all_att.add(p.getL().toString());
				String[] cat_pred = p.getR().toString().split(divide);
				float att = Float.parseFloat(p.getL().toString());
				float cat = Float.parseFloat(cat_pred[0]);
				float pred = Float.parseFloat(cat_pred[1]);
				data.add(new Pair<Float, Pair<Float, Float>>(att, new Pair<Float, Float>(cat, pred)));
			}
			// int data_size = data.size();
			// System.out.println("data size" +data_size);
			Pair<Float, Pair<Float, Float>> score_div_leaf = new Pair<Float, Pair<Float, Float>>(0f,
					new Pair<Float, Float>(0f, 0f));
			try {
				try {
					// get the grain ratio and division point
					score_div_leaf = Xgb4.gain_n(data, gamma,lambda,eta);
				} catch (Exception e) {
					e.printStackTrace();
				}
				PairWritable p = new PairWritable(new Text(key.toString()),
						new Text(score_div_leaf.getL().toString() + comma + score_div_leaf.getR().getL().toString()
								+ comma + score_div_leaf.getR().getR().toString()));
				///////////////////////////////////
				// System.out.println(p.toString());
				///////////////////////////////////
				context.write(NullWritable.get(), p);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static void cal_mr(String input_path, int depth, HashMap<String, Object> params) throws Exception {
		Job job = Job.getInstance();
		job.setJarByClass(Xgb4.class);
		job.setJobName("cal_mr");

		job.setMapperClass(Map_cal.class);
		job.setReducerClass(Reduce_cal.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(PairWritable.class);

		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(PairWritable.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path("output/att-" + depth));
		FileOutputFormat.setOutputPath(job, new Path("output/cal-" + depth));

		job.waitForCompletion(true);
	}

	public static class Map_cal extends Mapper<NullWritable, PairWritable, NullWritable, PairWritable> {
		public void map(NullWritable key, PairWritable value, Context context)
				throws IOException, InterruptedException {
			context.write(key, value);
		}
	}

	public static class Reduce_cal extends Reducer<NullWritable, PairWritable, NullWritable, PairWritable> {
		public void reduce(NullWritable key, Iterable<PairWritable> ps, Context context)
				throws IOException, InterruptedException {
			List<String> score_div_leaf = new ArrayList<String>();
			float max_score = -Float.MAX_VALUE;
			float score = 0f;
			PairWritable re = new PairWritable();
			for (PairWritable p : ps) {
				score_div_leaf = new LinkedList<String>(Arrays.asList(p.getR().toString().split(comma)));
				score = Float.parseFloat(score_div_leaf.get(0));
				System.out.println(p.getL().toString() + " score:" + score);
				if (score >= max_score) {
					re = new PairWritable(new Text(p.getL().toString()), new Text(p.getR().toString()));
					max_score = score;
					// System.out.println("max:"+max_score);
					// System.out.println("re:"+re.toString());
				}
			}
			context.write(key, re);
			System.out.println(re.toString());
		}
	}

	private final static Pair<Float, Pair<Float, Float>> gain_n(List<Pair<Float, Pair<Float, Float>>> data, float gamma, float lambda,float eta)
			throws Exception {
		Xgb4.sort(data);
		///////////////////////////////////
		// for (int i = 0; i < data.size(); i++) {
		// System.out.println(
		// data.get(i).getL() + comma + data.get(i).getR().getL() + comma +
		// data.get(i).getR().getR());
		// }
		////////////////////////////////////
		int data_size = data.size();
		// System.out.println("data size" +data_size);
		float res_sum = 0f;
		List<Pair<Float, Float>> cat_pred = new ArrayList<Pair<Float, Float>>();
		List<Float> cat = new ArrayList<Float>();
		List<Float> att = new ArrayList<Float>();
		for (int i = 0; i < data_size; i++) {
			cat_pred.add(data.get(i).getR());
			att.add(data.get(i).getL());
			cat.add(data.get(i).getR().getL());
			res_sum += (data.get(i).getR().getL() - data.get(i).getR().getR());
		}
		List<Pair<Float, Float>> GH = Xgb4.GH_square(cat_pred);
		float G = 0f, H = 0f, Gl = 0f, Hl = 0f, Gr = 0f, Hr = 0f;
		float score = -Float.MAX_VALUE;
		float new_OBJ = 0f;
		float div = 0f;
		float leaf = eta*(res_sum / data_size) * data_size / (data_size + 1);
		Set<Float> set_cat = new HashSet<Float>(cat);
		Set<Float> set_att = new HashSet<Float>(att);
		if (set_cat.size() == 1 || set_att.size() == 1 || data_size == 0) {
			return new Pair<Float, Pair<Float, Float>>(-1f, new Pair<Float, Float>(0f, leaf));
		}
		// if (set_att.size() == 1) {
		// return new Pair<Float, Pair<Float, Float>>(-1f, new Pair<Float, Float>(0f,
		// leaf));
		// }
		for (int i = 0; i < data_size; i++) {
			G += GH.get(i).getL();
			H += GH.get(i).getR();
		}
		float OBJ = (float) Math.pow(G, 2) / (H + lambda);
		for (int i = 0; i < data_size - 1; i++) {
			Gl += GH.get(i).getL();
			Gr = G - Gl;
			Hl += GH.get(i).getR();
			Hr = H - Hl;
			new_OBJ = (float) (Math.pow(Gl, 2) / (Hl + lambda) + Math.pow(Gr, 2) / (Hr + lambda) - OBJ - gamma);
			// System.out.println(new_OBJ);
			if (new_OBJ > score) {
				score = new_OBJ;
				div = (data.get(i).getL() + data.get(i + 1).getL()) / 2;
			}
		}
		if (div == data.get(0).getL()) {
			return new Pair<Float, Pair<Float, Float>>(-1f, new Pair<Float, Float>(0f, leaf));
		}
		return new Pair<Float, Pair<Float, Float>>(score, new Pair<Float, Float>(div, leaf));
	}

	public static void res_mr(String input_path, String xmlText, int c_round, float ini) throws Exception {
		Configuration conf = new Configuration();
		conf.setStrings("xml", xmlText);
		conf.setFloat("ini", ini);
		Job job = Job.getInstance(conf);
		job.setJarByClass(Xgb4.class);
		job.setJobName("res_mr");

		job.setMapperClass(Map_sum.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setNumReduceTasks(0);

		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path("res/" + c_round));
		job.setMapperClass(Map_res.class);
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(input_path));
		job.waitForCompletion(true);
	}

	public static class Map_res extends Mapper<LongWritable, Text, NullWritable, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String[] xmlText = conf.getStrings("xml");
			String res = "";
			float ini = conf.getFloat("ini", 0.5f);
			SAXReader reader = new SAXReader();
			Document doc = DocumentHelper.createDocument();
			try {
				doc = reader.read(new StringReader(xmlText[0]));
			} catch (DocumentException e) {
				e.printStackTrace();
			}
			Element root = doc.getRootElement();
			List<String> line = new ArrayList<String>(Arrays.asList(value.toString().split(comma)));
			String pre_cat_pred = line.remove(line.size() - 1);

			String[] cat_pred = pre_cat_pred.split(divide);
			// System.out.println(cat_pred.length);
			if (cat_pred.length > 1) {
				res = Float.toString(Float.parseFloat(Xgb4.res(root, line)) + Float.parseFloat(cat_pred[1]));
				Text re = new Text(Xgb4.join(line) + comma + cat_pred[0] + divide + res);
				context.write(NullWritable.get(), re);
			} else {
				res = Float.toString(Float.parseFloat(Xgb4.res(root, line)) + ini);
				Text re = new Text(value + divide + res);
				context.write(NullWritable.get(), re);
			}

		}
	}

	private final static String res(Element parent, List<String> obs) {
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
			// writer.println(att_value+flag+value_s);
			try {
				value = Float.parseFloat(value_s);
			} catch (NumberFormatException e) {
			}
			if ((flag.equals("l") && Float.parseFloat(att_value) < value)
					|| (flag.equals("r") && Float.parseFloat(att_value) >= value)) {
				return res(child, obs);
			}
		}
		String return_text = parent.getText();
		if (return_text.equals("")) {
			return "0";
		}
		return parent.getText();
	}

	public final static int minIndex(List<Float> list) {
		try {
			return list.indexOf(Collections.min(list));
		} catch (NoSuchElementException e) {
			return 0;
		}

	}

	public final static int maxIndex(List<Float> list) {
		try {
			return list.indexOf(Collections.max(list));
		} catch (NoSuchElementException e) {
			return 0;
		}
	}

	public final static List<Pair<Float, Float>> GH_square(List<Pair<Float, Float>> cat_pred) {
		int size = cat_pred.size();
		List<Pair<Float, Float>> re = new ArrayList<Pair<Float, Float>>();
		for (int i = 0; i < size; i++) {
			re.add(new Pair<Float, Float>(cat_pred.get(i).getR() - cat_pred.get(i).getL(), 1f));
		}
		return re;
	}

	public final static void sort(List<Pair<Float, Pair<Float, Float>>> data) {
		Collections.sort(data, new Comparator<Pair<Float, Pair<Float, Float>>>() {
			@Override
			public int compare(Pair<Float, Pair<Float, Float>> p1, Pair<Float, Pair<Float, Float>> p2) {
				return -p1.getR().getL().compareTo(p2.getR().getL());
			}
		});
		Collections.sort(data, new Comparator<Pair<Float, Pair<Float, Float>>>() {
			@Override
			public int compare(Pair<Float, Pair<Float, Float>> p1, Pair<Float, Pair<Float, Float>> p2) {
				return p1.getL().compareTo(p2.getL());
			}
		});
	}

	private final static String join(List<String> line) {
		String SEPARATOR = comma;
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
