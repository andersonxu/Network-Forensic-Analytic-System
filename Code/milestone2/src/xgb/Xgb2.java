package xgb;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
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
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.XMLWriter;

public class Xgb2 {
	public static final String divide = ":";
	public static final String comma = ",";
	public static final String space = "\\s+";
	public static FileSystem fs;

	public final static void main(String[] args) throws Exception {
		// check the number of arguments
		if (args.length != 2) {
			System.err.println("Usage: mr2/xgb2 <input path> <attribute names file path>");
			System.exit(-1);
		}
		final long startTime = System.currentTimeMillis();
		double ini = 0;
		double gamma = 0;
		double eta = 1.0;
		double lambda = 1.0;
		double alpha = 0.0;
		int max_depth = 15;
		int round = 2;
		HashMap<String, Object> params = new HashMap<String, Object>();
		params.put("ini", ini);
		params.put("gamma", gamma);
		params.put("eta", eta);
		params.put("lambda", lambda);
		params.put("alpha", alpha);
		params.put("max_depth", max_depth);
		params.put("round", round);

		fs = FileSystem.get(new Configuration());
		// delete previous output files
		fs.delete(new Path("output"), true);
		fs.delete(new Path("res"), true);
		System.out.println(args[0]);
		// read in attribute names for print off the model
		BufferedReader bReader = null;
		String name_line = "";
		try {
			bReader = new BufferedReader(new FileReader(new File(args[1])));
			name_line = bReader.readLine();
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
		Xgb2.train(args[0], params, name_line);
		// print off the training time
		final long totalTime = System.currentTimeMillis() - startTime;
		System.out.println("Total time: " + (double) totalTime / 1000);
	}

	private final static void train(String input_path, HashMap<String, Object> params, String name_line)
			throws Exception {
		File file = new File("models/");
		if (!file.exists())
			file.mkdirs();
		file = new File("res/");
		if (!file.exists())
			file.mkdirs();
		List<String> att_names = new ArrayList<String>(Arrays.asList(name_line.split(comma)));
		Document document;
		Element root;
		Document document_all = DocumentHelper.createDocument();
		Element root_all = document_all.addElement("All");
		Element att_names_ele = root_all.addElement("Att_names");
		att_names_ele.setText(Xgb2.join2(att_names));
		Element ini_ele = root_all.addElement("Ini_value");
		ini_ele.setText(params.get("ini").toString());
		XMLWriter writer;
		int depth = 0;
		String[] ini_set = new String[3];
		String orig_input_path = input_path;
		for (int c_round = 0; c_round < (int) params.get("round"); c_round++) {
			System.out.println("round starts");
			document = DocumentHelper.createDocument();
			root = document.addElement("DecisionTree");
			if (c_round > 0) {
				input_path = "res/" + (c_round - 1);
			}
			try {
				// train the model
				grow(input_path, root, depth, ini_set, att_names, params);
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
			BufferedReader bReader = null;
			BufferedWriter bw = null;
			try {
				bReader = new BufferedReader(new FileReader(new File(input_path)));
				bw = new BufferedWriter(new FileWriter("res/" + c_round));
				String l, re;
				double res;
				while ((l = bReader.readLine()) != null) {
					List<String> line = new ArrayList<String>(Arrays.asList(l.toString().split(comma)));
					String pre_cat_pred = line.remove(line.size() - 1);
					String[] cat_pred = pre_cat_pred.split(divide);
					if (cat_pred.length > 1) {
						res = Double.parseDouble(Xgb2.res(root, line, att_names)) + Double.parseDouble(cat_pred[1]);
						re = Xgb2.join(line) + comma + cat_pred[0] + divide + res;
					} else {
						res = Double.parseDouble(Xgb2.res(root, line, att_names)) + (double) params.get("ini");
						re = l + divide + res;
					}
					bw.write(re);
					bw.newLine();
				}
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {
					bReader.close();
					bw.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
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

	private final static void grow(String input_path, Element parent, int depth, String[] set, List<String> att_names,
			HashMap<String, Object> params) throws Exception {
		// depth is the depth of the tree
		depth++;
		System.out.println("depth:" + depth);
		boolean why = true;
		int max_why = 0;
		Setting settings = new Setting("0,-1.0,0.0,0");
		while (why) {
			try {
				if (max_why > 2) {
					// sometimes there will be a discI/O problem in hadoop
					System.out.println("cant fix the problem.");
					return;
				}
				max_why++;
				// summarise the data it needs
				Xgb2.sum_mr(input_path, depth, set, params);
				// calculate the gain ratio
				Xgb2.att_mr(input_path, depth, params);
				BufferedReader bReader = null;
				String line;
				List<Setting> settingsList = new ArrayList<Setting>();
				try {
					bReader = new BufferedReader(new FileReader(new File("output/att-" + depth + "/part-r-00000")));
					while ((line = bReader.readLine()) != null) {
						settingsList.add(new Setting(line));
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
				Collections.sort(settingsList, new Comparator<Setting>() {
					@Override
					public int compare(Setting p1, Setting p2) {
						Integer a1 = new Integer(p1.getAtt());
						Integer a2 = new Integer(p2.getAtt());
						return a1.compareTo(a2);
					}
				});
				Collections.sort(settingsList, new Comparator<Setting>() {
					@Override
					public int compare(Setting p1, Setting p2) {
						Double a1 = new Double(p1.getScore());
						Double a2 = new Double(p2.getScore());
						return -a1.compareTo(a2);
					}
				});
				settings = settingsList.get(0);
				why = false;
			} catch (Exception e) {
				try {
					// delete used data
					if (fs.exists(new Path("output/att-" + depth))) {
						fs.delete(new Path("output/att-" + depth), true);
						fs.delete(new Path("output/sum-" + depth), true);
					}
				} catch (FileAlreadyExistsException e2) {
					e.printStackTrace();
				}
				e.printStackTrace();
			}
		}
		// if can't go deeper terminate growing
		if (settings.getScore() <= 0.0 || depth > (int) params.get("max_depth")) {
			parent.setText(settings.getR());
		} else {
			String[] new_set = new String[3];
			// if this attribute is numeric go to left and right children
			new_set[0] = Integer.toString(settings.getAtt());
			new_set[1] = "l";
			new_set[2] = Double.toString(settings.getP());
			Element sonl, sonr;
			try {
				sonl = parent.addElement(att_names.get(settings.getAtt()));
				sonr = parent.addElement(att_names.get(settings.getAtt()));
			} catch (IndexOutOfBoundsException e) {
				sonl = parent.addElement("_" + Integer.toString(settings.getAtt()));
				sonr = parent.addElement("_" + Integer.toString(settings.getAtt()));
			}
			// Element sonl = parent.addElement(att_names.get(settings.getAtt()));
			sonl.addAttribute("flag", "l");
			sonl.addAttribute("value", String.format("%.04f", settings.getP()));
			grow(input_path, sonl, depth, new_set, att_names, params);
			new_set[1] = "r";
			// Element sonr = parent.addElement(att_names.get(settings.getAtt()));
			sonr.addAttribute("flag", "r");
			sonr.addAttribute("value", String.format("%.04f", settings.getP()));
			grow(input_path, sonr, depth, new_set, att_names, params);
		}

		try {
			// delete used data
			if (fs.exists(new Path("output/att-" + depth))) {
				fs.delete(new Path("output/att-" + depth), true);
				fs.delete(new Path("output/sum-" + depth), true);
			}
		} catch (FileAlreadyExistsException e) {
			e.printStackTrace();
		}
	}

	public static void sum_mr(String input_path, int depth, String[] set, HashMap<String, Object> params)
			throws Exception {
		Configuration conf = new Configuration();
		conf.setStrings("conf", set);
		conf.setDouble("ini", (double) params.get("ini"));
		Job job = Job.getInstance(conf);
		job.setJarByClass(Xgb2.class);
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
			double ini = conf.getDouble("ini", 0.5f);
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
			boolean l_r = Double.parseDouble(line[Integer.parseInt(set[0])]) < Double.parseDouble((set[2]));
			if (((set[1].equals("l") && l_r) || ((set[1].equals("r") && !l_r)))) {
				context.write(NullWritable.get(), value);
			}
		}
	}

	public static void att_mr(String input_path, int depth, HashMap<String, Object> params) throws Exception {
		Configuration conf = new Configuration();
		conf.setDouble("gamma", (double) params.get("gamma"));
		conf.setDouble("lambda", (double) params.get("lambda"));
		conf.setDouble("eta", (double) params.get("eta"));
		conf.setDouble("alpha", (double) params.get("alpha"));
		Job job = Job.getInstance(conf);
		job.setJarByClass(Xgb2.class);
		job.setJobName("att_mr");

		job.setMapperClass(Map_att.class);
		job.setReducerClass(Reduce_att.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(PairWritable.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(PairWritable.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

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
			double gamma = conf.getDouble("gamma", 0.0);
			double lambda = conf.getDouble("lambda", 1.0);
			double eta = conf.getDouble("eta", 1.0);
			double alpha = conf.getDouble("alpha", 0.0);
			// preparing the data
			List<Pair<Double, Pair<Double, Double>>> data = new ArrayList<Pair<Double, Pair<Double, Double>>>();
			for (PairWritable p : ps) {
				String[] cat_pred = p.getR().toString().split(divide);
				double att = Double.parseDouble(p.getL().toString());
				double cat = Double.parseDouble(cat_pred[0]);
				double pred = Double.parseDouble(cat_pred[1]);
				data.add(new Pair<Double, Pair<Double, Double>>(att, new Pair<Double, Double>(cat, pred)));
			}
			Pair<Double, Pair<Double, Double>> score_div_leaf = new Pair<Double, Pair<Double, Double>>(0.0,
					new Pair<Double, Double>(0.0, 0.0));
			try {
				try {
					// get the grain ratio and division point
					score_div_leaf = Xgb2.gain_n(data, gamma, lambda, alpha, eta);
				} catch (Exception e) {
					e.printStackTrace();
				}
				PairWritable p = new PairWritable(new Text(key.toString()),
						new Text(score_div_leaf.getL().toString() + comma + score_div_leaf.getR().getL().toString()
								+ comma + score_div_leaf.getR().getR().toString()));
				System.out.println(p.toString());
				context.write(NullWritable.get(), p);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private final static Pair<Double, Pair<Double, Double>> gain_n(List<Pair<Double, Pair<Double, Double>>> data,
			double gamma, double lambda, double alpha, double eta) throws Exception {
		Xgb2.sort(data);
		int data_size = data.size();
		// System.out.println("data size" +data_size);
		double res_sum = 0.0;
		List<Pair<Double, Double>> cat_pred = new ArrayList<Pair<Double, Double>>();
		List<Double> cat = new ArrayList<Double>();
		List<Double> att = new ArrayList<Double>();
		for (int i = 0; i < data_size; i++) {
			cat_pred.add(data.get(i).getR());
			att.add(data.get(i).getL());
			cat.add(data.get(i).getR().getL() - data.get(i).getR().getR());
			res_sum += cat.get(i);
		}
		List<Pair<Double, Double>> GH = Xgb2.GH_square(cat_pred);
		double G = 0.0, H = 0.0, Gl = 0.0, Hl = 0.0, Gr = 0.0, Hr = 0.0;
		double score = -Double.MAX_VALUE;
		double new_OBJ = 0.0;
		double div = 0.0;
		double leaf = Xgb2.round(eta * res_sum / (data_size + 1), 8);
		Set<Double> set_cat = new HashSet<Double>(cat);
		Set<Double> set_att = new HashSet<Double>(att);
		if (set_cat.size() == 1 || set_att.size() == 1 || data_size == 0) {
			return new Pair<Double, Pair<Double, Double>>(-1.0, new Pair<Double, Double>(0.0, leaf));
		}
		for (int i = 0; i < data_size; i++) {
			G += GH.get(i).getL();
			H += GH.get(i).getR();
		}
		double OBJ = (double) Math.pow(G + alpha, 2) / (H + lambda);
		for (int i = 0; i < data_size - 1; i++) {
			Gl += GH.get(i).getL();
			Gr = G - Gl;
			Hl += GH.get(i).getR();
			Hr = H - Hl;
			if (Double.compare(data.get(i).getL(), data.get(i + 1).getL()) != 0) {
				new_OBJ = Xgb2.round((double) (Math.pow(Gl + alpha, 2) / (Hl + lambda)
						+ Math.pow(Gr + alpha, 2) / (Hr + lambda) - OBJ - gamma), 6);
				if (new_OBJ > score) {
					score = new_OBJ;
					div = Xgb2.round((data.get(i).getL() + data.get(i + 1).getL()) / 2, 4);
				}
			}
		}
		if ((div == data.get(0).getL()) || (div == data.get(data_size - 1).getL())) {
			return new Pair<Double, Pair<Double, Double>>(-1.0, new Pair<Double, Double>(0.0, leaf));
		}
		return new Pair<Double, Pair<Double, Double>>(score, new Pair<Double, Double>(div, leaf));
	}

	private final static String res(Element parent, List<String> obs, List<String> att_names) {
		Iterator<Element> it = parent.elementIterator();
		String att_value = "";
		String flag = "";
		double value = 0.0;
		String value_s = "";
		while (it.hasNext()) {
			Element child = (Element) it.next();
			try {
				att_value = obs.get(att_names.indexOf(child.getName()));
			} catch (ArrayIndexOutOfBoundsException e) {
				att_value = obs.get(Integer.parseInt(child.getName().substring(1)));
			}
			flag = child.attribute("flag").getValue();
			value_s = child.attribute("value").getValue();
			try {
				value = Double.parseDouble(value_s);
			} catch (NumberFormatException e) {
			}
			if ((flag.equals("l") && Double.parseDouble(att_value) < value)
					|| (flag.equals("r") && Double.parseDouble(att_value) >= value)) {
				return res(child, obs, att_names);
			}
		}
		String return_text = parent.getText();
		if (return_text.equals("")) {
			return "0";
		}
		return parent.getText();
	}

	public final static List<Pair<Double, Double>> GH_square(List<Pair<Double, Double>> cat_pred) {
		int size = cat_pred.size();
		List<Pair<Double, Double>> re = new ArrayList<Pair<Double, Double>>();
		for (int i = 0; i < size; i++) {
			re.add(new Pair<Double, Double>(cat_pred.get(i).getR() - cat_pred.get(i).getL(), 1.0));
		}
		return re;
	}

	public final static void sort(List<Pair<Double, Pair<Double, Double>>> data) {
		Collections.sort(data, new Comparator<Pair<Double, Pair<Double, Double>>>() {
			@Override
			public int compare(Pair<Double, Pair<Double, Double>> p1, Pair<Double, Pair<Double, Double>> p2) {
				return -p1.getR().getL().compareTo(p2.getR().getL());
			}
		});
		Collections.sort(data, new Comparator<Pair<Double, Pair<Double, Double>>>() {
			@Override
			public int compare(Pair<Double, Pair<Double, Double>> p1, Pair<Double, Pair<Double, Double>> p2) {
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

	private final static String join2(List<String> line) {
		String SEPARATOR = divide;
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

	private final static double round(double value, int places) {
		if (places < 0)
			throw new IllegalArgumentException();
		long factor = (long) Math.pow(10, places);
		value = value * factor;
		long tmp = Math.round(value);
		return (double) tmp / factor;
	}

}
