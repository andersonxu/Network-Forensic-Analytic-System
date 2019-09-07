package c45mr;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
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
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.XMLWriter;

import c45mr.Pair;
import c45mr.Setting;
import xgb.Xgb2;

public class C45 {
	// public static List<String> att_names;
	public static final String divide = "-";
	public static final String comma = ",";
	public static final String numeric = "n";
	public static final String discrete = "d";
	public static final String space = "\\s+";
	public static final int percentile = 50;
	public static FileSystem fs;
	public static String training_time;

	public final static void main(String[] args) throws Exception {
		// check the number of arguments
		if (args.length != 2) {
			System.err.println("Usage: mr/C45 <input path> <attribute names file path>");
			System.exit(-1);
		}
		final long startTime = System.currentTimeMillis();
		fs = FileSystem.get(new Configuration());
		// delete previous output files
		fs.delete(new Path("output"), true);
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
		C45.train(args[0], args[0] + "model.xml", name_line);
		// print off the training time
		final long totalTime = System.currentTimeMillis() - startTime;
		training_time = "" + (double) totalTime / 1000;
		System.out.println("Total time: " + (double) totalTime / 1000);
	}

	private final static void train(String input_path, String xml_name, String name_line) throws Exception {
		List<String> att_names = new ArrayList<String>(Arrays.asList(name_line.split(comma)));
		Document document = DocumentHelper.createDocument();
		Element root_all = document.addElement("All");
		Element att_names_ele = root_all.addElement("Att_names");
		att_names_ele.setText(C45.join2(att_names));
		Element root = root_all.addElement("DecisionTree");
		int max_depth = 25;
		int depth = 0;
		String[] ini_set = new String[3];
		String ini_cat = "normal.";
		try {
			// train the model
			grow(input_path, root, depth, max_depth, ini_set, att_names, ini_cat);
		} catch (OutOfMemoryError e) {
			System.out.println("Out of memory");
		}
		// print off the model
		File file = new File(xml_name);
		XMLWriter writer;
		try {
			writer = new XMLWriter(new FileOutputStream(file));
			writer.setEscapeText(false);
			writer.write(document);
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private final static void grow(String input_path, Element parent, int depth, int max_depth, String[] set,
			List<String> att_names, String cat) throws Exception {
		// depth is the depth of the tree
		depth++;
		boolean why = true;
		int max_why = 0;
		Setting settings;
		List<Setting> settingsList = new ArrayList<Setting>();
		while (why) {
			try {
				if (max_why > 2) {
					// sometimes there will be a discI/O problem in hadoop
					System.out.println("cant fix the problem.");
					System.exit(1);
				}
				max_why++;
				// summarise the data it needs
				C45.sum_mr(input_path, depth, set);
				if (depth > max_depth) {
					C45.cal_mr(input_path, depth);
				} else {
					// calculate the gain ratio
					C45.att_mr(input_path, depth);
				}
				BufferedReader bReader = null;
				String line;
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
				double avg_gain = 0.0;
				for (int i = 0; i < settingsList.size(); i++) {
					avg_gain += settingsList.get(i).getG();
				}
				avg_gain /= settingsList.size();
				for (int i = 0; i < settingsList.size(); i++) {
					if (settingsList.get(i).getG() < avg_gain) {
						settingsList.remove(i);
						i--;
					}
				}
				Collections.sort(settingsList, new Comparator<Setting>() {
					@Override
					public int compare(Setting p1, Setting p2) {
						Double a1 = new Double(p1.getEnt());
						Double a2 = new Double(p2.getEnt());
						return -a1.compareTo(a2);
					}
				});
				why = false;
			} catch (Exception e) {
				try {
					// delete used data
					if (fs.exists(new Path("output/att-" + depth))) {
						fs.delete(new Path("output/att-" + depth), true);
					}
					if (fs.exists(new Path("output/sum-" + depth))) {
						fs.delete(new Path("output/sum-" + depth), true);
					}
				} catch (FileAlreadyExistsException e2) {
					e.printStackTrace();
				}
				e.printStackTrace();
			}
		}
		try {
			settings = settingsList.get(0);
			if (settings.getEnt() == 0.0 || depth > max_depth) {
				parent.setText(settings.getCat());
			} else {
				String[] new_set = new String[3];
				// if this attribute is numeric go to left and right children
				if (settings.is_num()) {
					new_set[0] = Integer.toString(settings.getAtt());
					new_set[1] = "l";
					new_set[2] = Double.toString(settings.getP());
					Element sonl = parent.addElement(att_names.get(settings.getAtt()));
					sonl.addAttribute("flag", "l");
					sonl.addAttribute("value", String.format("%.04f", settings.getP()));
					grow(input_path, sonl, depth, max_depth, new_set, att_names, settings.getCat());
					new_set[1] = "r";
					Element sonr = parent.addElement(att_names.get(settings.getAtt()));
					sonr.addAttribute("flag", "r");
					sonr.addAttribute("value", String.format("%.04f", settings.getP()));
					grow(input_path, sonr, depth, max_depth, new_set, att_names, settings.getCat());
				} else {
					// if this attribute is discrete, go to each possible value
					for (String att : settings.getAtts()) {
						new_set[0] = Integer.toString(settings.getAtt());
						new_set[1] = "m";
						new_set[2] = att;
						System.out.println(att);
						Element son = parent.addElement(att_names.get(settings.getAtt()));
						son.addAttribute("flag", "m");
						son.addAttribute("value", att);
						grow(input_path, son, depth, max_depth, new_set, att_names, settings.getCat());
					}
				}
			}
		} catch (IndexOutOfBoundsException e) {
			parent.setText("unknown");
		}
		why = true;
		max_why = 0;
		while (why) {
			try {
				// delete used data
				if (fs.exists(new Path("output/att-" + depth))) {
					fs.delete(new Path("output/att-" + depth), true);
				}
				if (fs.exists(new Path("output/sum-" + depth))) {
					fs.delete(new Path("output/sum-" + depth), true);
				}
				if (max_why > 2) {
					// sometimes there will be a discI/O problem in hadoop
					System.out.println("cant fix the problem.");
					System.exit(1);
				}
				why = false;
				max_why++;
			} catch (FileAlreadyExistsException e) {
				e.printStackTrace();
			}
		}

	}

	public static void cal_mr(String input_path, int depth) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(C45.class);
		job.setJobName("cal_mr");

		job.setMapperClass(Map_cal.class);
		job.setReducerClass(Reduce_cal.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path("output/att-" + depth));
		FileInputFormat.addInputPath(job, new Path("output/sum-" + depth));

		job.waitForCompletion(true);
	}

	public static class Map_cal extends Mapper<NullWritable, Text, Text, IntWritable> {
		public void map(NullWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] line = value.toString().split(comma);
			Text re = new Text(line[line.length - 1]);
			context.write(re, new IntWritable(1));
		}
	}

	public static class Reduce_cal extends Reducer<Text, IntWritable, NullWritable, Text> {
		public void reduce(Text key, Iterable<IntWritable> vs, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable v : vs) {
				sum += v.get();
			}
			Text re = new Text("1" + comma + discrete + comma + sum + comma + sum + comma + key + comma + "0");
			System.out.println(re.toString());
			context.write(NullWritable.get(), re);
		}
	}

	public static void sum_mr(String input_path, int depth, String[] set) throws Exception {
		Configuration conf = new Configuration();
		conf.setStrings("conf", set);
		Job job = Job.getInstance(conf);
		job.setJarByClass(C45.class);
		job.setJobName("sum_mr");

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
			job.setMapperClass(Map_sum2.class);
			job.setInputFormatClass(SequenceFileInputFormat.class);
			FileInputFormat.addInputPath(job, new Path("output/sum-" + depth));
		} else {
			job.setMapperClass(Map_sum.class);
			job.setInputFormatClass(TextInputFormat.class);
			FileInputFormat.addInputPath(job, new Path(input_path));
		}
		job.waitForCompletion(true);
	}

	public static class Map_sum extends Mapper<LongWritable, Text, NullWritable, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String[] set = conf.getStrings("conf");
			String[] line = value.toString().split(comma);
			if (set[0].equals("null")) {
				context.write(NullWritable.get(), value);
			} else if ((set[1].equals("m") && (line[Integer.parseInt(set[0])].equals(set[2]))) || ((set[1].equals("l")
					&& (Double.parseDouble(line[Integer.parseInt(set[0])]) < Double.parseDouble((set[2]))))
					|| ((set[1].equals("r") && (Double.parseDouble(line[Integer.parseInt(set[0])]) >= Double
							.parseDouble((set[2]))))))) {
				// emit if this is the line we need
				context.write(NullWritable.get(), value);
			}
		}
	}

	public static class Map_sum2 extends Mapper<NullWritable, Text, NullWritable, Text> {
		public void map(NullWritable key, Text value, Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String[] set = conf.getStrings("conf");
			String[] line = value.toString().split(comma);
			if (set[0].equals("null")) {
				context.write(NullWritable.get(), value);
			} else if ((set[1].equals("m") && (line[Integer.parseInt(set[0])].equals(set[2]))) || ((set[1].equals("l")
					&& (Double.parseDouble(line[Integer.parseInt(set[0])]) < Double.parseDouble((set[2]))))
					|| ((set[1].equals("r") && (Double.parseDouble(line[Integer.parseInt(set[0])]) >= Double
							.parseDouble((set[2]))))))) {
				context.write(NullWritable.get(), value);
			}
		}
	}

	public static void att_mr(String input_path, int depth) throws Exception {
		Job job = Job.getInstance();
		job.setJarByClass(C45.class);
		job.setJobName("att_mr");

		job.setMapperClass(Map_att.class);
		job.setReducerClass(Reduce_att.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

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
			String cat = line.get(line.size() - 1);
			PairWritable p;
			for (int i = 0; i < line.size() - 1; i++) {
				p = new PairWritable(new Text(line.get(i)), new Text(cat));
				context.write(new IntWritable(i), p);
			}
		}
	}

	public static class Reduce_att extends Reducer<IntWritable, PairWritable, NullWritable, Text> {
		public void reduce(IntWritable key, Iterable<PairWritable> ps, Context context)
				throws IOException, InterruptedException {
			// preparing the data
			List<String> data = new ArrayList<String>();
			List<String> all_att = new ArrayList<String>();
			for (PairWritable p : ps) {
				all_att.add(p.getL().toString());
				data.add(p.getL().toString() + divide + p.getR().toString());
			}
			String[] att_cla;
			Pair<Pair<Double, Double>, String> div_div_p = new Pair<Pair<Double, Double>, String>(
					new Pair<Double, Double>(0.0, 0.0), "0,0");
			Text re;
			System.out.println(all_att.size());
			if (all_att.size() == 0) {
				re = new Text(key.toString() + comma + numeric + comma + div_div_p.getL().getL().toString() + comma
						+ div_div_p.getL().getR().toString() + comma + div_div_p.getR());
				System.out.println(re.toString());
				context.write(NullWritable.get(), re);
				return;
			}
			try {
				if (C45.is_num(all_att)) {
					List<Pair<Double, String>> cats = new ArrayList<Pair<Double, String>>();
					Pair<Double, String> a;
					for (int i = 0; i < data.size(); i++) {
						att_cla = data.get(i).split(divide);
						a = new Pair<Double, String>(Double.parseDouble(att_cla[0]), att_cla[1]);
						cats.add(a);
					}
					try {
						// get the grain ratio and division point
						div_div_p = C45.gain_n(cats);
					} catch (Exception e) {
						e.printStackTrace();
					}
					re = new Text(key.toString() + comma + numeric + comma + div_div_p.getL().getL().toString() + comma
							+ div_div_p.getL().getR().toString() + comma + div_div_p.getR());
					System.out.println(re.toString());
					context.write(NullWritable.get(), re);
				} else {
					List<String> cat = new ArrayList<String>();
					List<String> att = new ArrayList<String>();
					for (int i = 0; i < data.size(); i++) {
						att_cla = data.get(i).split(divide);
						att.add(att_cla[0]);
						cat.add(att_cla[1]);
					}
					try {
						// get the grain ratio and division point
						div_div_p = C45.gain_s(cat, att);
					} catch (Exception e) {
						e.printStackTrace();
					}
					re = new Text(key.toString() + comma + discrete + comma + div_div_p.getL().getL().toString() + comma
							+ div_div_p.getL().getR().toString() + comma + div_div_p.getR());
					System.out.println(re.toString());
					context.write(NullWritable.get(), re);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private final static Pair<Pair<Double, Double>, String> gain_n(List<Pair<Double, String>> cats) throws Exception {
		Collections.sort(cats, new Comparator<Pair<Double, String>>() {
			@Override
			public int compare(Pair<Double, String> p1, Pair<Double, String> p2) {
				return p1.getR().compareTo(p2.getR());
			}
		});
		Collections.sort(cats, new Comparator<Pair<Double, String>>() {
			@Override
			public int compare(Pair<Double, String> p1, Pair<Double, String> p2) {
				return p1.getL().compareTo(p2.getL());
			}
		});
		List<String> new_cat = new ArrayList<String>();
		List<Double> new_att = new ArrayList<Double>();
		String re_String;
		for (int i = 0; i < cats.size(); i++) {
			new_cat.add(cats.get(i).getR());
			new_att.add(cats.get(i).getL());
		}
		Set<Double> set_att = new HashSet<Double>(new_att);
		int set_att_size = set_att.size();
		int sel = percentile;
		// return the most possible classification if can't divide the data
		if (set_att_size == 1) {
			// if (set_att_size == 1 || sel == 0.) {
			int num_max = 0;
			int num_cat = 0;
			String most_cat = "";
			Set<String> set_cat = new HashSet<String>(new_cat);
			for (String cat : set_cat) {
				num_cat = Collections.frequency(new_cat, cat);
				if (num_cat > num_max) {
					num_max = num_cat;
					most_cat = cat;
				}
			}
			re_String = most_cat + comma + new_att.get(0);
			return new Pair<Pair<Double, Double>, String>(new Pair<Double, Double>(0.0, 0.0), re_String);
		}

		Set<String> set_cat = new HashSet<String>(new_cat);
		if (set_cat.size() == 1) {
			return new Pair<Pair<Double, Double>, String>(new Pair<Double, Double>(0.0, 0.0),
					new_cat.get(0) + comma + "0");
		}
		final int cat_size = new_cat.size();
		List<Double> gains = new ArrayList<Double>();
		List<Integer> div_point = new ArrayList<Integer>();
		List<Double> sel_att = new ArrayList<Double>();
		List<Double> new_set_att = new ArrayList<Double>(set_att);
		Collections.sort(new_set_att, new Comparator<Double>() {
			@Override
			public int compare(Double p1, Double p2) {
				return p1.compareTo(p2);
			}
		});
		if (sel > 0) {
			if (set_att_size > sel) {
				for (int i = 0; i < sel; i++) {
					sel_att.add(new_set_att.get((int) Math.ceil(set_att_size * i / sel)));
					// System.out.println((int) Math.ceil(set_att_size * i / sel));
				}
			} else {
				sel_att = new_set_att;
			}
			Set<Double> set_sel_att = new HashSet<Double>(sel_att);
			sel_att = new ArrayList<Double>(set_sel_att);
			Collections.sort(sel_att, new Comparator<Double>() {
				@Override
				public int compare(Double p1, Double p2) {
					return p1.compareTo(p2);
				}
			});
			// System.out.println(new_att);
			System.out.println(sel_att);
			for (int i = 1, j = 0; i < cat_size && j < sel_att.size(); i++) {
				if ((!new_att.get(i).equals(new_att.get(i - 1))) && ((new_att.get(i - 1).equals(sel_att.get(j))))) {
					gains.add(entropy(new_cat.subList(0, i)) * (double) i / cat_size
							+ entropy(new_cat.subList(i, cat_size)) * (1 - (double) i / cat_size));
					div_point.add(i);
					// System.out.println(j);
					j++;
				}
			}
		} else {
			for (int i = 1; i < cat_size; i++) {
				if ((!new_att.get(i).equals(new_att.get(i - 1))) && (!new_cat.get(i).equals(new_cat.get(i - 1)))) {
					gains.add(entropy(new_cat.subList(0, i)) * (double) i / cat_size
							+ entropy(new_cat.subList(i, cat_size)) * (1 - (double) i / cat_size));
					div_point.add(i);
					// System.out.println(i + " " + new_att.get(i) + ":" + new_att.get(i - 1) + " "
					// + new_cat.get(i) + ":"
					// + new_cat.get(i - 1));
					// System.out.println(j);
				}
			}
			if (gains.size() == 0) {
				for (int i = 1; i < cat_size; i++) {
					if ((!new_att.get(i).equals(new_att.get(i - 1)))) {
						gains.add(entropy(new_cat.subList(0, i)) * (double) i / cat_size
								+ entropy(new_cat.subList(i, cat_size)) * (1 - (double) i / cat_size));
						div_point.add(i);
					}
				}
			}
		}
		final int min_index_gains = minIndex(gains);
		double division_p;
		// System.out.println(min_index_gains);
		// System.out.println(div_point.get(min_index_gains));
		// System.out.println(
		// (new_att.get(div_point.get(min_index_gains)) + ":" +
		// new_att.get(div_point.get(min_index_gains) - 1)));
		try {
			division_p = ((new_att.get(div_point.get(min_index_gains))
					+ new_att.get(div_point.get(min_index_gains) - 1)) / 2);
		} catch (IndexOutOfBoundsException e) {
			System.out.println("can't find div_p");
			division_p = new_att.get(0);
		}
		String cla;
		try {
			cla = new_cat.get(div_point.get(min_index_gains));
		} catch (IndexOutOfBoundsException e) {
			System.out.println("can't find div_p");
			cla = new_cat.get(0);
		}
		final double min_gains = gains.get(min_index_gains);
		final double gain = entropy(new_cat) - min_gains;
		final double p_1 = (double) div_point.get(min_index_gains) / cat_size;
		final double ent_att = -p_1 * (double) Math.log(p_1) - (1 - p_1) * (double) Math.log(1 - p_1);
		re_String = cla + comma + Double.toString(division_p);
		return new Pair<Pair<Double, Double>, String>(new Pair<Double, Double>(gain, gain / ent_att), re_String);
	}

	private final static Pair<Pair<Double, Double>, String> gain_s(List<String> categories, List<String> att)
			throws Exception {
		double s = 0.0;
		List<String> new_cat = new ArrayList<String>();
		List<String> new_att = new ArrayList<String>();
		for (int i = 0; i < att.size(); i++) {
			if (!att.get(i).equals("?")) {
				new_att.add(att.get(i));
				new_cat.add(categories.get(i));
			}
		}
		final int att_size = new_att.size();
		final int cat_size = new_cat.size();
		String re_String;
		Set<String> set_att = new HashSet<String>(att);
		List<String> atts = new ArrayList<String>(set_att);
		Set<String> set_cat = new HashSet<String>(new_cat);
		if (set_cat.size() == 1) {
			return new Pair<Pair<Double, Double>, String>(new Pair<Double, Double>(0.0, 0.0),
					new_cat.get(0) + comma + "0");
		}
		// return the most possible classification if can't divide the data
		if (set_att.size() == 1) {
			int num_max = 0;
			int num_cat = 0;
			String most_cat = "";
			set_cat = new HashSet<String>(categories);
			for (String cat : set_cat) {
				num_cat = Collections.frequency(categories, cat);
				if (num_cat > num_max) {
					num_max = num_cat;
					most_cat = cat;
				}
			}
			re_String = most_cat + comma + C45.join(atts);
			return new Pair<Pair<Double, Double>, String>(new Pair<Double, Double>(0.0, 0.0), re_String);
		}
		double p_i = 0.0;
		for (String i : set_att) {
			System.out.println(i);
			p_i = (double) Collections.frequency(att, i) / att_size;
			List<String> cat_i = new ArrayList<String>();
			for (int j = 0; j < cat_size; j++) {
				if (new_att.get(j).equals(i)) {
					cat_i.add(new_cat.get(j));
				}
			}
			s = s + p_i * entropy(cat_i);
		}
		final double gain = entropy(new_cat) - s;
		final double ent_att = entropy(new_att);

		if (ent_att == 0) {
			int num_max = 0;
			int num_cat = 0;
			String most_cat = "";
			set_cat = new HashSet<String>(categories);
			for (String cat : set_cat) {
				num_cat = Collections.frequency(categories, cat);
				if (num_cat > num_max) {
					num_max = num_cat;
					most_cat = cat;
				}
			}
			re_String = most_cat + comma + C45.join(atts);
			return new Pair<Pair<Double, Double>, String>(new Pair<Double, Double>(0.0, 0.0), re_String);
		} else {
			re_String = new_cat.get(0) + comma + C45.join(atts);
			return new Pair<Pair<Double, Double>, String>(new Pair<Double, Double>(gain, gain / ent_att), re_String);
		}
	}

	private final static double entropy(List<String> x) throws Exception {
		Set<String> set_cat = new HashSet<String>(x);
		int x_size = x.size();
		double ent = 0.0;
		double p_i = 0.0;
		for (String k : set_cat) {
			p_i = (double) Collections.frequency(x, k) / x_size;
			ent = ent - p_i * (double) Math.log(p_i);
		}
		return ent;
	}

	public final static int minIndex(List<Double> list) {
		try {
			return list.indexOf(Collections.min(list));
		} catch (NoSuchElementException e) {
			return 0;
		}

	}

	public final static int maxIndex(List<Double> list) {
		try {
			return list.indexOf(Collections.max(list));
		} catch (NoSuchElementException e) {
			return 0;
		}
	}

	private final static boolean is_num(List<String> att) throws Exception {
		Set<String> set = new HashSet<String>(att);
		for (String x : set) {
			if (!x.equals("?")) {
				try {
					Double.parseDouble(x);
				} catch (NumberFormatException e) {
					return false;
				}
			}
		}
		return true;
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

}
