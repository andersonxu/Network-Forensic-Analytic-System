package mr;

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

import mr.Setting;

public class C45 {
	public static List<String> att_names;
	public static final String divide = "-";
	public static final String comma = ",";
	public static final String numeric = "n";
	public static final String discrete = "d";
	public static final String space = "\\s+";
	public static final int percentile = 2;
	public static FileSystem fs;

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
		try {
			bReader = new BufferedReader(new FileReader(new File(args[1])));
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
		// train the model
		C45.train(args[0], args[0] + "model.xml");
		// print off the training time
		final long totalTime = System.currentTimeMillis() - startTime;
		System.out.println("Total time: " + (float) totalTime / 1000);
	}

	private final static void train(String input_path, String xml_name) throws Exception {
		Document document = DocumentHelper.createDocument();
		Element root = document.addElement("DecisionTree");
		int max_depth = 100;
		int depth = 0;
		String[] ini_set = new String[3];
		try {
			// train the model
			grow(input_path, root, depth, max_depth, ini_set);
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

	private final static void grow(String input_path, Element parent, int depth, int max_depth, String[] set)
			throws Exception {
		// depth is the depth of the tree
		depth++;
		// summarise the data it needs
		C45.sum_mr(input_path, depth, set);
		// calculate the gain ratio
		C45.att_mr(input_path, depth);
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
		float avg_gain = 0f;
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
		Setting settings = settingsList.get(0);
		if (settings.getEnt() == 0f || depth > max_depth) {
			parent.setText(settings.getCat());
		} else {
			String[] new_set = new String[3];
			// if this attribute is numeric go to left and right children
			if (settings.is_num()) {
				new_set[0] = Integer.toString(settings.getAtt());
				new_set[1] = "l";
				new_set[2] = Float.toString(settings.getP());
				Element sonl = parent.addElement(att_names.get(settings.getAtt()));
				sonl.addAttribute("flag", "l");
				sonl.addAttribute("value", String.format("%.04f", settings.getP()));
				grow(input_path, sonl, depth, max_depth, new_set);
				new_set[1] = "r";
				Element sonr = parent.addElement(att_names.get(settings.getAtt()));
				sonr.addAttribute("flag", "r");
				sonr.addAttribute("value", String.format("%.04f", settings.getP()));
				grow(input_path, sonr, depth, max_depth, new_set);
			} else {
				// if this attribute is discrete, go to each possible value
				for (String att : settings.getAtts()) {
					new_set[0] = Integer.toString(settings.getAtt());
					new_set[1] = "m";
					new_set[2] = att;
					Element son = parent.addElement(att_names.get(settings.getAtt()));
					son.addAttribute("flag", "m");
					son.addAttribute("value", att);
					grow(input_path, son, depth, max_depth, new_set);
				}
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

	public static void sum_mr(String input_path, int depth, String[] set) throws Exception {
		Configuration conf = new Configuration();
		conf.setStrings("conf", set);
		Job job = Job.getInstance(conf);
		job.setJarByClass(C45.class);
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
					&& (Float.parseFloat(line[Integer.parseInt(set[0])]) < Float.parseFloat((set[2]))))
					|| ((set[1].equals("r")
							&& (Float.parseFloat(line[Integer.parseInt(set[0])]) >= Float.parseFloat((set[2]))))))) {
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
					&& (Float.parseFloat(line[Integer.parseInt(set[0])]) < Float.parseFloat((set[2]))))
					|| ((set[1].equals("r")
							&& (Float.parseFloat(line[Integer.parseInt(set[0])]) >= Float.parseFloat((set[2]))))))) {
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
			Pair<Pair<Float, Float>, String> div_div_p = new Pair<Pair<Float, Float>, String>(
					new Pair<Float, Float>(0f, 0f), "");
			Text re;
			try {
				if (C45.is_num(all_att)) {
					List<Pair<Float, String>> cats = new ArrayList<Pair<Float, String>>();
					Pair<Float, String> a;
					for (int i = 0; i < data.size(); i++) {
						att_cla = data.get(i).split(divide);
						a = new Pair<Float, String>(Float.parseFloat(att_cla[0]), att_cla[1]);
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

	private final static Pair<Pair<Float, Float>, String> gain_n(List<Pair<Float, String>> cats) throws Exception {
		Collections.sort(cats, new Comparator<Pair<Float, String>>() {
			@Override
			public int compare(Pair<Float, String> p1, Pair<Float, String> p2) {
				return p1.getL().compareTo(p2.getL());
			}
		});
		List<String> new_cat = new ArrayList<String>();
		List<Float> new_att = new ArrayList<Float>();
		String re_String;
		for (int i = 0; i < cats.size(); i++) {
			new_cat.add(cats.get(i).getR());
			new_att.add(cats.get(i).getL());
		}
		Set<Float> set_att = new HashSet<Float>(new_att);
		int set_att_size = set_att.size();
		// return the most possible classification if can't divide the data
		if (set_att_size == 1) {
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
			return new Pair<Pair<Float, Float>, String>(new Pair<Float, Float>(0f, 0f), re_String);
		}
		Set<String> set_cat = new HashSet<String>(new_cat);
		if (set_cat.size() == 1) {
			return new Pair<Pair<Float, Float>, String>(new Pair<Float, Float>(0f, 0f), new_cat.get(0) + comma + "0");
		}
		final int cat_size = new_cat.size();
		List<Float> gains = new ArrayList<Float>();
		List<Integer> div_point = new ArrayList<Integer>();
		int sel = percentile;
		List<Float> sel_att = new ArrayList<Float>();
		List<Float> new_set_att = new ArrayList<Float>(set_att);
		Collections.sort(new_set_att, new Comparator<Float>() {
			@Override
			public int compare(Float p1, Float p2) {
				return p1.compareTo(p2);
			}
		});
		if (set_att_size > sel) {
			for (float i = 0; i < sel; i++) {
				sel_att.add(new_set_att.get((int) Math.ceil(set_att_size * i / sel)));
				// System.out.println((int) Math.ceil(set_att_size * i / sel));
			}
		} else {
			sel_att = new_set_att;
		}
		Set<Float> set_sel_att = new HashSet<Float>(sel_att);
		sel_att = new ArrayList<Float>(set_sel_att);
		Collections.sort(sel_att, new Comparator<Float>() {
			@Override
			public int compare(Float p1, Float p2) {
				return p1.compareTo(p2);
			}
		});
		for (int i = 1, j = 0; i < cat_size && j < sel_att.size(); i++) {
			if ((!new_att.get(i).equals(new_att.get(i - 1))) && ((new_att.get(i - 1).equals(sel_att.get(j))))) {
				gains.add(entropy(new_cat.subList(0, i)) * (float) i / cat_size
						+ entropy(new_cat.subList(i, cat_size)) * (1 - (float) i / cat_size));
				div_point.add(i);
				// System.out.println(j);
				j++;
			}
		}
		// if (gains.size() == 0) {
		// for (int i = 1; i < cat_size; i++) {
		// if ((!new_att.get(i).equals(new_att.get(i - 1)))) {
		// gains.add(entropy(new_cat.subList(0, i)) * (float) i / cat_size
		// + entropy(new_cat.subList(i, cat_size)) * (1 - (float) i / cat_size));
		// div_point.add(i);
		// }
		// }
		// }
		final int min_index_gains = minIndex(gains);
		float division_p;
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
		final float min_gains = gains.get(min_index_gains);
		final float gain = entropy(new_cat) - min_gains;
		final float p_1 = (float) div_point.get(min_index_gains) / cat_size;
		final float ent_att = -p_1 * (float) Math.log(p_1) - (1 - p_1) * (float) Math.log(1 - p_1);
		re_String = cla + comma + Float.toString(division_p);
		return new Pair<Pair<Float, Float>, String>(new Pair<Float, Float>(gain, gain / ent_att), re_String);
	}

	private final static Pair<Pair<Float, Float>, String> gain_s(List<String> categories, List<String> att)
			throws Exception {
		float s = 0f;
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
			return new Pair<Pair<Float, Float>, String>(new Pair<Float, Float>(0f, 0f), new_cat.get(0) + comma + "0");
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
			return new Pair<Pair<Float, Float>, String>(new Pair<Float, Float>(0f, 0f), re_String);
		}
		float p_i = 0f;
		for (String i : set_att) {
			p_i = (float) Collections.frequency(att, i) / att_size;
			List<String> cat_i = new ArrayList<String>();
			for (int j = 0; j < cat_size; j++) {
				if (new_att.get(j).equals(i)) {
					cat_i.add(new_cat.get(j));
				}
			}
			s = s + p_i * entropy(cat_i);
		}
		final float gain = entropy(new_cat) - s;
		final float ent_att = entropy(new_att);

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
			return new Pair<Pair<Float, Float>, String>(new Pair<Float, Float>(0f, 0f), re_String);
		} else {
			re_String = new_cat.get(0) + comma + C45.join(atts);

			return new Pair<Pair<Float, Float>, String>(new Pair<Float, Float>(gain, gain / ent_att), re_String);
		}
	}

	private final static float entropy(List<String> x) throws Exception {
		Set<String> set_cat = new HashSet<String>(x);
		int x_size = x.size();
		float ent = 0f;
		float p_i = 0f;
		for (String k : set_cat) {
			p_i = (float) Collections.frequency(x, k) / x_size;
			ent = ent - p_i * (float) Math.log(p_i);
		}
		return ent;
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

	private final static boolean is_num(List<String> att) throws Exception {
		Set<String> set = new HashSet<String>(att);
		for (String x : set) {
			if (!x.equals("?")) {
				try {
					Float.parseFloat(x);
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
}
