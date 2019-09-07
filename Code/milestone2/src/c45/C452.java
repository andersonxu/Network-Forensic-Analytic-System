package c45;


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.XMLWriter;

import c45.Pair;

import java.util.*;

public class C452 {
	public static List<Float> division_p = new ArrayList<Float>();

	public final static void main(String[] args) throws Exception {
		if (args.length != 1) {
			System.err.println("Usage: c45/C45 <input path>");
			System.exit(-1);
		}
		final long startTime = System.currentTimeMillis();
		System.out.println(args[0]);
		Path input_path = Paths.get(args[0]);
		List<List<String>> obs = new ArrayList<List<String>>();
		List<String> cat = new ArrayList<String>();
		List<String> att = new ArrayList<String>();
		List<String> file = Files.readAllLines(input_path);
		for (String line : file) {
			att = new ArrayList<String>(Arrays.asList(line.split(",")));
			cat.add(att.get(att.size() - 1));
			att.remove(att.size() - 1);
			obs.add(att);
		}
		final long readTime = System.currentTimeMillis() - startTime;
		System.out.println("Read time:" + (float) readTime / 1000);
		C452.train(obs, cat, args[0] + "model.xml");
		final long totalTime = System.currentTimeMillis() - startTime;
		System.out.println("Total time:" + (float) totalTime / 1000);
		System.out.println("Model file:" + args[0] + "model.xml");
	}

	private final static boolean train(List<List<String>> obs, List<String> cat, String xml_name) throws Exception {
		if (obs.size() != cat.size()) {
			return false;
		}
		List<String> att_names = obs.get(0);
		obs.remove(0);
		cat.remove(0);
		final List<List<String>> data = new ArrayList<List<String>>(transpose(obs));
		final List<String> categories = new ArrayList<String>(cat);
		Document document = DocumentHelper.createDocument();
		Element root = document.addElement("DecisionTree");
		try {
			grow_tree(data, categories, root, att_names);
		} catch (OutOfMemoryError e) {
			System.out.println("Out of memory");
		}

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
		return true;
	}

	private final static void grow_tree(List<List<String>> data, List<String> categories, Element parent,
			List<String> att_names) throws Exception {
		System.out.println("Start dividing");
		Set<String> set_cat = new HashSet<String>(categories);
		Set<String> set_data = new HashSet<String>();
		List<Float> division = new ArrayList<Float>();
		final int data_size = data.size();
		if (set_cat.size() > 1) {
			division_p = new ArrayList<Float>();
			for (int i = 0; i < data_size; i++) {
				// writer.println(i + "c");
				set_data = new HashSet<String>(data.get(i));
				if (set_data.size() == 1) {
					division.add(0f);
					division_p.add(0f);
				} else if (is_num(data.get(i))) {
					division.add(gain(categories, data.get(i)));

				} else {
					division.add(gain_ratio(categories, data.get(i)));
					division_p.add(0f);
				}
			}
			System.out.println("division finished");
			int max_index = maxIndex(division);
			if (division.get(max_index) == 0) {
				int num_max = 0;
				int num_cat = 0;
				String most_cat = "";
				for (String cat : set_cat) {
					num_cat = Collections.frequency(categories, cat);
					if (num_cat > num_max) {
						num_max = num_cat;
						most_cat = cat;
					}
					parent.setText(most_cat);
				}
			} else {
				int index_selected = max_index;
				String name_selected = att_names.get(index_selected);
				System.out.println("Seleted attribute: " + name_selected);
				set_data = new HashSet<String>(data.get(index_selected));
				if (is_num(data.get(index_selected))) {
					final float div_point = division_p.get(index_selected);
					System.out.println("div_p:" + div_point);
					List<List<String>> l_son_data = new ArrayList<List<String>>();
					List<List<String>> r_son_data = new ArrayList<List<String>>();
					List<String> l_son_category = new ArrayList<String>();
					List<String> r_son_category = new ArrayList<String>();
					for (int i = 0; i < data.size(); i++) {
						l_son_data.add(new ArrayList<String>());
						r_son_data.add(new ArrayList<String>());
					}
					for (int i = 0; i < categories.size(); i++) {
						if (Float.parseFloat(data.get(index_selected).get(i)) < div_point) {
							l_son_category.add(categories.get(i));
							for (int j = 0; j < data.size(); j++) {
								l_son_data.get(j).add(data.get(j).get(i));
							}
						} else {
							r_son_category.add(categories.get(i));
							for (int j = 0; j < data.size(); j++) {
								r_son_data.get(j).add(data.get(j).get(i));
							}
						}
					}

					int size_l_son_category = l_son_category.size();
					int size_r_son_category = r_son_category.size();
					if (size_l_son_category > 0 && size_r_son_category > 0) {
						System.out.println("go into child num");
						Element sonl = parent.addElement(name_selected);
						sonl.addAttribute("flag", "l");
						sonl.addAttribute("value", String.format("%.04f", div_point));
						grow_tree(l_son_data, l_son_category, sonl, att_names);
						Element sonr = parent.addElement(name_selected);
						sonr.addAttribute("flag", "r");
						sonr.addAttribute("value", String.format("%.04f", div_point));
						grow_tree(r_son_data, r_son_category, sonr, att_names);
					} else {
						int num_max = 0;
						int num_cat = 0;
						String most_cat = "";
						for (String cat : set_cat) {
							num_cat = Collections.frequency(categories, cat);
							if (num_cat > num_max) {
								num_max = num_cat;
								most_cat = cat;
							}
							parent.setText(most_cat);
						}
					}
				} else {
					System.out.println("go into child string");
					Set<String> set_data_s = new HashSet<String>(data.get(index_selected));
					for (String k : set_data_s) {
						List<String> son_category = new ArrayList<String>();
						List<List<String>> son_data = new ArrayList<List<String>>();
						for (int i = 0; i < data.size(); i++) {
							son_data.add(new ArrayList<String>());
						}
						for (int i = 0; i < categories.size(); i++) {
							if (data.get(index_selected).get(i).equals(k)) {
								son_category.add(categories.get(i));
								for (int j = 0; j < data.size(); j++) {
									son_data.get(j).add(data.get(j).get(i));
								}
							}
						}
						Element son = parent.addElement(name_selected);
						son.addAttribute("flag", "m");
						son.addAttribute("value", k);
						grow_tree(son_data, son_category, son, att_names);
					}
				}
			}
		} else {
			System.out.println("Reach the end");
			parent.setText(categories.get(0));
		}
	}

	private final static float gain(List<String> categories, List<String> att) throws Exception {
		List<Pair<Float, String>> cats = new ArrayList<Pair<Float, String>>();
		Pair<Float, String> a;
		for (int i = 0; i < att.size(); i++) {
			a = new Pair<Float, String>(Float.parseFloat(att.get(i)), categories.get(i));
			cats.add(a);
		}
		Collections.sort(cats, new Comparator<Pair<Float, String>>() {
			@Override
			public int compare(Pair<Float, String> p1, Pair<Float, String> p2) {
				return p1.getL().compareTo(p2.getL());
			}
		});
		List<String> new_cat = new ArrayList<String>();
		List<Float> new_att = new ArrayList<Float>();
		for (int i = 0; i < cats.size(); i++) {
			new_cat.add(cats.get(i).getR());
			new_att.add(cats.get(i).getL());
		}
		final int cat_size = new_cat.size();
		Set<String> set_cat = new HashSet<String>(new_cat);
		if (set_cat.size() == 1) {
			division_p.add(0f);
			return 0f;
		}
		List<Float> gains = new ArrayList<Float>();
		List<Integer> div_point = new ArrayList<Integer>();
		for (int i = 1; i < cat_size; i++) {
			if ((!new_att.get(i).equals(new_att.get(i - 1))) && (!new_cat.get(i).equals(new_cat.get(i - 1)))) {
				gains.add(entropy(new_cat.subList(0, i)) * (float) i / cat_size
						+ entropy(new_cat.subList(i, cat_size)) * (1 - (float) i / cat_size));
				div_point.add(i);
			}
		}
		if (gains.size() == 0) {
			for (int i = 1; i < cat_size; i++) {
				if ((!new_att.get(i).equals(new_att.get(i - 1)))) {
					gains.add(entropy(new_cat.subList(0, i)) * (float) i / cat_size
							+ entropy(new_cat.subList(i, cat_size)) * (1 - (float) i / cat_size));
					div_point.add(i);
				}
			}
		}
		final int min_index_gains = minIndex(gains);
		try {
			division_p
					.add((new_att.get(div_point.get(min_index_gains)) + new_att.get(div_point.get(min_index_gains) - 1))
							/ 2);
		} catch (IndexOutOfBoundsException e) {
			System.out.println("can't find div_p！！！！！！！！！！！！！");
			division_p.add(new_att.get(0));
		}

		final float min_gains = gains.get(min_index_gains);
		final float gain = entropy(new_cat) - min_gains;
		final float p_1 = (float) div_point.get(min_index_gains) / cat_size;
		final float ent_att = -p_1 * (float) Math.log(p_1) - (1 - p_1) * (float) Math.log(1 - p_1);
		return gain / ent_att;
	}

	private final static float gain_ratio(List<String> cat, List<String> att) throws Exception {
		float s = 0f;
		final int att_size = att.size();
		final int cat_size = cat.size();
		Set<String> set_att = new HashSet<String>(att);
		if (set_att.size() == 1) {
			return 0f;
		}
		float p_i = 0f;
		for (String i : set_att) {
			p_i = (float) Collections.frequency(att, i) / att_size;
			List<String> cat_i = new ArrayList<String>();
			for (int j = 0; j < cat_size; j++) {
				if (att.get(j).equals(i)) {
					cat_i.add(cat.get(j));
				}
			}
			s = s + p_i * entropy(cat_i);
		}
		final float gain = entropy(cat) - s;
		final float ent_att = entropy(att);
		if (ent_att == 0) {
			return 0f;
		} else {
			return gain / ent_att;
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

	// minimum
	public final static int minIndex(List<Float> list) {
		try {
			return list.indexOf(Collections.min(list));
		} catch (NoSuchElementException e) {
			return 0;
		}

	}

	// maximum
	public final static int maxIndex(List<Float> list) {
		try {
			return list.indexOf(Collections.max(list));
		} catch (NoSuchElementException e) {
			return 0;
		}
	}

	// if is numeric
	private final static boolean is_num(List<String> att) throws Exception {
		Set<String> set = new HashSet<String>(att);
		for (String x : set) {
			try {
				Float.parseFloat(x);
			} catch (NumberFormatException e) {
				return false;
			}
		}
		return true;
	}

	// zip the 2d list
	private final static <T> List<List<T>> transpose(List<List<T>> table) {
		List<List<T>> ret = new ArrayList<List<T>>();
		final int N = table.get(0).size();
		for (int i = 0; i < N; i++) {
			List<T> col = new ArrayList<T>();
			for (List<T> row : table) {
				col.add(row.get(i));
			}
			ret.add(col);
		}
		return ret;
	}

}
