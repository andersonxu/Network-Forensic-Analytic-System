package c45mr;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Setting {
	private int att_select;
	private boolean NorD;
	private Double gain;
	private Double ent;
	private String cat;
	private Double div_p;
	private List<String> atts;

	public static final String comma = ",";
	public static final String numeric = "n";
	public static final String discrete = "d";
	public static final String space = "\\s+";

	public Setting(String setting_line) {
		List<String> settings = new ArrayList<String>(Arrays.asList(setting_line.split(comma)));
		set(settings);
	}

	public void set(List<String> settings) {
		this.att_select = Integer.parseInt(settings.get(0));
		this.gain = Double.parseDouble(settings.get(2));
		this.ent = Double.parseDouble(settings.get(3));
		this.cat = settings.get(4);
		if (settings.get(1).equals(numeric)) {
			this.NorD = true;
			this.div_p = Double.parseDouble(settings.get(5));
			this.atts = new ArrayList<String>();
		} else {
			this.NorD = false;
			this.div_p = 0.0;
			this.atts = new ArrayList<String>(settings);
			this.atts.remove(0);
			this.atts.remove(0);
			this.atts.remove(0);
			this.atts.remove(0);
			this.atts.remove(0);
		}
	}

	public int getAtt() {
		return att_select;
	}

	public boolean is_num() {
		return NorD;
	}

	public Double getG() {
		return gain;
	}

	public Double getEnt() {
		return ent;
	}

	public String getCat() {
		return cat;
	}

	public Double getP() {
		return div_p;
	}

	public List<String> getAtts() {
		return atts;
	}
}
