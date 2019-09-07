package mr2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Setting {
	private int att_select;
	private Float score;
	private Float div_p;
	private String residual;

	public static final String comma = ",";
	public static final String space = "\\s+";

	public Setting(String setting_line) {
		List<String> settings = new ArrayList<String>(Arrays.asList(setting_line.split(comma)));
		this.att_select = Integer.parseInt(settings.get(0));
		this.score = Float.parseFloat(settings.get(1));
		this.div_p = Float.parseFloat(settings.get(2));
		this.residual = String.format("%.06f", Float.parseFloat(settings.get(3)));
	}

	public int getAtt(){
		return att_select;
	}
	public Float getScore(){
		return score;
	}
	public Float getP(){
		return div_p;
	}
	public String getR(){
		return residual;
	}
}
