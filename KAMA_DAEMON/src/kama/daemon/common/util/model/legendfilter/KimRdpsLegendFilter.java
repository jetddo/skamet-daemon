package kama.daemon.common.util.model.legendfilter;

import java.awt.Color;

public class KimRdpsLegendFilter {
	
	public Color getColor_Temperature_isobaric(double v) {
		
		if(Double.isNaN(v)) {
			return null;
		}
		
		v -= 273.15;
	
		
		if(v >= -100 && v < -15) {
			return new Color(171,66,160);
		} else if(v >= -15 && v < -10) {
			return new Color(115,147,190);
		} else if(v >= -10 && v < -5) {
			return new Color(90,185,204);
		} else if(v >= -5 && v < 0) {
			return new Color(52,167,204);
		} else if(v >= 0 && v < 5) {
			return new Color(39,132,197);
		} else if(v >= 5 && v < 10) {
			return new Color(24,92,188);
		} else if(v >= 10 && v < 15) {
			return new Color(45,143,18);
		} else if(v >= 15 && v < 20) {
			return new Color(112,179,31);
		} else if(v >= 20 && v < 25) {
			return new Color(188,219,47);
		} else if(v >= 25 && v < 30) {
			return new Color(246,244,56);
		} else if(v >= 30 && v < 35) {
			return new Color(236,179,26);
		} else if(v >= 35 && v < 40) {
			return new Color(233,139,26);
		} else if(v >= 40) {
			return new Color(231,104,32);
		}
		
		return null;
	}
}
