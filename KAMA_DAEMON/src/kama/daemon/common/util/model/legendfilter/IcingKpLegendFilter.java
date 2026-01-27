package kama.daemon.common.util.model.legendfilter;

import java.awt.Color;

public class IcingKpLegendFilter {
	
	public Color getColor_RAP(double v) {
		
		if(Double.isNaN(v)) {
			return null;
		}
		
		if(v == 1) {
			return new Color(0,204,255);
		} else if(v == 2) {
			return new Color(0,92,255);
		} else if(v == 3) {
			return new Color(0,0,163);
		} else if(v == 4) {
			return new Color(215,194,226);
		} else if(v == 5) {
			return new Color(168,122,193);
		} else if(v == 6) {
			return new Color(100,20,146);
		} 
		
		return null;
	}
}
