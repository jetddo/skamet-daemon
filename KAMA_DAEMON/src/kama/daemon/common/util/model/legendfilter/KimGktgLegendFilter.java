package kama.daemon.common.util.model.legendfilter;

import java.awt.Color;

public class KimGktgLegendFilter {
	
	public Color getColor_GTGMAX(double v) {
		
		if(Double.isNaN(v)) {
			return null;
		}
		
		if(v >= 0.15 && v < 0.22) {
			return new Color(0, 255, 0);
		} else if(v >= 0.22 && v < 0.34) {
			return new Color(255, 204, 0);
		} else if(v >= 0.34) {
			return new Color(255, 0, 0);
		}
		
		return null;
	}
}
