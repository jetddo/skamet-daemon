package kama.daemon.common.util.model.legendfilter;

import java.awt.Color;

public class GktgArcvLegendFilter {
	
	public Color getColor_GTGMAX(double v) {
		
		if(Double.isNaN(v)) {
			return null;
		}
		
		if(v >= 0.15 && v < 0.17) {
			return new Color(0, 255, 0);
		} else if(v >= 0.17 && v < 0.24) {
			return new Color(255, 204, 0);
		} else if(v >= 0.24) {
			return new Color(255, 0, 0);
		}
		
		return null;
	}
	
	public Color getColor_GTGDEF(double v) {
		
		if(Double.isNaN(v)) {
			return null;
		}
		
		if(v >= 0.15 && v < 0.17) {
			return new Color(0, 255, 0);
		} else if(v >= 0.17 && v < 0.24) {
			return new Color(255, 204, 0);
		} else if(v >= 0.24) {
			return new Color(255, 0, 0);
		}
		
		return null;
	}
	
	public Color getColor_GTGMWT(double v) {
		
		if(Double.isNaN(v)) {
			return null;
		}
		
		if(v >= 0.15 && v < 0.17) {
			return new Color(0, 255, 0);
		} else if(v >= 0.17 && v < 0.24) {
			return new Color(255, 204, 0);
		} else if(v >= 0.24) {
			return new Color(255, 0, 0);
		}
		
		return null;
	}
}
