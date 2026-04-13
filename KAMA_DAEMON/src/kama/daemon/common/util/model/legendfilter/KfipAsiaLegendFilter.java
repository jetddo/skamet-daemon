package kama.daemon.common.util.model.legendfilter;

import java.awt.Color;

public class KfipAsiaLegendFilter {
	
	public Color getColor_KIMFIP(double v) {
		
		if(Double.isNaN(v)) {
			return null;
		}
			
		if (v <= 0) {
	        return new Color(0, 0, 0, 0); // 완전 투명 (또는 null 써도 됨)
	    }

	    if (v < 0.5) {                 // Trace
	        return new Color(200, 230, 255);
	    } else if (v < 1.0) {          // Very Light
	        return new Color(100, 200, 255);
	    } else if (v < 1.5) {          // Light
	        return new Color(0, 255, 255);
	    } else if (v < 2.5) {          // Light ~ Moderate
	        return new Color(0, 200, 0);
	    } else if (v < 3.5) {          // Moderate
	        return new Color(255, 255, 0);
	    } else if (v < 4.5) {          // Moderate ~ Severe
	        return new Color(255, 140, 0);
	    } else if (v <= 5.0) {         // Severe
	        return new Color(255, 0, 0);
	    }	
		
		return null;
	}
}
