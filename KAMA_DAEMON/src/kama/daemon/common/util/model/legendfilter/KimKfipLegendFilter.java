package kama.daemon.common.util.model.legendfilter;

import java.awt.Color;

public class KimKfipLegendFilter {
	
	public Color getColor_KIMFIP(double v) {
		
		if(Double.isNaN(v)) {
			return null;
		}
			
		if (v <= 0) {
	        return new Color(0, 0, 0, 0); // 완전 투명 (또는 null 써도 됨)
	    }

	    if (v >= 0.16 && v < 0.34) {
	    	return new Color(128, 228, 16);
	    } else if (v >= 0.34 && v < 0.52) {
        	return new Color(255, 252, 0);
        } else if (v >= 0.52 && v < 0.88) {
        	return new Color(255, 130, 0);
        } else if (v >= 0.88) {
        	return new Color(255, 0, 0);
        }
		
		return null;
	}
}
