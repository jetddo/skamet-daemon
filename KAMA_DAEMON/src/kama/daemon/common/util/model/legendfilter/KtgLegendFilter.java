package kama.daemon.common.util.model.legendfilter;

import java.awt.Color;

public class KtgLegendFilter {
	
	public Color getColor_KTG_AUC_20(double v) {
		
		if(Double.isNaN(v)) {
			return null;
		}
			
		if (v >= 0.3 && v < 0.475) {			// LGT
			return new Color(0, 255, 0);
		} else if (v >= 0.475 && v < 0.75) {	// MOD
			return new Color(255, 204, 0);
		} else if (v >= 0.75) {						// SEV
			return new Color(255, 0, 0);
		}		
		
		return null;
	}
}
