package kama.daemon.common.util.model.legendfilter;

import java.awt.Color;

public class KimGdpsLegendFilter {
	
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
	
	public Color getColor_Relative_humidity_isobaric(double v) {
		
		if(Double.isNaN(v)) {
			return null;
		}
		
		if(v >= 0 && v < 10) {
			return new Color(165, 0, 38);      // 매우 건조 - 진한 빨강
		} else if(v >= 10 && v < 20) {
			return new Color(215, 48, 39);
		} else if(v >= 20 && v < 30) {
			return new Color(244, 109, 67);
		} else if(v >= 30 && v < 40) {
			return new Color(253, 174, 97);
		} else if(v >= 40 && v < 50) {
			return new Color(254, 224, 144);
		} else if(v >= 50 && v < 60) {
			return new Color(224, 243, 248);   // 적정 습도
		} else if(v >= 60 && v < 70) {
			return new Color(171, 217, 233);
		} else if(v >= 70 && v < 80) {
			return new Color(116, 173, 209);
		} else if(v >= 80 && v < 90) {
			return new Color(69, 117, 180);
		} else if(v >= 90 && v <= 100) {
			return new Color(49, 54, 149);     // 매우 습함 - 진한 파랑
		}
		
		return null;
	}
	
	public Color getColorTropopause(float v) {

	    if(Float.isNaN(v)) {
	        return null;
	    }

	    /*
	     * tropopause height range (km)
	     */
	    float min = 8.5f;
	    float max = 17.0f;

	    /*
	     * normalize
	     */
	    float t =
	        (v - min)
	        /
	        (max - min);

	    t = Math.max(0f, Math.min(1f, t));

	    /*
	     * gamma correction
	     * 중간톤 강조
	     */
	    t = (float)Math.pow(t, 0.80);

	    /*
	     * subtle meteorological palette
	     *
	     * deep blue
	     * -> blue
	     * -> cyan
	     * -> mint
	     * -> pale warm
	     */
	    Color[] colors = new Color[] {

	        new Color(72, 105, 165),   // deep blue
	        new Color(78, 135, 190),   // blue
	        new Color(92, 175, 200),   // cyan
	        new Color(135, 205, 180),  // mint
	        new Color(190, 220, 170),  // pale green
	        new Color(235, 228, 185)   // warm ivory
	    };

	    int n = colors.length - 1;

	    float scaled =
	        t * n;

	    int idx =
	        (int)Math.floor(scaled);

	    if(idx >= n) {
	        return colors[n];
	    }

	    float localT =
	        scaled - idx;

	    Color c1 = colors[idx];
	    Color c2 = colors[idx + 1];

	    int r =
	        (int)(c1.getRed()
	        +
	        (c2.getRed() - c1.getRed()) * localT);

	    int g =
	        (int)(c1.getGreen()
	        +
	        (c2.getGreen() - c1.getGreen()) * localT);

	    int b =
	        (int)(c1.getBlue()
	        +
	        (c2.getBlue() - c1.getBlue()) * localT);

	    return new Color(r, g, b);
	}
}
