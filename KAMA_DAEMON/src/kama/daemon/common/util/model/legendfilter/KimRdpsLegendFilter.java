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
}
