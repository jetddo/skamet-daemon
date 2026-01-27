package kama.daemon.common.util.model.legendfilter;

import java.awt.Color;

public class GktgLegendFilter {
	
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
	/*GKTG 높은 임계값 추가(SEV)*/
	public Color getColor_GTGMAXSEV(double v) {
		
		if(Double.isNaN(v)) {
			return null;
		}
		
		/*임계치 변경 예정*/
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
		
		/*2024-05-09 청천난류 임계값 변경*/
		if(v >= 0.15 && v < 0.17) {
			return new Color(0, 255, 0);
		} else if(v >= 0.17 && v < 0.24) {
			return new Color(255, 204, 0);
		} else if(v >= 0.24) {
			return new Color(255, 0, 0);
		}

//		if(v >= 0.15 && v < 0.22) {
//			return new Color(0, 255, 0);
//		} else if(v >= 0.22 && v < 0.34) {
//			return new Color(255, 204, 0);
//		} else if(v >= 0.34) {
//			return new Color(255, 0, 0);
//		}
		
		return null;
	}
	
	/*2023-09-14 청천난류 높은 임계값 추가(SEV)*/
	public Color getColor_GTGDEFSEV(double v) {
		
		if(Double.isNaN(v)) {
			return null;
		}
		
		/*임계치 변경 예정*/
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
		
		/*2024-05-09 산악파 임계값 변경*/
		if(v >= 0.15 && v < 0.17) {
			return new Color(0, 255, 0);
		} else if(v >= 0.17 && v < 0.24) {
			return new Color(255, 204, 0);
		} else if(v >= 0.24) {
			return new Color(255, 0, 0);
		}
		
//		if(v >= 0.15 && v < 0.22) {
//			return new Color(0, 255, 0);
//		} else if(v >= 0.22 && v < 0.34) {
//			return new Color(255, 204, 0);
//		} else if(v >= 0.34) {
//			return new Color(255, 0, 0);
//		}
		
		return null;
	}
	
	/*2023-09-14 산악파 높은 임계값 추가(SEV)*/
	public Color getColor_GTGMWTSEV(double v) {
		
		if(Double.isNaN(v)) {
			return null;
		}
		
		/*임계치 변경 예정*/
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
