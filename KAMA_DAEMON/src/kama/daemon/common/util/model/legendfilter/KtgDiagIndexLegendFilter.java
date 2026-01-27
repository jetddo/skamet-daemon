package kama.daemon.common.util.model.legendfilter;

import java.awt.Color;

public class KtgDiagIndexLegendFilter {
	
	public Color getColor_def(double v) {

	    if (Double.isNaN(v)) {
	        return null;
	    }

	    // 최소값 0.0 ~ 최대값 0.002 (0.5e-3 step)
	    if (v < 0.0005) {
	        return new Color(153, 204, 255);     // 연한 파랑
	    } else if (v < 0.0010) {
	        return new Color(102, 178, 255);     // 하늘 파랑
	    } else if (v < 0.0015) {
	        return new Color(255, 230, 179);     // 부드러운 노랑/중립
	    } else if (v < 0.0020) {
	        return new Color(255, 153, 102);     // 주황 계열
	    } else {
	        return new Color(255, 51, 51);       // 강한 빨강
	    }
	}

	public Color getColor_pvu(double v) {

	    if (Double.isNaN(v)) {
	        return null;
	    }

	    if (v < -5) {
	        return new Color(0, 0, 255);          // 진한 파랑
	    } else if (v < -4) {
	        return new Color(25, 25, 255);
	    } else if (v < -3) {
	        return new Color(51, 51, 255);
	    } else if (v < -2) {
	        return new Color(76, 76, 255);
	    } else if (v < -1) {
	        return new Color(102, 102, 255);
	    } else if (v < 0) {
	        return new Color(127, 127, 255);
	    } else if (v < 1) {
	        return new Color(153, 153, 255);
	    } else if (v < 2) {
	        return new Color(178, 178, 255);
	    } else if (v < 3) {
	        return new Color(204, 204, 255);
	    } else if (v < 4) {
	        return new Color(255, 204, 204);      // 거의 중간 (밝은 분홍)
	    } else if (v < 5) {
	        return new Color(255, 178, 178);
	    } else if (v < 6) {
	        return new Color(255, 153, 153);
	    } else if (v < 7) {
	        return new Color(255, 127, 127);
	    } else if (v < 8) {
	        return new Color(255, 102, 102);
	    } else if (v < 9) {
	        return new Color(255, 76, 76);
	    } else if (v < 10) {
	        return new Color(255, 51, 51);
	    } else if (v < 11) {
	        return new Color(255, 25, 25);
	    } else {
	        return new Color(255, 0, 0);          // 진한 빨강
	    }
	}
	
	public Color getColor_div(double v) {

	    if (Double.isNaN(v)) {
	        return null;
	    }

	    // 범위: -0.001 ~ 0.001 (step 0.0005)
	    if (v < -0.0010) {
	        return new Color(0, 51, 153);        // 진한 파랑
	    } else if (v < -0.0005) {
	        return new Color(102, 153, 255);     // 파랑
	    } else if (v < 0.0) {
	        return new Color(204, 229, 255);     // 연한 파랑(중립)
	    } else if (v < 0.0005) {
	        return new Color(255, 230, 179);     // 연노랑
	    } else if (v < 0.0010) {
	        return new Color(255, 153, 102);     // 주황
	    } else {
	        return new Color(255, 51, 51);       // 빨강
	    }
	}

	public Color getColor_vws(double v) {

	    if (Double.isNaN(v)) {
	        return null;
	    }

	    if (v < 0.005) {
	        return new Color(204, 229, 255);   // 매우 낮음
	    } else if (v < 0.010) {
	        return new Color(153, 204, 255);   // 낮음
	    } else if (v < 0.015) {
	        return new Color(102, 178, 255);   // 약간 낮음
	    } else if (v < 0.020) {
	        return new Color(51, 153, 255);    // 중간 정도
	    } else if (v < 0.025) {
	        return new Color(0, 128, 255);     // 중상
	    } else if (v < 0.030) {
	        return new Color(0, 102, 204);     // 꽤 높음
	    } else if (v < 0.035) {
	        return new Color(0, 76, 153);      // 높은 값
	    } else if (v < 0.040) {
	        return new Color(128, 0, 128);     // 보라톤
	    } else if (v < 0.045) {
	        return new Color(204, 0, 0);       // 강한 빨강
	    } else if (v < 0.050) {
	        return new Color(255, 51, 51);     // 매우 강한 빨강
	    } else {
	        return new Color(255, 0, 0);       // 최고치
	    }
	}
	
	public Color getColor_lapse(double v) {

	    if (Double.isNaN(v)) {
	        return null;
	    }

	    // 범위: -0.015 ~ 0.015 (step 0.005)
	    if (v < -0.015) {
	        return new Color(0, 51, 153);        // 진한 파랑
	    } else if (v < -0.010) {
	        return new Color(51, 102, 204);      // 파랑
	    } else if (v < -0.005) {
	        return new Color(102, 153, 255);     // 연파랑
	    } else if (v < 0.0) {
	        return new Color(204, 229, 255);     // 아주 연한 파랑 (중립)
	    } else if (v < 0.005) {
	        return new Color(255, 230, 179);     // 연노랑 (중립 양수)
	    } else if (v < 0.010) {
	        return new Color(255, 178, 102);     // 연주황
	    } else if (v < 0.015) {
	        return new Color(255, 102, 102);     // 빨강
	    } else {
	        return new Color(255, 0, 0);         // 진한 빨강
	    }
	}


}
