package kama.daemon.common.util.model.legendfilter;

import java.awt.Color;

public class HobsLegendFilter {
    
    public Color getTempColor(double v) {
		
		if(Double.isNaN(v)) {
			return null;
		} 
		
		if(v < -15) {
			return new Color(233, 175, 255);			
		} else if(v >= -15 && v < -14.5) {
			return new Color(233, 175, 255);
		} else if(v >= -14.5 && v < -14) {
            return new Color(218, 135, 254);
        } else if(v >= -14 && v < -13.5) {
            return new Color(207, 95, 255);
        } else if(v >= -13.5 && v < -13) {
            return new Color(194, 59, 254);
        } else if(v >= -13 && v < -12.5) {
            return new Color(183, 24, 254);
        } else if(v >= -12.5 && v < -12) {
            return new Color(174, 0, 255);
        } else if(v >= -12 && v < -11.5) {
            return new Color(160, 0, 246);
        } else if(v >= -11.5 && v < -11) {
            return new Color(146, 0, 230);
        } else if(v >= -11 && v < -10.5) {
            return new Color(135, 0, 208);
        } else if(v >= -10.5 && v < -10) {
            return new Color(127, 0, 191);
        } else if(v >= -10 && v < -9.5) {
            return new Color(203, 204, 232);
        } else if(v >= -9.5 && v < -9) {
            return new Color(179, 180, 222);
        } else if(v >= -9 && v < -8.5) {
            return new Color(154, 155, 213);
        } else if(v >= -8.5 && v < -8) {
            return new Color(128, 129, 199);
        } else if(v >= -8 && v < -7.5) {
            return new Color(100, 102, 188);
        } else if(v >= -7.5 && v < -7) {
            return new Color(74, 76, 178);
        } else if(v >= -7 && v < -6.5) {
            return new Color(48, 50, 168);
        } else if(v >= -6.5 && v < -6) {
            return new Color(24, 26, 157);
        } else if(v >= -6 && v < -5.5) {
            return new Color(4, 6, 150);
        } else if(v >= -5.5 && v < -5) {
            return new Color(0, 0, 144);
        } else if(v >= -5 && v < -4.5) {
            return new Color(172, 229, 254);
        } else if(v >= -4.5 && v < -4) {
            return new Color(135, 219, 255);
        } else if(v >= -4 && v < -3.5) {
            return new Color(95, 207, 255);
        } else if(v >= -3.5 && v < -3) {
            return new Color(58, 193, 254);
        } else if(v >= -3 && v < -2.5) {
            return new Color(24, 181, 254);
        } else if(v >= -2.5 && v < -2) {
            return new Color(1, 172, 254);
        } else if(v >= -2 && v < -1.5) {
            return new Color(0, 158, 249);
        } else if(v >= -1.5 && v < -1) {
            return new Color(0, 141, 224);
        } else if(v >= -1 && v < -0.5) {
            return new Color(0, 128, 197);
        } else if(v >= -0.5 && v < 0) {
            return new Color(0, 119, 179);
        } else if(v >= 0 && v < 0.5) {
            return new Color(150, 253, 150);
        } else if(v >= 0.5 && v < 1) {
            return new Color(104, 251, 104);
        } else if(v >= 1 && v < 1.5) {
            return new Color(61, 248, 61);
        } else if(v >= 1.5 && v < 2) {
            return new Color(22, 242, 23);
        } else if(v >= 2 && v < 2.5) {
        	return new Color(0, 235, 0);
        } else if(v >= 2.5 && v < 3) {
            return new Color(0, 213, 0);
        } else if(v >= 3 && v < 3.5) {
            return new Color(0, 190, 0);
        } else if(v >= 3.5 && v < 4) {
            return new Color(0, 164, 0);
        } else if(v >= 4 && v < 4.5) {
            return new Color(0, 142, 0);
        } else if(v >= 4.5 && v < 5) {
            return new Color(0, 128, 0);
        } else if(v >= 5 && v < 5.5) {
            return new Color(255, 244, 155);
        } else if(v >= 5.5 && v < 6) {
            return new Color(254, 234, 109);
        } else if(v >= 6 && v < 6.5) {
            return new Color(254, 227, 64);
        } else if(v >= 6.5 && v < 7) {
            return new Color(255, 224, 20);
        } else if(v >= 7 && v < 7.5) {
            return new Color(254, 214, 0);
        } else if(v >= 7.5 && v < 8) {
            return new Color(248, 205, 0);
        } else if(v >= 8 && v < 8.5) {
            return new Color(237, 195, 0);
        } else if(v >= 8.5 && v <9) {
            return new Color(224,185,0);
        } else if(v >=9 && v <9.5) {
            return new Color(212,177,0);
        } else if(v >=9.5 && v <10) {
            return new Color(204,171,0);
        } else if(v >=10 && v <10.5) {
            return new Color(255,174,174);
        } else if(v >=10.5 && v <11) {
            return new Color(249, 133, 133);
		} else if (v >= 11 && v < 11.5) {
			return new Color(247, 95, 95);
		} else if (v >= 11.5 && v < 12) {
			return new Color(245, 59, 59);
		} else if (v >= 12 && v < 12.5) {
			return new Color(242, 26, 26);
		} else if (v >= 12.5 && v < 13) {
			return new Color(238, 3, 3);
		} else if (v >= 13 && v < 13.5) {
			return new Color(227, 0, 0);
		} else if (v >= 13.5 && v < 14) {
			return new Color(216, 0, 0);
		} else if (v >= 14 && v < 14.5) {
			return new Color(203, 0, 0);
		} else if (v >= 14.5) {
			return new Color(191, 0, 0);
		}	
		
		return null;
	}
    
    public Color getWsColor(double v) {
    	
        if(Double.isNaN(v)) {
            return null;
        }
        
        if(v >= 0.1 && v < 2) {
        	return new Color(255, 236, 109);
        } else if(v >= 2 && v < 4) {
        	return new Color(254, 220, 24);
        } else if(v >= 4 && v < 6) {
        	return new Color(248, 205, 0);
        } else if(v >= 6 && v < 8) {
        	return new Color(224, 185, 0);
        } else if(v >= 8 && v < 10) {
        	return new Color(204, 171, 0);
        } else if(v >= 10 && v < 11) {
        	return new Color(103, 255, 103);
        } else if(v >= 11 && v < 12) {
        	return new Color(20, 244, 21);
        } else if(v >= 12 && v < 13) {
        	return new Color(0, 216, 0);
        } else if(v >= 13 && v < 14) {
        	return new Color(0, 165, 0);
        } else if(v >= 14 && v <15) {
        	return new Color(0, 128, 0);
        } else if(v >=15 && v <16) {
        	return new Color(135,220,255);
        } else if(v >=16 && v <17) {
        	return new Color(58,193,254);
        } else if(v >=17 && v <18) {
        	return new Color(1,172,254);
        } else if(v >=18 && v <19) {
        	return new Color(0,141,222);
        } else if(v >=19 && v <20) {
        	return new Color(0,119,181);
        } else if(v >=20 && v <21) {
        	return new Color(179,180,222);
        } else if(v >=21 && v <22) {
        	return new Color(128,129,199);
        } else if(v >=22 && v <23) {
        	return new Color(74,76,178);
        } else if(v >=23 && v <24) {
        	return new Color(21,23,158);
        } else if(v >=24 && v <25) {
        	return new Color(0,0,144);
        } else if(v >=25 && v <26) {
			return new Color(221, 135, 255);
		} else if (v >= 26 && v < 27) {
			return new Color(196, 57, 255);
		} else if (v >= 27 && v < 28) {
			return new Color(174, 0, 255);
		} else if (v >= 28 && v < 29) {
			return new Color(146, 0, 228);
		} else if (v >= 29 && v < 30) {
			return new Color(127, 0, 193);
		} else if (v >= 30 && v < 32) {
			return new Color(253, 133, 133);
		} else if (v >= 32 && v < 34) {
			return new Color(245, 59, 59);
		} else if (v >= 34 && v < 36) {
			return new Color(242, 0, 0);
		} else if (v >= 36 && v < 38) {
			return new Color(216, 0, 0);
		} else if (v >= 38) {
			return new Color(200, 0, 0);
        }
        
        return null;
    }
    
	public Color getRainColor(double v) {
    	
        if(Double.isNaN(v)) {
            return null;
        }
   
		if(v >= 0.1 && v < 0.2) {
			return new Color(255, 238, 108);
		} else if(v >= 0.2 && v < 0.4) {
			return new Color(255, 223, 20);
		} else if(v >= 0.4 && v < 0.6) {
			return new Color(252, 207, 0);
		} else if(v >= 0.6 && v < 0.8) {
			return new Color(227, 187, 0);
		} else if(v >= 0.8 && v < 1) {
			return new Color(206, 172, 0);
		} else if(v >= 1 && v < 2) {
			return new Color(104, 249, 104);
		} else if(v >= 2 && v < 3) {
			return new Color(17, 246, 18);
		} else if(v >= 3 && v < 4) {
			return new Color(0, 216, 0);
		} else if(v >= 4 && v <5) {
			return new Color(0,165,0);
		} else if(v >=5 && v <6) {
			return new Color(135,220,255);
		} else if(v >=6 && v <7) {
			return new Color(55,195,255);
		} else if(v >=7 && v <8) {
			return new Color(0,173,255);
		} else if(v >=8 && v <9) {
			return new Color(0,141,225);
		} else if(v >=9 && v <10) {
			return new Color(0,118,180);
		} else if(v >=10 && v <12) {
			return new Color(180,181,225);
		} else if(v >=12 && v <14) {
			return new Color(128,129,198);
		} else if(v >=14 && v <16) {
			return new Color(71,74,179);
		} else if(v >=16 && v <18) {
			return new Color(31,33,154);
		} else if(v >=18 && v <20) {
			return new Color(1,1,143);
		} else if(v >=20 && v <26) {
			return new Color(221, 135, 255);
		} else if(v >=26 && v <32) {
			return new Color(196, 56, 255);
		} else if(v >=32 && v <38) {
			return new Color(174, 0, 255);
		} else if(v >=38 && v <44) {
			return new Color(146, 0, 231);
		} else if(v >=44 && v <50) {
			return new Color(127, 0, 193);
		} else if(v >=50 && v <60) {
			return new Color(253, 133, 133);
		} else if(v >=60 && v <70) {
			return new Color(249, 56, 56);
		} else if(v >=70 && v <80) {
			return new Color(242, 0, 0);
		} else if(v >=80 && v <90) {
			return new Color(216, 0, 0);
		} else if(v >=90) {
			return new Color(202, 0, 0);
		}
        
		return null;
	}
	
	public Color getHumColor(double v) {
		
		if (Double.isNaN(v)) {
			return null;
		}
		
		if (v >= 0 && v < 2) {
            return new Color(255, 174, 174);
        } else if (v >= 2 && v < 4) {
            return new Color(250, 133, 133);
        } else if (v >= 4 && v < 6) {
            return new Color(248, 95, 95);
        } else if (v >= 6 && v < 8) {
            return new Color(246, 59, 59);
        } else if (v >= 8 && v < 10) {
            return new Color(243, 25, 25);
        } else if (v >= 10 && v < 12) {
            return new Color(239, 2, 2);
        } else if (v >= 12 && v < 14) {
            return new Color(228, 0, 0);
        } else if (v >= 14 && v <16) {
            return new Color(214,0,0);
        } else if (v >=16 && v <18) {
            return new Color(200,0,0);
        } else if (v >=18 && v <20) {
            return new Color(191,0,0);
        } else if (v >=20 && v <22) {
            return new Color(255,244,155);
        } else if (v >=22 && v <24) {
            return new Color(255,238,108);
        } else if (v >=24 && v <26) {
            return new Color(255,228,64);
        } else if (v >=26 && v <28) {
            return new Color(255,221,23);
        } else if (v >=28 && v <30) {
            return new Color(255,214,0);
        } else if (v >=30 && v <32) {
            return new Color(249,205,0);
        } else if (v >=32 && v <34) {
            return new Color(241,197,0);
        } else if (v >=34 && v <36) {
            return new Color(225,185,0);
        } else if (v >= 36 && v < 38) {
            return new Color(212,177,0);
        } else if (v >= 38 && v < 40) {
            return new Color(204,171,0);
        } else if (v >= 40 && v < 42) {
            return new Color(150,254,150);
        } else if (v >= 42 && v < 44) {
            return new Color(104,252,104);
        } else if (v >= 44 && v < 46) {
            return new Color(61,249,61);
        } else if (v >= 46 && v < 48) {
            return new Color(21,243,22);
        } else if (v >= 48 && v < 50) {
            return new Color(0,234,0);
        } else if (v >= 50 && v < 52) {
            return new Color(0,213,0);
        } else if (v >= 52 && v < 54) {
            return new Color(0,189,0);
        } else if (v >= 54 && v < 56) {
            return new Color(0,164,0);
        } else if (v >= 56 && v <58) {
            return new Color(0,142,0);
        } else if (v >=58 && v <60) {
            return new Color(0,128,0);
        } else if (v >=60 && v <62) {
            return new Color(172,230,255);
        } else if (v >=62 && v <64) {
            return new Color(135,217,255);
        } else if (v >=64 && v <66) {
            return new Color(96,205,255);
        } else if (v >=66 && v <68) {
            return new Color(58,193,255);
        } else if (v >=68 && v <70) {
            return new Color(23,181,255);
        } else if (v >=70 && v <72) {
            return new Color(0,172,255);
        } else if (v >=72 && v <74) {
            return new Color(0,157,246);
        } else if (v >=74 && v <76) {
			return new Color(0, 141, 223);
		} else if (v >= 76 && v < 78) {
			return new Color(0, 128, 197);
		} else if (v >= 78 && v < 80) {
			return new Color(0, 119, 179);
		} else if (v >= 80 && v < 82) {
			return new Color(203, 204, 233);
		} else if (v >= 82 && v < 84) {
			return new Color(179, 180, 223);
		} else if (v >= 84 && v < 86) {
			return new Color(154, 155, 211);
		} else if (v >= 86 && v < 88) {
			return new Color(128, 129, 199);
		} else if (v >= 88 && v < 90) {
			return new Color(99, 101, 190);
		} else if (v >= 90 && v < 92) {
			return new Color(73, 75, 178);
		} else if (v >= 92 && v < 94) {
			return new Color(45, 47, 170);
		} else if (v >= 94 && v < 96) {
			return new Color(20, 22, 158);
		} else if (v >= 96 && v < 98) {
			return new Color(7, 9, 149);
		} else if (v >= 98) {
			return new Color(0, 0, 145);
        }
		
		return null;
	}
	
	public Color getPressureColor(double v) {
		
		if (Double.isNaN(v)) {
			return null;
		}
		
		if (v < 1005.5) {
            return new Color(229, 173, 254);
        } else if (v >= 1005.5 && v < 1006.0) {
            return new Color(218, 135, 254);
        } else if (v >= 1006.0 && v < 1006.5) {
            return new Color(204, 96, 254);
        } else if (v >= 1006.5 && v < 1007.0) {
            return new Color(194, 59, 254);
        } else if (v >= 1007.0 && v < 1007.5) {
            return new Color(183, 23, 254);
        } else if (v >= 1007.5 && v < 1008.0) {
            return new Color(173, 0, 254);
        } else if (v >= 1008.0 && v < 1008.5) {
            return new Color(160, 0, 246);
        } else if (v >= 1008.5 && v < 1009.0) {
            return new Color(147, 0, 230);
        } else if (v >= 1009.0 && v < 1009.5) {
            return new Color(135, 0, 206);
        } else if (v >= 1009.5 && v < 1010.0) {
            return new Color(127, 0, 191);
        } else if (v >= 1010.0 && v <1010.5) {
            return new Color(203,204,233);
        } else if (v >=1010.5 && v <1011.0) {
            return new Color(179,180,223);
        } else if (v >=1011.0 && v <1011.5) {
            return new Color(154,155,211);
        } else if (v >=1011.5 && v <1012.0) {
            return new Color(128,129,199);
        } else if (v >=1012.0 && v <1012.5) {
			return new Color(99, 101, 190);
		} else if (v >= 1012.5 && v < 1013.0) {
			return new Color(73, 75, 178);
		} else if (v >= 1013.0 && v < 1013.5) {
			return new Color(45, 47, 170);
		} else if (v >= 1013.5 && v < 1014.0) {
			return new Color(20, 22, 158);
		} else if (v >= 1014.0 && v < 1014.5) {
			return new Color(7, 9, 149);
		} else if (v >= 1014.5 && v < 1015.0) {
			return new Color(0, 0, 145);
		} else if (v >= 1015.0 && v < 1015.5) {
			return new Color(172, 230, 255);
		} else if (v >= 1015.5 && v < 1016.0) {
			return new Color(135, 217, 255);
		} else if (v >= 1016.0 && v < 1016.5) {
			return new Color(96, 205, 255);
		} else if (v >= 1016.5 && v < 1017.0) {
			return new Color(58, 193, 255);
		} else if (v >= 1017.0 && v < 1017.5) {
			return new Color(23, 181, 255);
		} else if (v >= 1017.5 && v < 1018.0) {
			return new Color(0, 172, 255);
		} else if (v >= 1018.0 && v < 1018.5) {
			return new Color(0, 157, 246);
		} else if (v >= 1018.5 && v < 1019.0) {
			return new Color(0, 141, 223);
		} else if (v >= 1019.0 && v < 1019.5) {
			return new Color(0, 128, 197);
		} else if (v >= 1019.5 && v < 1020.0) {
			return new Color(0, 119, 179);
        } else if (v >= 1020.0 && v < 1020.5) {
            return new Color(150, 254, 150);
        } else if (v >= 1020.5 && v < 1021.0) {
            return new Color(104, 252, 104);
        } else if (v >= 1021.0 && v < 1021.5) {
            return new Color(61, 249, 61);
        } else if (v >= 1021.5 && v < 1022.0) {
            return new Color(21, 243, 22);
        } else if (v >= 1022.0 && v < 1022.5) {
            return new Color(0, 234, 0);
        } else if (v >= 1022.5 && v < 1023.0) {
            return new Color(0, 213, 0);
        } else if (v >= 1023.0 && v < 1023.5) {
            return new Color(0, 189, 0);
        } else if (v >= 1023.5 && v < 1024.0) {
            return new Color(0, 164, 0);
        } else if (v >= 1024.0 && v < 1024.5) {
            return new Color(0, 142, 0);
        } else if (v >= 1024.5 && v < 1025.0) {
            return new Color(0, 128, 0);
        } else if (v >= 1025.0 && v < 1025.5) {
            return new Color(255,244,155);
        } else if (v >= 1025.5 && v < 1026.0) {
            return new Color(255,238,108);
        } else if (v >= 1026.0 && v < 1026.5) {
            return new Color(255,228,64);
        } else if (v >= 1026.5 && v < 1027.0) {
            return new Color(255,221,23);
        } else if (v >= 1027.0 && v < 1027.5) {
			return new Color(255, 214, 0);
		} else if (v >= 1027.5 && v < 1028.0) {
			return new Color(249, 205, 0);
		} else if (v >= 1028.0 && v < 1028.5) {
			return new Color(241, 197, 0);
		} else if (v >= 1028.5 && v < 1029.0) {
			return new Color(225, 185, 0);
		} else if (v >= 1029.0 && v < 1029.5) {
			return new Color(212, 177, 0);
		} else if (v >= 1029.5 && v < 1030.0) {
			return new Color(204, 171, 0);
		} else if (v >= 1030.0 && v < 1030.5) {
			return new Color(255, 174, 174);
		} else if (v >= 1030.5 && v < 1031.0) {
			return new Color(250, 133, 133);
		} else if (v >= 1031.0 && v < 1031.5) {
			return new Color(248, 95, 95);
		} else if (v >= 1031.5 && v < 1032.0) {
			return new Color(246, 59, 59);
		} else if (v >= 1032.0 && v < 1032.5) {
			return new Color(243, 25, 25);
		} else if (v >= 1032.5 && v < 1033.0) {
			return new Color(239, 2, 2);
		} else if (v >= 1033.0 && v < 1033.5) {
			return new Color(228, 0, 0);
		} else if (v >= 1033.5 && v < 1034.0) {
			return new Color(214, 0, 0);
		} else if (v >= 1034.0 && v < 1034.5) {
			return new Color(200, 0, 0);
        } else if (v >= 1034.5) {
            return new Color(191, 0, 0);
        }
		
		
		return null;
	}
}
