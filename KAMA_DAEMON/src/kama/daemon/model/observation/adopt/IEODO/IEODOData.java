package kama.daemon.model.observation.adopt.IEODO;

import java.util.Date;

public class IEODOData {
	public Date tm;		// 관측시간(KST)
	public float wd;	// 풍향(degree)
	public float ws;	// 풍속(m/s)
	public float wd_gst;// GUST풍향(degree)
	public float ws_gst;// GUST풍속(m/s)
	public float ta;	// 기온(C)
	public float hm;	// 습도(%)
	public float pa;	// 현지기압(hPa)
	public float rn_day;// 일강수량(mm)
	public float rn_int;// 강우강도(mm/h)
	public float si;	// 10분 일상(W/m^2)
	public float ss;	// 일조합(hour)
	public float vi;	// 시정(m)
	public float wh_max;// 최대파고(m)
	public float wh_sig;// 유의파고(m)
	public float wo;	// 파향(degree)
	public float wp;	// 파주기(sec)
	public float wg;	// 파장(m)
	public float sc;	// 유속(m/s)
	public float ls;	// 조위(m)
	public float tw;	// 수온(C)
	public float sa;	// 염분(psu)
	public float pm;	// 미립자수
	public float o3;	// 오존(PPB)
	// 2019-11-27 추가
	public float wh_sig2;// 유의파고2(m)
	public float cla_1lyr;	// 운량
	public float base_1lyr;	// 운고
	public float sd;	// 유향(degree)
}
