package kama.daemon.model.prediction.adopt.MOS;

import java.util.Date;

public class MOSData {
	public Date tm;			// 예측시간(KST)
	public Date fcstTm;		// 생산시간(UTC)
	public int stnId;		// 지점 아이디
	public String stnCd;	// 지점 코드
	public float tmp;		// 기온(℃)
}
