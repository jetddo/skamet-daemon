package kama.daemon.model.prediction.adopt.SHEAR;

import java.util.Date;
import java.util.List;

public class SHEARData {
	public Date tm;				// 예측시간(KST)
	public Date fcstTm;			// 생산시간(UTC)
	public String stnCd;		// 지점 코드
	public int idx;				// IDX
	public List<Object> list; 	// values
}
