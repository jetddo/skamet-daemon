package kama.daemon.model.assessment.adopt.RISK_MATRIX_GRID;

import java.util.Date;

public class RISK_MATRIX_Data {
	public Date InputTime;// 입력 시간
	public int stnId;	// 지점 아이디
	public int member;	// 앙상블 멤버(0~12, 13)
	public Date fctTm;	// 생산 시간
	public Date tm;	// 예측 시간
	public int lowHeight;	// 하위 고도
	public int highHeight;	// 상위 고도
	public float windSpeed;	// 풍속
	public float windDirection;	// 풍향
	public float temperature;	// 기온
}
