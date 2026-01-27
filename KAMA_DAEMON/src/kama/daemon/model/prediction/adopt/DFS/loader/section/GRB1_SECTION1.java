package kama.daemon.model.prediction.adopt.DFS.loader.section;

public final class GRB1_SECTION1 {

	public int length;
	public int version;
	public int center;
	public int id;
	public int grid;
	public int flag;
	public int unit;
	public int layer;
	public int height;
	public int YY;           		// 시작시간 : 년
	public int MM;           		// 시작시간 : 월
	public int DD;           		// 시작시간 : 일
	public int HH;           		// 시작시간 : 시
	public int MI;           		// 시작시간 : 분
	public int forecast_time_unit;
	public int P1;           		// 예보시간 (시작시간에서 +시간)
	public int P2;
	public int time_range;
	public int avg_num;
	public int missing_num;
	public int century;
	public int subcenter;
	public int Scale_D;

	public void clear() {
		this.length = 0;
		this.version = 0;
		this.center = 0;
		this.id = 0;
		this.grid = 0;
		this.flag = 0;
		this.unit = 0;
		this.layer = 0;
		this.height = 0;
		this.YY = 0;
		this.MM = 0;
		this.DD = 0;
		this.HH = 0;
		this.MI = 0;
		this.forecast_time_unit = 0;
		this.P1 = 0;
		this.P2 = 0;
		this.time_range = 0;
		this.avg_num = 0;
		this.missing_num = 0;
		this.century = 0;
		this.subcenter = 0;
		this.Scale_D = 0;
	}
	
}
