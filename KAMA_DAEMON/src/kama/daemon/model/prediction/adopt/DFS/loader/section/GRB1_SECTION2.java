package kama.daemon.model.prediction.adopt.DFS.loader.section;

public final class GRB1_SECTION2 {

	public int length;
	public int NV;
	public int PV;
	public int data_type;
	public int nx;
	public int ny;
	public int lat1;
	public int lon1;
	public int flag;
	public int lov;
	public int dx;
	public int dy;

	public void clear() {
		this.length = 0;
		this.NV = 0;
		this.PV = 0;
		this.data_type = 0;
		this.nx = 0;
		this.ny = 0;
		this.lat1 = 0;
		this.lon1 = 0;
		this.flag = 0;
		this.lov = 0;
		this.dx = 0;
		this.dy = 0;
	}

}
