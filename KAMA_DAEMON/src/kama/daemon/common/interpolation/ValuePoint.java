package kama.daemon.common.interpolation;

public class ValuePoint {

	public float v;
	public float x;
	public float y;		
	public float lon;
	public float lat;	
	
	public ValuePoint(float x, float y, float v) {
		this.x = x;
		this.y = y;
		this.lon = x;
		this.lat = y;
		this.v = v;
	}

	@Override
	public String toString() {
		return "ValuePoint [v=" + v + ", x=" + x + ", y=" + y + ", lon=" + lon + ", lat=" + lat + "]";
	}
	
	
}
