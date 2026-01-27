package kama.daemon.common.util.model;

public class PointLonLat {
	
	private double lon;
    private double lat;    

    public PointLonLat(double lon, double lat) {        
        this.lon = lon;
        this.lat = lat;
    }

	public double getLat() {
		return lat;
	}

	public void setLat(double lat) {
		this.lat = lat;
	}

	public double getLon() {
		return lon;
	}

	public void setLon(double lon) {
		this.lon = lon;
	}

	@Override
	public String toString() {
		return "PointLonLat [lon=" + lon + ", lat=" + lat + "]";
	}    
	
	
}