package kama.daemon.model.observation.adopt.NAVY;

import java.util.Date;

/**
 * @author chlee
 * Created on 2016-12-12.
 */
public class NAVYData
{
    public Date RecordTime;
    public int StationID;
    public double MOR;
    public double cloudHeight;
    public double windDirection;
    public double windSpeed;
    public double temperature;
    public double humidity;
	@Override
	public String toString() {
		return "NAVYData [RecordTime=" + RecordTime + ", StationID="
				+ StationID + ", MOR=" + MOR + ", cloudHeight=" + cloudHeight
				+ ", windDirection=" + windDirection + ", windSpeed="
				+ windSpeed + ", temperature=" + temperature + ", humidity="
				+ humidity + "]";
	}
    
    
}
