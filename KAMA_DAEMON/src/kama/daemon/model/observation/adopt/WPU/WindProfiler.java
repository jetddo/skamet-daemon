package kama.daemon.model.observation.adopt.WPU;

import java.util.Date;

/**
 * @author chlee
 * Created on 2016-12-08.
 */
public class WindProfiler implements Cloneable
{
    public Date RecordTime;
    public int WMO_LocationCode;
    public double Height;
    public double Latitude;
    public double Longitude;
    public double WindDirection;
    public double WindSpeed;
    public int DataStatus;
    public int IndexSequence;

    @Override
    public Object clone()
    {
        WindProfiler wf;

        wf = new WindProfiler();
        wf.RecordTime = (Date)RecordTime.clone();
        wf.Height = Height;
        wf.WMO_LocationCode = WMO_LocationCode;
        wf.Latitude = Latitude;
        wf.Longitude = Longitude;
        wf.WindDirection = WindDirection;
        wf.WindSpeed = WindSpeed;
        wf.DataStatus = DataStatus;
        wf.IndexSequence = IndexSequence;

        return wf;
    }
}