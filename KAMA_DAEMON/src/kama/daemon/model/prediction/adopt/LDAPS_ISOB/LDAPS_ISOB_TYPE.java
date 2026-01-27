package kama.daemon.model.prediction.adopt.LDAPS_ISOB;

import java.util.Date;

/**
 * @author chlee
 * Created on 2017-01-16.
 */
public class LDAPS_ISOB_TYPE
{
    public int StationID = Integer.MIN_VALUE;
    public Date PredictedTime = null;
    public Date ModelProducedTime = null;
    public int Floor = Integer.MIN_VALUE;
    public double Altitude = Double.NaN;
    public double WindDirection = Double.NaN;
    public double WindSpeed = Double.NaN;
    public double Temperature = Double.NaN;
    public double EquivPotlTemperature = Double.NaN;
    public double LessThanFourCalc = Double.NaN;
    public double IceInfo = Double.NaN;
    public double WaterInfo = Double.NaN;
}
