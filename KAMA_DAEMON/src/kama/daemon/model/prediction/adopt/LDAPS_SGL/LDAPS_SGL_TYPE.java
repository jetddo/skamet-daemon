package kama.daemon.model.prediction.adopt.LDAPS_SGL;

import java.lang.reflect.Field;
import java.util.Date;
import java.util.HashMap;

/**
 * @author chlee
 * Created on 2017-01-05.
 */
public class LDAPS_SGL_TYPE
{
    public int StationID = Integer.MIN_VALUE;
    public Date PredictedTime = null;
    public Date ModelProducedTime = null;
    public double FogVisibility = Double.NaN;
    public double CloudVariableVisibility = Double.NaN;
    public double WindDirection = Double.NaN;
    public double WindSpeed = Double.NaN;
    public double Temperature = Double.NaN;
    public double Humidity = Double.NaN;
    public double FloudFloorAltitude = Double.NaN;
    public int PrecipBool = Integer.MIN_VALUE;
    public double ConsiderableTemperature = Double.NaN;
}