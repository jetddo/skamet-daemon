package kama.daemon.model.prediction.adopt.UKMO;

/**
 * @author chlee
 * Created on 2016-12-29.
 */
public class AirportData
{
    public static final String[] Names = { "덕적도", "칠발도", "부안", "거문도", "통영", "추자도", "거제도", "동해", "울산" };
    public static final int[] StnIDs = { 22101, 22102, 22186, 22103, 22188, 22184, 22104, 22105, 22189 };
    public static final float[] Lats = { 37.24f, 34.79f, 35.66f, 34.0f, 34.39f, 33.79f, 34.77f, 37.48f, 35.35f };
    public static final float[] Lons = { 126.02f, 125.78f, 125.81f, 127.5f, 128.23f, 126.14f, 128.9f, 129.95f, 129.84f };
    public static final int Length = Names.length;
}