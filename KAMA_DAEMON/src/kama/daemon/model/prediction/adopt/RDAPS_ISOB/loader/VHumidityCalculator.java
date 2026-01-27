package kama.daemon.model.prediction.adopt.RDAPS_ISOB.loader;

/**
 * @author chlee
 * Created on 2017-01-17.
 */
public class VHumidityCalculator
{
    private final VerticalVisibilityLoader _loader;

    public VHumidityCalculator(VerticalVisibilityLoader loader)
    {
        _loader = loader;
    }

    public double calculateDewPoint(int stnID, int time)
    {
        double[] values = _loader.getRelativeHumidityListByTime(stnID);
        double humidity = values[time];
        double[][] t_values = _loader.getTemperatureListByPresAndTime(stnID);
        double temperature = t_values[0][time]; // 가장 최하층 (지표면) 의 온도값

        double dewPoint;

        dewPoint = temperature - ((100 - humidity) / 5.0);

        return dewPoint;
    }

    public double calculateDewPointByFloorLv(int stnID, int time, int floorLevel)
    {
        double dewPoint = calculateDewPoint(stnID, time);
        double altitude = VerticalVisibilityLoader.floorIndexToAltitudeMeter(floorLevel);
        double newDewPoint;

        // 100m 당 이슬점 온도 0.2도 하강
        newDewPoint = dewPoint - ((altitude / 100) * 0.2);

        return newDewPoint;
    }

    public double calculateRelativeHumidityByFloorLv(int stnID, int time, int floorLevel)
    {
        double newDewPoint = calculateDewPointByFloorLv(stnID, time, floorLevel);
        double[][] t_values = _loader.getTemperatureListByPresAndTime(stnID);
        double temperature = t_values[floorLevel][time];
        double relHumidity;

        relHumidity = 100 - 5 * (temperature - newDewPoint);

        return relHumidity;
    }
}