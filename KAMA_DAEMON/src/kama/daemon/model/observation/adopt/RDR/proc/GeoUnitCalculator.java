package kama.daemon.model.observation.adopt.RDR.proc;

import kama.daemon.common.util.converter.PointF;

/**
 * Created by chlee on 2017-02-23.
 */
public class GeoUnitCalculator
{
    private static final int RADIUS_EARTH = 6371;

    public static class DistanceInfo
    {
        public double distance;
        public double bearing;
    }

    /**
     * 특정 경위도 지점으로부터 일정 거리 (km) 로 이동했을때 목적지의 경위도값을 받아오는 함수
     * @param c1 시작 경위도 지점
     * @param distance 거리 (km)
     * @param bearing 각도 (0 ~ 360)
     * @return 계산한 목적지의 경위도 지점
     */
    public static PointF calculateDestination(PointF c1, double distance, double bearing)
    {
        double ang_dist; // delta. angular distance d/R

        ang_dist = distance / RADIUS_EARTH;

        double lat2 = ToDegrees(Math.asin(Math.sin(ToRadians(c1.getY())) * Math.cos(ang_dist) + Math.cos(ToRadians(c1.getY())) * Math.sin(ang_dist) * Math.cos(ToRadians(bearing))));
        double long2 = ToDegrees(ToRadians(c1.getX()) + Math.atan2(Math.sin(ToRadians(bearing)) * Math.sin(ang_dist) * Math.cos(ToRadians(c1.getY())),
                Math.cos(ang_dist) - Math.sin(ToRadians(c1.getY())) * Math.sin(ToRadians(lat2))));

        //Console.WriteLine("{0}, {1}", (double)lat2, (double)long2);

        return new PointF((float)long2, (float)lat2);
    }

    /**
     * 두 경위도 지점간의 거리(km)와 각도 계산
     * http://www.movable-type.co.uk/scripts/latlong.html
     * @param c1 시작 경위도 지점
     * @param c2 끝 경위도 지점
     * @return 두 지점간의 거리 (km) 및 시작 지점에서 끝 지점으로부터의 각도 (0~360)
     */
    public static DistanceInfo distanceBetweenTwoCoordinates(PointF c1, PointF c2)
    {
        double R = RADIUS_EARTH * 1000;
        double pi1 = ToRadians(c1.getY());
        double pi2 = ToRadians(c2.getY());
        double delta_pi = ToRadians(c2.getY() - c1.getY());
        double delta_lambda = ToRadians(c2.getX() - c1.getX());

        // angular distance in radians
        double a = Math.pow(Math.sin(delta_pi / 2), 2) +
                Math.cos(pi1) * Math.cos(pi2) *
                Math.pow(Math.sin(delta_lambda / 2), 2);

        // square of half the chord length between the points
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));

        // bearing 계산
        double y = Math.sin(delta_lambda) * Math.cos(pi2);
        double x = Math.cos(pi1) * Math.sin(pi2) - Math.sin(pi1) * Math.cos(pi2) * Math.cos(ToRadians(c2.getY()) - ToRadians(c1.getY()));
        double bearing = ToDegrees(Math.atan2(y, x));

        if (bearing < 0)
        {
            bearing += 360;
        }

        DistanceInfo distanceInfo = new DistanceInfo();
        distanceInfo.bearing = bearing;
        distanceInfo.distance = RADIUS_EARTH * c; // km

        return distanceInfo;
    }

    /**
     * 도분초 => 라디안 변경 함수
     * @param degree 각도
     * @return 라디안
     */
    // http://stackoverflow.com/questions/135909/what-is-the-method-for-converting-radians-to-degrees
    private static double ToRadians(double degree)
    {
        return degree * (Math.PI / 180);
    }

    /**
     * 라디안 => 도분초 변경 함수
     * @param radians 라디안
     * @return 각도
     */
    private static double ToDegrees(double radians)
    {
        return radians * (180 / Math.PI);
    }
}
