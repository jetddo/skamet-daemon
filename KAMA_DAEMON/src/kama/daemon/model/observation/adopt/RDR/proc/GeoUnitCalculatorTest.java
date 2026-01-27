package kama.daemon.model.observation.adopt.RDR.proc;

import kama.daemon.common.util.converter.PointF;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by chlee on 2017-02-23.
 */
public class GeoUnitCalculatorTest
{
    /**
     * 임의의 두 지점 거리를 측정 후, 실제 값과 맞는지 테스트
     * @throws Exception
     */
    @Test
    public void distanceBetweenTwoCoordinates() throws Exception
    {
        // 유럽의 랜덤한 두 지점
        // c1: 50.06638889, 5.71472222
        // c2: 58.64388889, 3.07000000

        GeoUnitCalculator.DistanceInfo info;
        double distance;

        // 오라클 출처
        // http://www.sunearthtools.com/tools/distance.php

        info = GeoUnitCalculator.distanceBetweenTwoCoordinates(new PointF(5.71472222, 50.06638889), new PointF(3.07000000, 58.64388889));
        Assert.assertEquals(969.1266, info.distance, 1);

        // 추가 임의 지점
        info = GeoUnitCalculator.distanceBetweenTwoCoordinates(new PointF(25.22644, 49.49890), new PointF(3.07, 58.64388889));
        Assert.assertEquals(1754.6542, info.distance, 1);

        info = GeoUnitCalculator.distanceBetweenTwoCoordinates(new PointF(126.39332580566406, 39.071475982666016), new PointF(128.006769, 37.616370));
        Assert.assertEquals(214.4777, info.distance, 1);

        double degrees = info.bearing * (180 / Math.PI);
        Assert.assertEquals(138.99, info.bearing, 1);

        // 바로 위값에서 파라메터 위치만 변경
        info = GeoUnitCalculator.distanceBetweenTwoCoordinates(new PointF(128.006769, 37.616370), new PointF(126.39332580566406, 39.071475982666016));

        degrees = info.bearing * (180 / Math.PI);
        Assert.assertEquals(318.99, info.bearing, 1);
    }

    /**
     * A, B 두 지점 사이의 거리 및 방향 계산 후, A로부터 B지점까지의 거리로 B지점 역계산하여
     * 값이 동일한지 확인
     */
    @Test
    public void distanceTest()
    {
        PointF a = new PointF(126.39332580566406, 39.071475982666016);
        PointF b = new PointF(128.006769, 37.616370);

        GeoUnitCalculator.DistanceInfo info = GeoUnitCalculator.distanceBetweenTwoCoordinates(a, b);
        Assert.assertEquals(214.4777, info.distance, 1);

        double degrees = info.bearing * (180 / Math.PI);

        Assert.assertEquals(138.99, info.bearing, 1);

        PointF output = GeoUnitCalculator.calculateDestination(a, info.distance, info.bearing);

        Assert.assertTrue(isDoubleEquals(b.getX(), output.getX(), 0.5) && isDoubleEquals(b.getY(), output.getY(), 0.5));
    }

    private final boolean isDoubleEquals(double value1, double value2, double epsilon)
    {
        return (Math.abs(value2 - value1) < epsilon);
    }
}