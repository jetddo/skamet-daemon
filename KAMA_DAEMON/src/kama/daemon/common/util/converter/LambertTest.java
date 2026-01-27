package kama.daemon.common.util.converter;

import kama.daemon.common.util.Log;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author chlee
 * Created on 2017-01-11.
 */
public class LambertTest
{
    @Test
    public void dummyTest() throws Exception
    {
        Lambert lambert;
        lambert = new Lambert(Lambert.GRID_PARAM_TYPE.RDR);

        PointF ptf = lambert.lambertToWgs84(367, 0);

        Log.print(Double.toString(ptf.x));
    }
    
    @Test
    public void dummyTest1() throws Exception
    {
        Lambert lambert;
        lambert = new Lambert(Lambert.GRID_PARAM_TYPE.RDR2);

        int x = 960;
        int y = 1200;
        
        double top = Double.MIN_VALUE;
        double bottom = Double.MAX_VALUE;
        double left = Double.MAX_VALUE;
        double right =  Double.MIN_VALUE;
        
        //121.5453,30.79203,132.5218,40.38961
        
//        41.95685958862305
//        30.73603630065918
//        121.41840362548828
//        133.3776092529297

        for(int i=0 ; i<x ; i++) {
        	
        	for(int j=0 ; j<y ; j++) {
        		
        		PointF ptf = lambert.lambertToWgs84(i, j);
        		
        		top = Math.max(ptf.y, top);
        		bottom = Math.min(ptf.y, bottom);
        		left = Math.min(ptf.x, left);
        		right = Math.max(ptf.x, right);
        	}
        }
        
        System.out.println(top);
        System.out.println(bottom);
        System.out.println(left);
        System.out.println(right);
    }

    @Test
    public void lamcproj() throws Exception
    {
        // 테스트 방법 (임의의 점을 찍어서 대한민국 주변 좌표가 나오면 됨.)
        Lambert lambert;
        lambert = new Lambert(Lambert.GRID_PARAM_TYPE.RDAPS_SGL_EQVPTL);

        // XY 좌표 (0,0): 대한민국 서남쪽 서해바다 (중국 동남쪽 상하이 주변)
        PointF eastChina = lambert.lambertToWgs84(0, 0);

        // 상하이 주변 좌표와 일치 여부
        Assert.assertEquals(eastChina.getY(), 32.145024, 0.5);
        Assert.assertEquals(eastChina.getX(), 121.756416, 0.5);

        // 특정 xy 좌표값을 경위도로 변환 후 다시 xy 좌표로 변환하였을 때 원래의 입력값이 나오면 pass.
        testXY2LatLon(0, 0);
        testXY2LatLon(10, 0);
        testXY2LatLon(20, 10);
        testXY2LatLon(20, 20);
        testXY2LatLon(100, 100);

        // 특정 경위도값을 xy 좌표로 변환 후 다시 경위도로 변환하였을 때 원래의 입력값이 나오면 pass.
        testLatLon2XY(37.560412, 126.802258); // 김포공항 주변
        testLatLon2XY(37.503033, 126.881986); // 구로역 주변
        testLatLon2XY(35.178161, 129.169325); // 부산 해운대구
        testLatLon2XY(36.076954, 120.380178); // 중국 청도
        testLatLon2XY(42.012069, 128.059898); // 백두산 천지
    }

    @Test
    public void testKnownCoordinate()
    {
        Lambert lambert;
        lambert = new Lambert(Lambert.GRID_PARAM_TYPE.RDAPS_SGL_EQVPTL);

        PointF latlon;
        double[] xy;

        latlon = lambert.lambertToWgs84(10, 10);
        Assert.assertTrue(compareValues(new double[] { 32.647857666015625, 122.3070068359375 }, new double[] { latlon.getY(), latlon.getX() }));

        latlon = lambert.lambertToWgs84(50, 100);
        Assert.assertTrue(compareValues(new double[] { 37.6607551574707, 124.81124877929688 }, new double[] { latlon.getY(), latlon.getX() }));

        // 2017/03/03 추가
        // lambert 변환시에 (367,0)에서 위도값이 0로 나오는 문제점이 발견되었고, 수정 완료.
        latlon = lambert.lambertToWgs84(367, 0);
        Assert.assertTrue(compareValues(new double[] { 30.24211883544922, 144.9334259033203 }, new double[] { latlon.getY(), latlon.getX() }));
    }

    private boolean compareValues(double[] coordinate1, double[] coordinate2)
    {
        boolean equals;

        equals = (Math.abs(coordinate1[0] - coordinate2[0]) < 0.001);
        equals &= (Math.abs(coordinate1[1] - coordinate2[1]) < 0.001);

        return equals;
    }

    private void testXY2LatLon(double input_x, double input_y)
    {
        Lambert lambert;
        lambert = new Lambert(Lambert.GRID_PARAM_TYPE.RDAPS_SGL_EQVPTL);

        PointF latlon = lambert.lambertToWgs84(input_x, input_y);
        PointF output = lambert.wgs84ToLambert(latlon.getY(), latlon.getX());

        Assert.assertEquals(input_x, output.getX(), 0.5);
        Assert.assertEquals(input_y, output.getY(), 0.5);
    }

    private void testLatLon2XY(double input_lat, double input_lon)
    {
        Lambert lambert;
        lambert = new Lambert(Lambert.GRID_PARAM_TYPE.RDAPS_SGL_EQVPTL);

        PointF xy = lambert.wgs84ToLambert(input_lat, input_lon);
        PointF output = lambert.lambertToWgs84(xy.getX(), xy.getY());

        Assert.assertEquals(input_lat, output.getY(), 0.5);
        Assert.assertEquals(input_lon, output.getX(), 0.5);
    }
}