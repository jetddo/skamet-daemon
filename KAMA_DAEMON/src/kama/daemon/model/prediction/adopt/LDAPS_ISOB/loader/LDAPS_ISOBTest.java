package kama.daemon.model.prediction.adopt.LDAPS_ISOB.loader;

import kama.daemon.common.util.JUnitTestUtil;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * @author chlee
 * Created on 2017-01-16.
 */
public class LDAPS_ISOBTest
{
    // private static final String DATA_FILE_PATH = JUnitTestUtil.resourceTestDir("ldps_city_grph_2017082118.dat");
	private static final String DATA_FILE_PATH = JUnitTestUtil.resourceTestDir("ldps_city_grph_2016110600.dat");
    private static final int STATION_ID = 47092;

    //
    //  (2017/01/20)
    //  Memo: 연직시계열 부분은 일부 test case 들이 미완성으로 구현되어 있음.
    //

    @Test
    @Ignore
    public void getHumidityByAltitudeTest() throws Exception
    {
        // 고도에 따른 습도값 계산하는 테스트
        // 유효값이 나오지 않아 추후 VHumidityCalculator 모듈 자체를 제거 예정 (작성일: 2017/01/17)
        VerticalVisibilityLoader loader = new VerticalVisibilityLoader(DATA_FILE_PATH);
        double[][] values = loader.getTemperatureListByPresAndTime(STATION_ID);

        VHumidityCalculator calc = new VHumidityCalculator(loader);

        for (int i = 0; i < values.length; i++)
        {
            for (int j = 0; j < values[i].length; j++)
            {
                double relHum = calc.calculateRelativeHumidityByFloorLv(STATION_ID, i, j);

                if (!(0 <= relHum && 100 >= relHum))
                {
                    // 무효값.
                    return;
                }
                //Assert.assertTrue(0 <= relHum && 100 >= relHum);
            }
        }
    }

    @Test
    public void getRainInfoListTest() throws Exception
    {
        VerticalVisibilityLoader loader = new VerticalVisibilityLoader(DATA_FILE_PATH);
        double[] values = loader.getRainInfoListByTime(STATION_ID);

        for (int i = 0; i < values.length; i++)
        {

        }
    }

    @Test
    public void getRelHumidityListTest() throws Exception
    {
        VerticalVisibilityLoader loader = new VerticalVisibilityLoader(DATA_FILE_PATH);
        double[] values = loader.getRelativeHumidityListByTime(STATION_ID);

        for (int i = 0; i < values.length; i++)
        {

        }
    }

    /**
     * 등압면 온도값 가져오기 (상당온위값 포함)
     * @throws Exception
     */
    @Test
    public void getTemperatureListTest() throws Exception
    {
        VerticalVisibilityLoader loader = new VerticalVisibilityLoader(DATA_FILE_PATH);
        double[][] values = loader.getTemperatureListByPresAndTime(STATION_ID);
        
        System.out.println(values.length);
        for (int pres = 0; pres < values.length; pres++)
        {
            for (int time = 0; time < values[pres].length; time++)
            {
            	System.out.println("Validate temperature: " + values[pres][time] + ", pres: " + pres + ",time: " + time);
                double eqvPtlTemp;
                int pressureValue;
                int altitudeMeter;

                pressureValue = VerticalVisibilityLoader.floorIndexToHectoPascal(pres);
                altitudeMeter = VerticalVisibilityLoader.floorIndexToAltitudeMeter(pres);
                
                // 대기권 유효 온도범위는 섭씨로 -100 < 온도 < 50
                Assert.assertTrue("Validate temperature", values[pres][time] < 50 && values[pres][time] > -100);

                // 상당온위값 계산 (유효범위 -100 ~ 500, 재확인 필요...)
                eqvPtlTemp = (values[pres][time] + 273.15) * Math.pow((1000 / pressureValue), 0.288) - 273.15;
                Assert.assertTrue("Validate temperature", eqvPtlTemp < 500 && eqvPtlTemp > -100);
            }
        }
    }

    @Test
    public void getIceInfoTest() throws Exception
    {
        VerticalVisibilityLoader loader = new VerticalVisibilityLoader(DATA_FILE_PATH);
        double[][] values = loader.getCloudWaterInfoListByPresAndTime(STATION_ID);
    }

    /**
     * 단일면 습도값 가져오기
     * @throws Exception
     */
    @Test
    public void getRelativeHumidityListTest() throws Exception
    {
        VerticalVisibilityLoader loader = new VerticalVisibilityLoader(DATA_FILE_PATH);
        double[] values = loader.getRelativeHumidityListByTime(STATION_ID);

        for (int i = 0; i < values.length; i++)
        {
        	System.out.println("Validate humidity: " + values[i]);
            // 유효 습도 범위: 0 ~ 100%
            Assert.assertTrue("Validate humidity", values[i] >= 0 && values[i] <= 100);
        }
    }

    /**
     * 등압면 풍향, 풍속 가져오기
     * @throws Exception
     */
    @Test
    public void getWindPropertyTestUV() throws Exception
    {
        VerticalVisibilityLoader loader = new VerticalVisibilityLoader(DATA_FILE_PATH);
        double[][] u_values = loader.getWindUListByPresAndTime(STATION_ID);
        double[][] v_values = loader.getWindVListByPresAndTime(STATION_ID);

        double windDir, windSpd;

        for (int pres = 0; pres < u_values.length; pres++)
        {
            for (int time = 0; time < u_values[pres].length; time++)
            {
                // 풍향, 풍속값 계산
                windDir = (270 - Math.atan2(v_values[pres][time], u_values[pres][time]) * 180 / Math.PI) % 360;
                windSpd = Math.sqrt(Math.pow(u_values[pres][time], 2) + Math.pow(v_values[pres][time], 2));

                System.out.println("U:" + u_values[pres][time] + ",V: " + v_values[pres][time] + ", SP:" + windSpd+ ", SD:" + windDir);

                // 풍향 값은 각도이기에, 0에서 360 사이의 값이 나와야 함
                Assert.assertTrue("Wind direction validation", windDir >= 0 && windDir <= 360);

                // 풍속값이 m/s 라는 전제하에 0에서 200 사이 값으로 가정.
                // (여유있게 200으로 가정하였음. 추후 assert fail 시에 값 상향 필요.)
                Assert.assertTrue("Wind speed validation", windSpd >= 0 && windSpd <= 200);
            }
        }
    }

    @Test
    public void testHeight()
    {
        // 고도값 (1000 ~ 50 hPa) to m

    }
}