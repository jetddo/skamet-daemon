package kama.daemon.model.prediction.adopt.WAFC;

import kama.daemon.common.util.JUnitTestUtil;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author chlee
 * Created on 2017-01-24.
 */
public class WAFCLoaderTest
{
    /**
     * 경위도 값을 주고 예시 데이터의 coordinate 값을 제대로 받아오는지 테스트
     * @throws Exception
     */
    @Test
    public void loadGribDataByLonLatTest() throws Exception
    {
        WAFCLoader loader = new WAFCLoader(JUnitTestUtil.resourceTestDir("20161009_0000f36.grib2"));

        double val;

        val = loader.getTropoHeight(90, 0);
        Assert.assertEquals(9631.135, val, 0.01);

        val = loader.getTropoHeight(88.75, 0);
        Assert.assertEquals(9090.935, val, 0.01);

        val = loader.getTropoHeight(85.0, 207.5);
        Assert.assertEquals(9272.534, val, 0.01);

        val = loader.getTropoHeight(33.75, 5.0);
        Assert.assertEquals(12190.534, val, 0.01);

        val = loader.getTropoHeight(-76.25, 202.5);
        Assert.assertEquals(7862.935, val, 0.01);
    }
}