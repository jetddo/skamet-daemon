package kama.daemon.model.prediction.adopt.RDAPS_SGL.loader;

import kama.daemon.common.util.JUnitTestUtil;
import kama.daemon.common.util.converter.Lambert;
import kama.daemon.common.util.converter.PointF;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * @author chlee
 * Created on 2017-01-02.
 */
@Ignore
public class RDAPS_SGLTest
{
    // Granularity: class 단위
    // Test 목적: 유효값을 잘 뽑아오는지 테스트

    /**
     * 안개-습도 예상도
     * @throws Exception
     */
    @Test
    public void FogHumidityLoaderTest() throws Exception
    {
        FogHumidityLoader loader = new FogHumidityLoader(JUnitTestUtil.resourceTestDir("ldps_l1p5_fk4s.2016110606_000-000"), 1); // 끝에 000: 현재 시간, 001: 다음 시간, 002: , ... 036까지
        double[] tt;
        double ret;
        //tt = loader.getVisbValue(41.2351f, 127.5761f);
        // 유효 습도값: 0 ~ 100

        // 동해시, 습도높음
        tt = loader.getValue(37.524019f, 129.098084f);
        Assert.assertTrue(tt[0] >= 0 && tt[0] <= 100);
        Assert.assertTrue(tt[0] > 50);

        // 북한 문천시, 습도높음
        tt = loader.getValue(39.267123f, 127.350794f);
        Assert.assertTrue(tt[0] >= 0 && tt[0] <= 100);
        Assert.assertTrue(tt[0] > 50);

        // 서울, 습도낮음
        tt = loader.getValue(37.552731f, 126.984004f);
        Assert.assertTrue(tt[0] >= 0 && tt[0] <= 100);
        Assert.assertTrue(tt[0] <= 50);

        // Shenyang, 습도낮음
        tt = loader.getValue(41.843780f, 123.439764f);
        Assert.assertTrue(tt[0] >= 0 && tt[0] <= 100);
        Assert.assertTrue(tt[0] <= 50);

        // 군산앞바다, 습도 다소 높음
        tt = loader.getValue(35.957495f, 126.378735f);
        //tt = loader.getVisbValue(34.5484, 129.373961);
        Assert.assertTrue(tt[0] >= 0);
        Assert.assertTrue(tt[0] <= 100);
        Assert.assertTrue(tt[0] > 50);
    }

    /**
     * 안개-시정
     * @throws Exception
     */
    @Test
    public void UFogLoaderTest() throws Exception
    {
        // daba_ldps_conline.dat needed
        UFogLoader loader = new UFogLoader(JUnitTestUtil.resourceTestDir("ldps_l1p5_ufog.2016110606_000"));
        // 계절, 날씨 상황에 따라 특정 지역의 실제 시정값이 평소와 다를 수 있음. 이 경우에 옳은 데이터임에도 오류로 판별할 수 있으니 확인 요망.
        String visErrMessage = "Unusual visibility value for the selected area. Please review if a false negative.";

        double[] visb;

        // 유효 시정값: 0 ~ 50

        // 제주시, 시정낮음
        visb = loader.getVisGValue(33.592922f, 126.533182f);
        Assert.assertTrue(visErrMessage, visb[0] >= 0 && visb[0] <= 50);
        Assert.assertTrue(visErrMessage, visb[0] <= 25);

        // 북한 신포, 시정 다소 높음
        visb = loader.getVisGValue(41.375205f, 129.769378f);
        Assert.assertTrue(visErrMessage, visb[0] >= 0 && visb[0] <= 50);
        Assert.assertTrue(visErrMessage, visb[0] > 20);

        // 서울, 시정 보통
        visb = loader.getVisGValue(37.552731f, 126.984004f);
        Assert.assertTrue(visErrMessage, visb[0] >= 0 && visb[0] <= 50);
        Assert.assertTrue(visErrMessage, visb[0] >= 10 && visb[0] <= 40);

        // Shenyang, 시정 보통
        visb = loader.getVisGValue(41.843780f, 123.439764f);
        Assert.assertTrue(visErrMessage, visb[0] >= 0 && visb[0] <= 50);
        Assert.assertTrue(visErrMessage, visb[0] >= 10 && visb[0] <= 40);

        // 군산앞바다, 시정 낮음
        visb = loader.getVisGValue(35.957495f, 126.378735f);
        Assert.assertTrue(visErrMessage, visb[0] >= 0 && visb[0] <= 50);
        Assert.assertTrue(visErrMessage, visb[0] <= 25);
    }

    /**
     * 구름변수-안개가이던스 (안개-구름 물리변수기반 안개가이던스)
     * @throws Exception
     */
    @Test
    public void FogGuidanceLoaderTest() throws Exception
    {
        FogGuidanceLoader loader = new FogGuidanceLoader(JUnitTestUtil.resourceTestDir("lfog_visb.2017010100_000"));
        // 계절, 날씨 상황에 따라 특정 지역의 실제 시정값이 평소와 다를 수 있음. 이 경우에 옳은 데이터임에도 오류로 판별할 수 있으니 확인 요망.
        String visErrMessage = "Unusual visibility value for the selected area. Please review if a false negative.";

        double visb;

        // 제주시, 시정 낮음
        visb = loader.getVisbValue(33.592922f, 126.533182f);
        Assert.assertTrue(visErrMessage, visb >= 0 && visb <= 50);
        Assert.assertTrue(visErrMessage, visb <= 25);

        // 북한 신포, 시정 다소 높음
        visb = loader.getVisbValue(41.375205f, 129.769378f);
        Assert.assertTrue(visErrMessage, visb >= 0 && visb <= 50);
        Assert.assertTrue(visErrMessage, visb > 20);

        // 서울, 시정 보통
        visb = loader.getVisbValue(37.552731f, 126.984004f);
        Assert.assertTrue(visErrMessage, visb >= 0 && visb <= 50);
        Assert.assertTrue(visErrMessage, visb >= 10 && visb <= 40);

        // Shenyang, 시정 보통
        visb = loader.getVisbValue(41.843780f, 123.439764f);
        Assert.assertTrue(visErrMessage, visb >= 0 && visb <= 50);
        Assert.assertTrue(visErrMessage, visb >= 10 && visb <= 40);

        // 군산앞바다, 시정 낮음
        visb = loader.getVisbValue(35.957495f, 126.378735f);
        Assert.assertTrue(visErrMessage, visb >= 0 && visb <= 50);
        Assert.assertTrue(visErrMessage, visb <= 25);

        visb = loader.getVisbValue(33.200410, 129.982112);
        //System.out.println(visb);
    }

    @Test@Ignore
    public void lambertConicConversionTest() throws Exception
    {
        PointF pointF;
        FogGuidanceLoader loader = new FogGuidanceLoader(JUnitTestUtil.resourceTestDir("lfog_visb.2017010100_000"));
        // 계절, 날씨 상황에 따라 특정 지역의 실제 시정값이 평소와 다를 수 있음. 이 경우에 옳은 데이터임에도 오류로 판별할 수 있으니 확인 요망.
        String visErrMessage = "Unusual visibility value for the selected area. Please review if a false negative.";

        Lambert lambert = new Lambert(Lambert.GRID_PARAM_TYPE.RDAPS_SGL_EQVPTL);
        pointF = lambert.lambertToWgs84(301, 0);

        pointF = lambert.lambertToWgs84(301, 780);
        pointF = lambert.lambertToWgs84(0, 0);
        pointF = lambert.lambertToWgs84(601, 780);

        double visb;
        double lat = 38, lon = 126;

        while (Math.abs(lat - 33.695417) > 0.01 && Math.abs(lon - 131.750911) > 0.01)
        {
            visb = loader.getVisbValue(lat, lon);
            System.out.println(visb);

            lat -= 0.01;
            lon += 0.01;
        }
    }

    @Test
    public void EquivPotentialTempLoaderTest() throws Exception
    {
        // 상당온위 테스트
        // 유효값 범위: 282~369
        EquivPotentialTempLoader loader;

        loader = new EquivPotentialTempLoader(JUnitTestUtil.resourceTestDir("ldps_lc06_grph.2016110606_000"));

        double val = loader.getEq700Value(35.305435, 129.011095);
        Assert.assertTrue(val >= 282 && val <= 369);

        val = loader.getEq850Value(35.305435, 129.011095);
        Assert.assertTrue(val >= 282 && val <= 369);

        val = loader.getEq925Value(35.305435, 129.011095);
        Assert.assertTrue(val >= 282 && val <= 369);
    }
}