package kama.daemon.model.prediction.adopt.UKMO;

import kama.daemon.common.util.JUnitTestUtil;
import org.junit.*;

import java.io.File;

/**
 * @author chlee
 * Created on 2016-12-29.
 */
public class UKMOTest
{
    /**
     * USST Loader가 해상 온도값을 잘 받아오는지 테스트
     * @throws Exception
     */
    @Test
    public void USSTLoaderTest() throws Exception
    {
        File file = new File(JUnitTestUtil.resourceTestDir("usst_raw.2016101800.gdat"));

        // 테스트 데이터 파일이 존재하여야 함.
        if (!(file.exists()))
        {
            throw new RuntimeException("Unable to locate test file.");
        }

        USSTLoader loader = new USSTLoader(file.getAbsolutePath());
        //double result = loader.GetValue(37.44f, 126.47f); // 지점이 -999인경우 해수면이 아님(?)
        //double result = loader.GetValue(36.00f, 125.14f); // 지점이 -999인경우 해수면이 아님(?)
        float result;

        //
        // 육지에서는: 반드시 섭씨 -999도가 나와야 함.
        // 해변 or 해상: 실제 온도값 나와야 함.
        //

        // 구로. -999 expected.
        result = loader.GetValue(37.494693f, 126.855905f);
        Assert.assertEquals(result, -999, 0.1);

        // 태백. -999 expected.
        result = loader.GetValue(37.175258f, 128.982488f);
        Assert.assertEquals(result, -999, 0.1);

        // 인천공항 앞바다. must not be -999.
        result = loader.GetValue(37.433425f, 126.46665f);
        Assert.assertNotEquals(result, -999, 0.1);

        // 진도 앞바다. must not be -999.
        result = loader.GetValue(34.383492f, 126.271934f);
        Assert.assertNotEquals(result, -999, 0.1);

        // 서해바다. must not be -999.
        result = loader.GetValue(36.116501f, 123.476232f);
        Assert.assertNotEquals(result, -999, 0.1);
    }
}