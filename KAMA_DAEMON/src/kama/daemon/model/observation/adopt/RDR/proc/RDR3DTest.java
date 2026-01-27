package kama.daemon.model.observation.adopt.RDR.proc;

import kama.daemon.common.util.JUnitTestUtil;
import kama.daemon.common.util.GZipTgzReader;
import kama.daemon.common.util.Log;
import kama.daemon.common.util.converter.Lambert;
import kama.daemon.common.util.converter.PointF;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * @author chlee
 * Create on 1/18/2017.
 */
public class RDR3DTest
{
    private final File SAMPLE_RDR_FILE = new File(JUnitTestUtil.resourceTestDir("RDR_CNQCZ_3D80_201609010100.bin.gz"));

    /**
     * x, y 값 뽑아내기 위한 함수 (테스트 목적이 아님).
     */
    @Test
    public void extract_XY_coordinates()
    {
        PointF pointF;
        Lambert lambert;
        lambert = new Lambert(Lambert.GRID_PARAM_TYPE.RDR);

        //pointF = lambert.lambertToWgs84(960, 1200);
        pointF = lambert.lambertToWgs84(320, 1200 - 873);

        Log.print(String.format("%f,%f", pointF.getX(), pointF.getY()));
    }

    /**
     * 4개의 사전에 정의된 항로 (G597 등)의 연직 단면도를 이미지 파일로 표출하는 테스트
     * @throws Exception
     */
    @Test
    public void extractVerticalSliceFromRouteID() throws Exception
    {
        String[] routeIDs = { "G597", "G585", "A582", "A593", "Y722", "Y711" };
        RDR3dReader reader;
        reader = new RDR3dReader(SAMPLE_RDR_FILE.getAbsolutePath());
        String MD5_oracle[] = { "5a7d80d9a9f9c03930e997193c7fb800", "c83d7d7c854ef62c5bc2b2e898637d28", "ced5cf7a49482f1fd863f3d66d14811e", "ae1cae969935f30adc842f98fd7a42fd", "ca53b3f4137bcfb27b049cfb8de4b6f9", "f224fae167a62024e74b6b2366aaa69b" };
        boolean testPassed = true;

        reader.regridAndSmoothAllLayers();

        for (int i = 0; i < routeIDs.length; i++)
        {
            RDR3dLayer layer = reader.getVerticalSlice(routeIDs[i], 500);
            File outputFile = new File(JUnitTestUtil.tempOutputDir(String.format("%02d_%s.png", i, routeIDs[i])));

            layer.saveAs(outputFile, true);

            String hash = getMD5Hash(outputFile);
            testPassed &= hash.equals(MD5_oracle[i]);
        }

        Assert.assertTrue("extractCompleteVerticalTest() failed.", testPassed);
    }

    /**
     * 임의의 한 지점에 대한 연직 값을 받아와서 이미지 파일로 표출
     * (1차원 점을 쭉 늘려 선으로 표출)
     * @throws Exception
     */
    @Test
    public void extractOneVerticalValueTest() throws Exception
    {
        //
        // 테스트 안정화되면 oracle 이미지 파일과 대조 필요
        //
        RDR3dReader reader;
        reader = new RDR3dReader(SAMPLE_RDR_FILE.getAbsolutePath());
        String MD5_oracle = "d1d7bef590889c630d92fa990ad52e02";

        reader.regridAndSmoothAllLayers();

        int[] values = reader.getVerticalSlice(433, 295);

        RDR3dLayer layer;
        layer = new RDR3dLayer(100, 81);

        for (int x = 0; x < 100; x++)
        {
            layer.setVerticalValues(x, values);
        }

        File outputFile = new File(JUnitTestUtil.tempOutputDir("ext_onevalue.png"));
        layer.saveAs(outputFile);

        String hash = getMD5Hash(outputFile);
        Assert.assertTrue("extractCompleteVerticalTest() failed.", hash.equals(MD5_oracle));
    }

    /**
     * 임의의 두 지점으로부터 연직 단면을 이미지 파일로 표출
     * @throws Exception
     */
    @Test
    public void extractVerticalValuesFromTwoCoordsTest() throws Exception
    {
        //
        // 테스트 안정화되면 oracle 이미지 파일과 대조 필요
        //
        RDR3dReader reader;
        reader = new RDR3dReader(SAMPLE_RDR_FILE.getAbsolutePath());
        String MD5_oracle = "5a512230face33b9e42e538e2daea589";

        reader.regridAndSmoothAllLayers();

        //RDR3dLayer layer = reader.getVerticalSlice(new PointF(126.39332580566406, 39.071475982666016), new PointF(128.006769, 37.616370));
        //RDR3dLayer layer = reader.getVerticalSlice(new PointF(127.663300, 34.437866), new PointF(129.064072,32.597450), 250);

        RDR3dLayer layer = reader.getVerticalSlice(new PointF(125.796321, 39.043380), new PointF(127.653010, 39.883061), 250);
        boolean testResult = true;

        File outputFile = new File(JUnitTestUtil.tempOutputDir("ext_vvalue_twocoords.png"));
        layer.saveAs(outputFile);

        String hash = getMD5Hash(outputFile);
        testResult &= hash.equals(MD5_oracle);

        // 격자의 좌우 위치가 맞는지 테스트 할때 이 지점 사용하면 됨.
        MD5_oracle = "233740acb93f1761e6e3030dd0444333";
        layer = reader.getVerticalSlice(new PointF(124.220200,33.003147), new PointF(125.122055,33.753872), 250);
        outputFile = new File(JUnitTestUtil.tempOutputDir("ext_vvalue_twocoords_02.png"));

        layer.saveAs(outputFile);
        hash = getMD5Hash(outputFile);
        testResult &= hash.equals(MD5_oracle);

        Assert.assertTrue("extractCompleteVerticalTest() failed.", testResult);
    }

    /**
     * 셋 이상의 지점으로부터 연직 단면도 이미지 파일로 표출
     * @throws Exception
     */
    @Test
    public void extractCompleteVerticalTest() throws Exception
    {
        RDR3dReader reader;
        reader = new RDR3dReader(SAMPLE_RDR_FILE.getAbsolutePath());
        String MD5_oracle = "0db774816e2fb6a38ad9c927631c6b52";

        reader.regridAndSmoothAllLayers();

        //RDR3dLayer layer = reader.getVerticalSlice(new PointF[] { new PointF(125.771843, 39.078096), new PointF(127.618365, 39.990128), new PointF(126.524324, 33.479452) }, 250);
        RDR3dLayer layer = reader.getVerticalSlice(new PointF[] { new PointF(125.796321, 39.043380), new PointF(127.653010, 39.883061), new PointF(126.532405, 33.365726) }, 250);
        //RDR3dLayer layer = reader.getVerticalSlice(new PointF[] { new PointF(123.868576, 32.795738), new PointF(126.311607, 34.680325) }, 250);

        File outputFile = new File(JUnitTestUtil.tempOutputDir("ext_vvalue_threecoords.png"));
        layer.saveAs(outputFile);

        String hash = getMD5Hash(outputFile);
        Assert.assertTrue("extractCompleteVerticalTest() failed.", hash.equals(MD5_oracle));
    }


    //================================================
    //region 레이더 압축 데이터 추출 테스트
    //================================================
    /**
     * 레이더 (CNQCZ) 파일의 압축을 제대로 푸는지 테스트
     * @throws Exception
     */
    @Test
    public void extractTest() throws Exception
    {
        String MD5_oracle = "573607aba22af7780caaa90fbbbd7e2d";
        File sampleFile = SAMPLE_RDR_FILE;
        File tempFile = new File(JUnitTestUtil.tempOutputDir("rdrimgtest_temp_output.bin"));
        GZipTgzReader reader = new GZipTgzReader(sampleFile.getAbsolutePath());
        reader.extractGZipToFile(tempFile);
        String hash = getMD5Hash(tempFile);

        Assert.assertTrue("RDR gz extraction to file failed.", hash.equals(MD5_oracle));

        byte[] buffer = reader.readAllBytes();
        Files.write(tempFile.toPath(), buffer);

        hash = getMD5Hash(tempFile);
        Assert.assertTrue("RDR gz extractionextraction to memory (RAM) failed.", hash.equals(MD5_oracle));
    }

    /**
     * 레이더 (CNQCZ) 파일로부터 이미지 파일을 제대로 추출하는지 테스트
     * @throws Exception
     */
    @Test
    public void radarImageExtractionTest() throws Exception
    {
        File sampleFile = SAMPLE_RDR_FILE;
        File tempFile;
        String[] MD5_oracles =
                { "9b2c193e28f9f2dabb36cf5cada9335b",
                    "f06a43f05fdb4e50e39e734441f6fcf6",
                    "973fb01a18d2150f94ea279fc9f1506e",
                    "e6932c71067f4e40b53e166247d61e5f",
                    "98e374cac3619a390044f9cccfb4f7e9"
                }; // 5개의 output image file 에 대한 MD5 hash 값
        boolean testResult = true;

        RDR3dReader reader;
        reader = new RDR3dReader(sampleFile.getAbsolutePath());

        reader.regridAndSmoothAllLayers();

        for (int i = 0; i < 5; i++)
        {
            String hash;

            tempFile = new File(JUnitTestUtil.tempOutputDir(String.format("rdrimgtest_temp_output_%02d.png", i)));

            // 추출한 이미지를 임시 파일로 저장 후, MD5 해쉬값 추출
            reader.getLayer(i).saveAs(tempFile);
            hash = getMD5Hash(tempFile);
            testResult &= hash.equals(MD5_oracles[i]);
        }

        Assert.assertTrue("RDR image extraction test.", testResult);
    }

    /**
     * 특정 파일로부터 MD5 해쉬값 추출
     * @param file 해쉬값 추출할 파일
     * @return
     * @throws IOException
     * @throws NoSuchAlgorithmException
     */
    private String getMD5Hash(File file) throws IOException, NoSuchAlgorithmException
    {
        MessageDigest md = MessageDigest.getInstance("MD5");

        try (FileInputStream fis = new FileInputStream(file))
        {
            byte[] dataBytes = new byte[1024];

            int nread = 0;

            while ((nread = fis.read(dataBytes)) != -1)
            {
                md.update(dataBytes, 0, nread);
            }

            byte[] mdbytes = md.digest();

            // convert the byte to hex format
            StringBuffer sb = new StringBuffer();

            for (int i = 0; i < mdbytes.length; i++)
            {
                sb.append(Integer.toString((mdbytes[i] & 0xff) + 0x100, 16).substring(1));
            }

            return sb.toString();
        }
    }
    //endregion
    //================================================
}