package kama.daemon.model.prediction.adopt.FOG_LR;

import kama.daemon.common.db.struct.ProcessorInfo;
import kama.daemon.common.util.DateFormatter;
import kama.daemon.common.util.JUnitTestUtil;
import org.apache.commons.lang3.time.DateUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.text.ParseException;
import java.util.Date;
import java.util.List;

/**
 * @author chlee
 * Created on 2017-01-25.
 */
public class HeightTempLoaderTest
{
    @Test
    public void retriveDataTest() throws Exception
    {
        File testFile = new File(JUnitTestUtil.resourceTestDir("lfog_out.0090.2017011900.bin"));
        ProcessorInfo processorInfo = retrieveSimpleProcessorInfo(testFile);
        int stationID = Integer.parseInt(testFile.getName().split("\\.")[1]);
        HeightTempLoader loader = new HeightTempLoader(testFile, processorInfo.FileDateFromNameUTC, processorInfo.FileDateFromNameKST, stationID);
        List<HeightTempLoader.HeightTempInfo> dataList = loader.retrieveData();

        for (HeightTempLoader.HeightTempInfo info : dataList)
        {
            // 받아온 값들이 유효한 값인지 테스트
            Assert.assertTrue("Validate station ID", info.station_id > 0);
            Assert.assertTrue("Validate temperature", info.temperature > -100 && info.temperature < 100);
            Assert.assertTrue("Validate dew point", info.dew_point > -100 && info.dew_point < 100);
            Assert.assertTrue("Validate relative humidity", info.rel_humidity <= 100 && info.rel_humidity >= 0);
            Assert.assertTrue("Validate pressure", info.pressure >= 0);
            Assert.assertTrue("Validate floor", info.floor >= 0);
        }
    }

    private ProcessorInfo retrieveSimpleProcessorInfo(File file) throws ParseException
    {
        String fileName;
        String[] token;

        fileName = file.getName();
        token = fileName.split("\\.");

        ProcessorInfo processorInfo;
        processorInfo = new ProcessorInfo();

        processorInfo.FileDateFromNameUTC = DateFormatter.parseDate(token[2], "yyyyMMddHH");
        processorInfo.FileDateFromNameKST = DateUtils.addHours((Date)processorInfo.FileDateFromNameUTC.clone(), 9);

        return processorInfo;
    }
}