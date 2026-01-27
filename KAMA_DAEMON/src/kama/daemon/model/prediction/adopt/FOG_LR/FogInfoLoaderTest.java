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
 * Created on 2017-01-23.
 */
public class FogInfoLoaderTest
{
    /**
     * LFOG / RFOG 데이터 파싱 테스트 (예외 호출되는지 확인)
     * @throws Exception
     */
    @Test
    public void parseFogdataTest() throws Exception
    {
        String filePath;
        File file;
        ProcessorInfo processorInfo;
        FogInfoLoader loader;
        List<FogInfoLoader.FogInfo> fogInfoList;

        // 샘플 데이터
        filePath = JUnitTestUtil.resourceTestDir("lfog_vis_201611051200.asc");

        // 데이터 프로세서 정보 간단하게 만들기 (테스트에 필요)
        file = new File(filePath);
        processorInfo = retrieveSimplifiedDataProcessor(file);

        // 안개 정보 받아오기
        loader = new FogInfoLoader(file, processorInfo.FileDateFromNameUTC, processorInfo.FileDateFromNameKST);
        fogInfoList = loader.retrieveFogInfoList();

        for (FogInfoLoader.FogInfo info : fogInfoList)
        {
            // 시정값 유효여부 판별
            Assert.assertTrue("Validate station ID", info.StationID >= 0);
            Assert.assertTrue("Validate visibility value", info.Visibility >= 0);
        }
    }

    private ProcessorInfo retrieveSimplifiedDataProcessor(File file) throws ParseException
    {
        String[] tok;
        Date fileDate;
        ProcessorInfo processorInfo;

        // Parse date
        tok = file.getName().split("\\.")[0].split("_");
        fileDate = DateFormatter.parseDate(tok[2], "yyyyMMddHH");

        // create data processor info
        processorInfo = new ProcessorInfo();
        processorInfo.FileDateFromNameUTC = (Date)fileDate.clone();
        processorInfo.FileDateFromNameKST = DateUtils.addHours((Date)fileDate.clone(), 9);

        return processorInfo;
    }
}