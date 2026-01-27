package kama.daemon.model.prediction.adopt.FOG_LR;

import kama.daemon.common.db.struct.ProcessorInfo;
import org.apache.commons.lang3.time.DateUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author chlee
 * Created on 2017-01-25.
 * 지역특화: 안개가이던스 (LFOG / RFOG) 에서 데이터 파싱 클래스
 */
public class FogInfoLoader
{
    private List<FogInfo> _fogInfoList;
    private Date _timeUTC, _timeKST;

    public class FogInfo implements Cloneable
    {
        public int StationID;
        public Date PredictTime;
        public Date ModelProducedTime;
        public double Visibility; // 48시간 예측

        @Override
        public FogInfo clone()
        {
            FogInfo info;

            info = new FogInfo();

            info.StationID = StationID;
            info.PredictTime = (Date)PredictTime.clone();
            info.ModelProducedTime = (Date)ModelProducedTime.clone();
            info.Visibility = Visibility;

            return info;
        }
    }

    public FogInfoLoader(File file, Date timeUTC, Date timeKST) throws IOException
    {
        _timeUTC = timeUTC;
        _timeKST = timeKST;

        try (BufferedReader br = new BufferedReader(new FileReader(file)))
        {
            // 데이터 읽어오기
            _fogInfoList = parseFogdata(br);
        }
    }

    public List<FogInfo> retrieveFogInfoList()
    {
        return _fogInfoList;
    }

    /**
     * LFOG / RFOG 데이터 파싱
     * @param br
     * @return
     * @throws IOException
     */
    private List<FogInfo> parseFogdata(BufferedReader br) throws IOException
    {
        String line;
        String[] token;
        String header;
        String[] tempToken;

        int floor, ceil; // 시간 시작, 끝값

        List<Integer> stn_ids = Arrays.asList(new Integer[] { 92, 110, 113, 151, 163, 167, 153, 182 });
        List<FogInfo> fogInfoList = new ArrayList<>();

        // 첫줄 header 생략.
        header = br.readLine();
        header = header.split(Pattern.quote("OBSTYPE#STNID#VISIBILITY[km]("))[1];
        tempToken = header.split(Pattern.quote("-+"));

        floor = Integer.parseInt(tempToken[0]);
        ceil = Integer.parseInt(tempToken[1].split(Pattern.quote("hr)#="))[0]);

        while ((line = br.readLine()) != null)
        {
            FogInfo fogInfo;
            int stnID;

            // token: #
            token = line.split("#");

            stnID = Integer.parseInt(token[1]);

            // 정의된 공항 지점만 추출
            if (stn_ids.contains(stnID))
            {
                int index = 0;

                // 시정값 추출 (n 시간, header 에 정의되어 있음.)
                for (int i = floor; i <= ceil; i++)
                {
                    fogInfo = new FogInfo();
                    fogInfo.StationID = stnID;
                    fogInfo.PredictTime = DateUtils.addHours(_timeKST, i);
                    fogInfo.ModelProducedTime = _timeUTC;
                    fogInfo.Visibility = Double.parseDouble(token[index + 2]);
                    index++;

                    fogInfoList.add(fogInfo);
                }
            }
        }

        return fogInfoList;
    }
}
