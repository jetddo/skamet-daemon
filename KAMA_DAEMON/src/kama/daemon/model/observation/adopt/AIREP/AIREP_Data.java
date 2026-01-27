package kama.daemon.model.observation.adopt.AIREP;

import kama.daemon.common.util.DateFormatter;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.text.ParseException;
import java.util.Date;

/**
 * @author chlee
 * Created on 2017-02-02.
 */
public class AIREP_Data
{
    // Memo: DB 테이블 구성이 전부 VARCHAR2로 되어 있어 숫자형 데이터도 전부 String 으로 처리하였음.

    public Date InputTime; // 입력 시각
    public String StationCode; // 발표 공항
    public Date InputSystemTime; // 시스템 시각
    public String InputName; // 입력자
    public String InputIP; // 입력 IP
    public String AircraftType; // 항공기 식별부호
    public String Latitude; // 위도
    public String Longitude; // 경도
    public Date ObservedTime; // 관측시각
    public String FlightLevel; // 비행고도
    public String Temperature; // 기온
    public String WindDirection; // 풍향
    public String WindSpeed; // 풍속
    public String Turbulance; // 난류
    public String ACFT_ICING_INTST; // 항공기착빙 (뭔지모름..)
    public String Humidity; // 습도
    public String RMK; // RMK (뭔지모름..)
    public String MessageText; // 원문

    public static AIREP_Data loadAIREPData(File file) throws IOException, ParseException
    {
        AIREP_Data data;
        String text = new String(Files.readAllBytes(file.toPath()));
        String[] token;
        int index = 0;

        token = text.split("#");

        data = new AIREP_Data();
        data.InputTime = DateFormatter.parseDate(token[index++], "yyyyMMddHHmmss");
        data.StationCode = token[index++];

        if (token[index] != null)
        {
            data.InputSystemTime = DateFormatter.parseDate(token[index], "yyyyMMddHHmm");
        }
        else
        {
            data.InputSystemTime = null;
        }

        index++;

        data.InputName = token[index++];
        data.InputIP = token[index++];
        data.AircraftType = token[index++];
        data.Latitude = token[index++];
        data.Longitude = token[index++];

        if (token[index] != null)
        {
            data.ObservedTime = DateFormatter.parseDate(token[index], "yyyy.MM.dd HH:mm");
        }
        else
        {
            data.ObservedTime = null;
        }

        index++;

        data.FlightLevel = token[index++];
        data.Temperature = token[index++];
        data.WindDirection = token[index++];
        data.WindSpeed = token[index++];
        data.Turbulance = token[index++];
        data.ACFT_ICING_INTST = token[index++];
        data.Humidity = token[index++];
        data.RMK = token[index++];
        data.MessageText = token[index++];

        return data;
    }
}













