package kama.daemon.model.observation.adopt.UPP;

import java.util.Date;

/**
 * @author chlee
 * Created on 2017-01-10.
 */
public class UPPData
{
    public String StationID; // 지점번호
    public Date RecordTime;
    public int PA; // 현기기압
    public int GH; // 고도
    public double RH; // 습도
    public int TA; // 기온
    public int TD; // 이슬점 온도
    public int WD; // 풍향
    public int WS; // 풍속
    public double LAT; // 위도
    public double LON; // 경도
}