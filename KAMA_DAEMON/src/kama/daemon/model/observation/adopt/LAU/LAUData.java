package kama.daemon.model.observation.adopt.LAU;

import java.util.List;

/**
 * @author chlee
 * Created on 2016-12-05.
 */
public class LAUData
{
    public LAUHeader Header;
    public int ProtocolYear;
    public int ProtocolMonth;
    public int ProtocolDay;
    public int AWSYear;
    public int AWSMonth;
    public int AWSDay;
    public int AWSHour;
    public int AWSMinute;
    public int DataType;
    public int DataTypeIndex;
    public int AWSID;
    public List<Integer> FooterData;
    public int FooterV1;
    public int FooterV2;
}