package kama.daemon.model.observation.adopt;

import kama.daemon.common.util.DaemonUtils;
import kama.daemon.common.util.DateFormatter;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author chlee
 * Created on 2016-11-28.
 */
public class AMDAR_DataConv
{
    private static final String[] _prevColOrder = { "FLIGHT_ID", "LATITUDE", "LONGITUDE", "ALTITUDE", "TEMP", "WD", "WSPD", "TM", "FLY_STAT", "NUM", "S_AIRPORT", "D_AIRPORT" };
    private static final String[] _newColOrder = { "TM", "LATITUDE", "LONGITUDE", "ALTITUDE", "FLIGHT_ID", "TEMP", "WD", "WSPD", "S_AIRPORT", "D_AIRPORT", "FLY_STAT" };

    public AMDAR_DataConv()
    {
    }

    private Object[] _convertRecordWithNewOrder(Object[] bindArray)
    {
        // #Bind Info
        // bindArray[0] = STN_ID => FLIGHT_ID
        // bindArray[1] = LAT => LATITUDE
        // bindArray[2] = LON => LONGITUDE
        // bindArray[3] = HT => ALTITUDE
        // bindArray[4] = TEMP => TEMP
        // bindArray[5] = WD => WD
        // bindArray[6] = WS => WSPD
        // bindArray[7] = UTC => TM
        // bindArray[8] = FLY => FLY_STAT
        // bindArray[9] = NUM => N/A
        // bindArray[10] = AIR_S => S_AIRPORT
        // bindArray[11] = AIR_E => D_AIRPORT
        // bindArray[12] = STN_NUM => N/A

        HashMap<String, String> dictRecord;
        Object[] objNewArray;

        dictRecord = new HashMap<String, String>();
        objNewArray = new Object[_newColOrder.length];

        /*if (_prevColOrder.length > bindArray.length)
        {

        }*/

        // Insert in previous order
        for (int i = 0; i < _prevColOrder.length; i++)
        {
            dictRecord.put(_prevColOrder[i], bindArray[i].toString());
        }

        // Pull out with new order
        for (int i = 0; i < _newColOrder.length; i++)
        {
            objNewArray[i] = dictRecord.get(_newColOrder[i]);
        }

        return objNewArray;
    }

    // Convert resource text to buffered reader (for compatibility)
    public List<Object[]> retrieveRecords(String resText, Date fileTime) throws IOException, ArrayIndexOutOfBoundsException
    {
        try (InputStream is = new ByteArrayInputStream(resText.getBytes());
                BufferedReader br = new BufferedReader(new InputStreamReader(is)))
        {
            return _readPreviousData(br, fileTime, false);
        }
    }

    public List<Object[]> retrieveRecords(BufferedReader br, Date fileTime) throws IOException, ArrayIndexOutOfBoundsException
    {
        return _readPreviousData(br, fileTime, false);
    }

    public List<Object[]> retrieveOldRecords(BufferedReader br, Date fileTime) throws IOException, ArrayIndexOutOfBoundsException
    {
        return _readPreviousData(br, fileTime, true);
    }

    private List<Object[]> _readPreviousData(BufferedReader br, Date fileTime, boolean isOldVersion) throws IOException, ArrayIndexOutOfBoundsException
    {
        List<Object[]> arrayOfRecords;
        Object[] bindArray;
        SimpleDateFormat sdf2;
        String sTimeStamp;

        bindArray = new Object[13];
        sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        arrayOfRecords = new ArrayList<Object[]>();
        sTimeStamp = sdf2.format(fileTime);

        LineNumberReader rdr = new LineNumberReader(br);

        String[] strLines = new String[2];

        for (String line; (line = rdr.readLine()) != null; )
        {
            if (rdr.getLineNumber() == 5)
            {
                strLines[0] = line;
            }

            if (rdr.getLineNumber() == 7)
            {
                strLines[1] = line;
            }
        }

        int num = 0;

        bindArray[0] = strLines[0].split("/")[1].substring(3, 9);
        bindArray[12] = bindArray[0] + "_" + sTimeStamp;

        String[] strValues = strLines[1].replace("    ", ",").split(",");

        if ("A".equals(strValues[0].substring(5, 6)) && "O".equals(strLines[0].substring(3, 4)))
        {
            bindArray[1] = strValues[0].substring(21, 23) + "." + strValues[0].substring(23, 26);
            bindArray[2] = strValues[0].substring(27, 30) + "." + strValues[0].substring(30, 33);
            bindArray[3] = strValues[1].substring(0, 4);
            bindArray[3] = DaemonUtils.isNumber((String) bindArray[3]) ? Float.valueOf((String) bindArray[3]).toString() : "0";

            if ("P".equals(strValues[1].substring(4, 5)))
            {
                bindArray[4] = Float.valueOf(strValues[1].substring(5, 8)).toString();
            }
            else
            {
                bindArray[4] = "-" + Float.valueOf(strValues[1].substring(5, 8)).toString();
            }

            bindArray[5] = Float.valueOf(strValues[1].substring(8, 11)).toString();
            bindArray[6] = Float.valueOf(strValues[1].substring(11, 14)).toString();
            //bindArray[7] = sTimeStamp.substring(0, 7) + "-" + strValues[0].substring(6, 8) + " " + strValues[0].substring(29, 31) + ":" + strValues[0].substring(31, 33) + ":00";
            bindArray[7] = getBindArray7AfterCorrectingFollowingMonthIssue(fileTime, sTimeStamp, strValues, sdf2, "AAMDAR");
            bindArray[7] = DaemonUtils.addSeconds(sdf2, (String) bindArray[7], 6);
            bindArray[8] = strValues[0].substring(5, 6);
            bindArray[9] = String.valueOf(num++);
            bindArray[10] = strValues[0].substring(12, 16);
            bindArray[11] = strValues[0].substring(16, 20);

            // ?�로??DB (AMIS)??경우
            if (!isOldVersion)
            {
                // LON, LAT 값이 100보다 ?�을 경우 (* 10000) 처리
                correctBindArrayLonLat(bindArray);

                // ?�코???�정????추�?(?�로???�이블에 맞게)
                arrayOfRecords.add(_convertRecordWithNewOrder(bindArray));
            }
            else
            {
                // AMDAR ?�이�??�력
                arrayOfRecords.add(bindArray.clone());
            }

            //----------------------------------------------------------------------------------------------------------------//

            bindArray[3] = strValues[2].substring(0, 4);
            bindArray[3] = DaemonUtils.isNumber((String) bindArray[3]) ? Float.valueOf((String) bindArray[3]).toString() : "0";
            bindArray[4] = Float.valueOf(strValues[2].substring(5, 8)).toString();
            bindArray[5] = Float.valueOf(strValues[2].substring(8, 11)).toString();
            bindArray[6] = Float.valueOf(strValues[2].substring(11, 14)).toString();
            bindArray[7] = DaemonUtils.addSeconds(sdf2, (String) bindArray[7], 6);
            bindArray[9] = String.valueOf(num++);

            // ?�로??DB (AMIS)??경우
            if (!isOldVersion)
            {
                // LON, LAT 값이 100보다 ?�을 경우 (* 10000) 처리
                correctBindArrayLonLat(bindArray);

                // ?�코???�정????추�?(?�로???�이블에 맞게)
                arrayOfRecords.add(_convertRecordWithNewOrder(bindArray));
            }
            else
            {
                // AMDAR ?�이�??�력
                arrayOfRecords.add(bindArray.clone());
            }

            //----------------------------------------------------------------------------------------------------------------//

            bindArray[1] = strValues[3].substring(2, 4) + "." + strValues[3].substring(4, 7);
            bindArray[2] = strValues[3].substring(8, 11) + "." + strValues[3].substring(11, 14);
            bindArray[3] = strValues[3].substring(14, 18);
            bindArray[3] = DaemonUtils.isNumber((String) bindArray[3]) ? Float.valueOf((String) bindArray[3]).toString() : "0";

            if ("P".equals(strValues[3].substring(18, 19)))
            {
                bindArray[4] = Float.valueOf(strValues[3].substring(19, 22)).toString();
            }
            else
            {
                bindArray[4] = "-" + Float.valueOf(strValues[3].substring(19, 22)).toString();
            }

            bindArray[5] = Float.valueOf(strValues[3].substring(22, 25)).toString();
            bindArray[6] = Float.valueOf(strValues[3].substring(25, 28)).toString();
            bindArray[7] = DaemonUtils.addSeconds(sdf2, (String) bindArray[7], 30);
            bindArray[9] = String.valueOf(num++);

            // ?�로??DB (AMIS)??경우
            if (!isOldVersion)
            {
                // LON, LAT 값이 100보다 ?�을 경우 (* 10000) 처리
                correctBindArrayLonLat(bindArray);

                // ?�코???�정????추�?(?�로???�이블에 맞게)
                arrayOfRecords.add(_convertRecordWithNewOrder(bindArray));
            }
            else
            {
                // AMDAR ?�이�??�력
                arrayOfRecords.add(bindArray.clone());
            }

            //----------------------------------------------------------------------------------------------------------------//

            for (int i = 4; i < strValues.length - 1; i++)
            {
                bindArray[1] = strValues[i].substring(1, 3) + "." + strValues[i].substring(3, 6);
                bindArray[2] = strValues[i].substring(7, 10) + "." + strValues[i].substring(10, 13);
                bindArray[3] = strValues[i].substring(13, 17);
                bindArray[3] = DaemonUtils.isNumber((String) bindArray[3]) ? Float.valueOf((String) bindArray[3]).toString() : "0";

                if ("P".equals(strValues[i].substring(17, 18)))
                {
                    bindArray[4] = Float.valueOf(strValues[i].substring(18, 21)).toString();
                }
                else
                {
                    bindArray[4] = "-" + Float.valueOf(strValues[i].substring(18, 21)).toString();
                }

                bindArray[5] = Float.valueOf(strValues[i].substring(21, 24)).toString();
                bindArray[6] = Float.valueOf(strValues[i].substring(24, 27)).toString();
                bindArray[7] = DaemonUtils.addSeconds(sdf2, (String) bindArray[7], 30);
                bindArray[9] = String.valueOf(num++);

                // ?�로??DB (AMIS)??경우
                if (!isOldVersion)
                {
                    // LON, LAT 값이 100보다 ?�을 경우 (* 10000) 처리
                    correctBindArrayLonLat(bindArray);

                    // ?�코???�정????추�?(?�로???�이블에 맞게)
                    arrayOfRecords.add(_convertRecordWithNewOrder(bindArray));
                }
                else
                {
                    // AMDAR ?�이�??�력
                    arrayOfRecords.add(bindArray.clone());
                }
            }
        }
        else if ("A".equals(strValues[0].substring(5, 6)) && "K".equals(strLines[0].substring(3, 4)))
        {
            bindArray[1] = strValues[0].substring(21, 23) + "." + strValues[0].substring(23, 26);
            bindArray[2] = strValues[0].substring(27, 30) + "." + strValues[0].substring(30, 33);
            bindArray[3] = strValues[1].substring(0, 4);
            bindArray[3] = DaemonUtils.isNumber((String) bindArray[3]) ? Float.valueOf((String) bindArray[3]).toString() : "0";

            if ("P".equals(strValues[15].substring(4, 5)))
            {
                bindArray[4] = Float.valueOf(strValues[15].substring(5, 8)).toString();
            }
            else
            {
                bindArray[4] = "-" + Float.valueOf(strValues[15].substring(5, 8)).toString();
            }

            bindArray[5] = Float.valueOf(strValues[1].substring(8, 11)).toString();
            bindArray[6] = Float.valueOf(strValues[1].substring(11, 14)).toString();
            //bindArray[7] = sTimeStamp.substring(0, 7) + "-" + strValues[0].substring(33, 35) + " " + strValues[0].substring(35, 37) + ":" + strValues[0].substring(37, 39) + ":00";
            bindArray[7] = getBindArray7AfterCorrectingFollowingMonthIssue(fileTime, sTimeStamp, strValues, sdf2, "KAMDAR");
            bindArray[7] = DaemonUtils.addSeconds(sdf2, (String) bindArray[7], 6);
            bindArray[8] = strValues[0].substring(5, 6);
            bindArray[9] = String.valueOf(num++);
            bindArray[10] = strValues[0].substring(12, 16);
            bindArray[11] = strValues[0].substring(16, 20);

            // ?�로??DB (AMIS)??경우
            if (!isOldVersion)
            {
                // LON, LAT 값이 100보다 ?�을 경우 (* 10000) 처리
                correctBindArrayLonLat(bindArray);

                // ?�코???�정????추�?(?�로???�이블에 맞게)
                arrayOfRecords.add(_convertRecordWithNewOrder(bindArray));
            }
            else
            {
                // AMDAR ?�이�??�력
                arrayOfRecords.add(bindArray.clone());
            }

            //----------------------------------------------------------------------------------------------------------------//

            bindArray[3] = strValues[15].substring(0, 4);
            bindArray[3] = DaemonUtils.isNumber((String) bindArray[3]) ? Float.valueOf((String) bindArray[3]).toString() : "0";

            if ("P".equals(strValues[15].substring(4, 5)))
            {
                bindArray[4] = Float.valueOf(strValues[15].substring(5, 8)).toString();
            }
            else
            {
                bindArray[4] = "-" + Float.valueOf(strValues[15].substring(5, 8)).toString();
            }

            bindArray[5] = Float.valueOf(strValues[15].substring(8, 11)).toString();
            bindArray[6] = Float.valueOf(strValues[15].substring(11, 14)).toString();
            bindArray[7] = DaemonUtils.addSeconds(sdf2, (String) bindArray[7], 6);
            bindArray[9] = String.valueOf(num++);

            // ?�로??DB (AMIS)??경우
            if (!isOldVersion)
            {
                // LON, LAT 값이 100보다 ?�을 경우 (* 10000) 처리
                correctBindArrayLonLat(bindArray);

                // ?�코???�정????추�?(?�로???�이블에 맞게)
                arrayOfRecords.add(_convertRecordWithNewOrder(bindArray));
            }
            else
            {
                // AMDAR ?�이�??�력
                arrayOfRecords.add(bindArray.clone());
            }

            //----------------------------------------------------------------------------------------------------------------//

            bindArray[1] = strValues[16].substring(2, 4) + "." + strValues[16].substring(4, 7);
            bindArray[2] = strValues[16].substring(8, 11) + "." + strValues[16].substring(11, 14);
            bindArray[3] = strValues[16].substring(14, 18);
            bindArray[3] = DaemonUtils.isNumber((String) bindArray[3]) ? Float.valueOf((String) bindArray[3]).toString() : "0";

            if ("P".equals(strValues[16].substring(18, 19)))
            {
                bindArray[4] = Float.valueOf(strValues[16].substring(19, 22)).toString();
            }
            else
            {
                bindArray[4] = "-" + Float.valueOf(strValues[16].substring(19, 22)).toString();
            }

            bindArray[5] = Float.valueOf(strValues[16].substring(22, 25)).toString();
            bindArray[6] = Float.valueOf(strValues[16].substring(25, 28)).toString();
            bindArray[7] = DaemonUtils.addSeconds(sdf2, (String) bindArray[7], 30);
            bindArray[9] = String.valueOf(num++);

            // ?�로??DB (AMIS)??경우
            if (!isOldVersion)
            {
                // LON, LAT 값이 100보다 ?�을 경우 (* 10000) 처리
                correctBindArrayLonLat(bindArray);

                // ?�코???�정????추�?(?�로???�이블에 맞게)
                arrayOfRecords.add(_convertRecordWithNewOrder(bindArray));
            }
            else
            {
                // AMDAR ?�이�??�력
                arrayOfRecords.add(bindArray.clone());
            }

            //----------------------------------------------------------------------------------------------------------------//

            for (int i = 17; i < strValues.length - 1; i++)
            {
                bindArray[1] = strValues[i].substring(1, 3) + "." + strValues[i].substring(3, 6);
                bindArray[2] = strValues[i].substring(7, 10) + "." + strValues[i].substring(10, 13);
                bindArray[3] = strValues[i].substring(13, 17);
                bindArray[3] = DaemonUtils.isNumber((String) bindArray[3]) ? Float.valueOf((String) bindArray[3]).toString() : "0";

                if ("P".equals(strValues[i].substring(17, 18)))
                {
                    bindArray[4] = Float.valueOf(strValues[i].substring(18, 21)).toString();
                }
                else
                {
                    bindArray[4] = "-" + Float.valueOf(strValues[i].substring(18, 21)).toString();
                }

                bindArray[5] = Float.valueOf(strValues[i].substring(21, 24)).toString();
                bindArray[6] = Float.valueOf(strValues[i].substring(24, 27)).toString();
                bindArray[7] = DaemonUtils.addSeconds(sdf2, (String) bindArray[7], 30);
                bindArray[9] = String.valueOf(num++);

                // ?�로??DB (AMIS)??경우
                if (!isOldVersion)
                {
                    // LON, LAT 값이 100보다 ?�을 경우 (* 10000) 처리
                    correctBindArrayLonLat(bindArray);

                    // ?�코???�정????추�?(?�로???�이블에 맞게)
                    arrayOfRecords.add(_convertRecordWithNewOrder(bindArray));
                }
                else
                {
                    // AMDAR ?�이�??�력
                    arrayOfRecords.add(bindArray.clone());
                }
            }

        }
        else
        {
            bindArray[1] = strValues[0].substring(17, 19) + "." + strValues[0].substring(19, 22);
            bindArray[2] = strValues[0].substring(23, 26) + "." + strValues[0].substring(26, 29);
            bindArray[3] = strValues[0].substring(33, 37);
            bindArray[3] = DaemonUtils.isNumber((String) bindArray[3]) ? Float.valueOf((String) bindArray[3]).toString() : "0";

            if ("P".equals(strValues[0].substring(37, 38)))
            {
                bindArray[4] = Float.valueOf(strValues[0].substring(38, 41)).toString();
            }
            else
            {
                bindArray[4] = "-" + Float.valueOf(strValues[0].substring(38, 41)).toString();
            }

            bindArray[5] = Float.valueOf(strValues[0].substring(41, 44)).toString();
            bindArray[6] = Float.valueOf(strValues[0].substring(44, 47)).toString();
            bindArray[7] = getBindArray7AfterCorrectingFollowingMonthIssue(fileTime, sTimeStamp, strValues, sdf2, "AAMDAR");
            bindArray[8] = strValues[0].substring(5, 6);
            bindArray[9] = String.valueOf(num++);
            bindArray[10] = strValues[0].substring(8, 12);
            bindArray[11] = strValues[0].substring(12, 16);

            // ?�로??DB (AMIS)??경우
            if (!isOldVersion)
            {
                // LON, LAT 값이 100보다 ?�을 경우 (* 10000) 처리
                correctBindArrayLonLat(bindArray);

                // ?�코???�정????추�?(?�로???�이블에 맞게)
                arrayOfRecords.add(_convertRecordWithNewOrder(bindArray));
            }
            else
            {
                // AMDAR ?�이�??�력
                arrayOfRecords.add(bindArray.clone());
            }

            for (int i = 1; i < strValues.length - 1; i++)
            {

                bindArray[1] = strValues[i].substring(2, 4) + "." + strValues[i].substring(4, 7);
                bindArray[2] = strValues[i].substring(8, 11) + "." + strValues[i].substring(11, 14);
                bindArray[3] = strValues[i].substring(18, 22);
                bindArray[3] = DaemonUtils.isNumber((String) bindArray[3]) ? Float.valueOf((String) bindArray[3]).toString() : "0";

                if ("P".equals(strValues[i].substring(22, 23)))
                {
                    bindArray[4] = Float.valueOf(strValues[i].substring(23, 26)).toString();
                }
                else
                {
                    bindArray[4] = "-" + Float.valueOf(strValues[i].substring(23, 26)).toString();
                }

                bindArray[5] = Float.valueOf(strValues[i].substring(26, 29)).toString();
                bindArray[6] = Float.valueOf(strValues[i].substring(29, 32)).toString();
                //bindArray[7] = sTimeStamp.substring(0, 7) + "-" + strValues[0].substring(6, 8) + " " + strValues[i].substring(14, 16) + ":" + strValues[i].substring(16, 18) + ":00";
                bindArray[7] = getBindArray7AfterCorrectingFollowingMonthIssue(fileTime, sTimeStamp, strValues, sdf2, "KAMDAR");
                bindArray[9] = String.valueOf(num++);

                // ?�로??DB (AMIS)??경우
                if (!isOldVersion)
                {
                    // LON, LAT 값이 100보다 ?�을 경우 (* 10000) 처리
                    correctBindArrayLonLat(bindArray);

                    // ?�코???�정????추�?(?�로???�이블에 맞게)
                    arrayOfRecords.add(_convertRecordWithNewOrder(bindArray));
                }
                else
                {
                    // AMDAR ?�이�??�력
                    arrayOfRecords.add(bindArray.clone());
                }
            }
        }

        return arrayOfRecords;
    }

    /**
     * 2017/09/12: ?�일 기록?�짜가 ?�음 ?�이�? ?�코???�짜가 ?�전 ?�의 말일????발생?�는 문제?�에 ?�???�결�?
     * @param fileTime ?�일명에 기록???�짜�?(e.g., AAMDAR_201609011111HL7738_1.txt => 2016??9??1??....)
     * @param timeStamp ?�일명으로�???받아??timestamp
     * @param strValues AAMDAR ?�코?��? split ??array? (?�실�??�음)
     * @param sdf2 timestamp ??date format
     * @return
     */
    private String getBindArray7AfterCorrectingFollowingMonthIssue(Date fileTime, String timeStamp, String[] strValues, SimpleDateFormat sdf2, String amdarType)
    {
        String bindArray7Value;
        int[] idxs;

        if (amdarType.equals("AAMDAR"))
        {
            idxs = new int[] { 0, 7, 33, 35, 35, 37, 37, 39 };
        }
        else if (amdarType.equals("KAMDAR"))
        {
        	if(!("A").equals(strValues[0].substring(5, 6))) {
				idxs = new int[] { 0, 7, 6, 8, 14, 16, 16, 18 };
        	} else {
        		idxs = new int[] { 0, 7, 6, 8, 29, 31, 31, 33};
        	}
        }
        else
        {
            throw new RuntimeException("Invalid AMDAR type.");
        }

        // 2017/09/12: ?�일 기록?�짜가 ?�음 ?�이�? ?�코???�짜가 ?�전 ?�의 말일????발생?�는 문제???�결
        if (Integer.parseInt(timeStamp.substring(8, 10)) < Integer.parseInt(strValues[0].substring(33, 35)))
        {
            Date tempFileTime = DateFormatter.subtractDays((Date)fileTime.clone(), 1);
            String sTimeStampTemp = sdf2.format(tempFileTime);

            bindArray7Value = sTimeStampTemp.substring(idxs[0], idxs[1]) + "-" + strValues[0].substring(idxs[2], idxs[3]) + " " + strValues[0].substring(idxs[4], idxs[5]) + ":" + strValues[0].substring(idxs[6], idxs[7]) + ":00";
        }
        else
        {
            bindArray7Value = timeStamp.substring(idxs[0], idxs[1]) + "-" + strValues[0].substring(idxs[2], idxs[3]) + " " + strValues[0].substring(idxs[4], idxs[5]) + ":" + strValues[0].substring(idxs[6], idxs[7]) + ":00";
        }

        return bindArray7Value;
    }

    private void correctBindArrayLonLat(Object[] bindArray)
    {
        // 2016.12.20 ?�정?�항
        // LON, LAT 값이 100보다 ?�을 경우 (* 10000) 처리
        bindArray[1] = _repairLonLatValue(bindArray[1]);
        bindArray[2] = _repairLonLatValue(bindArray[2]);
    }

    // 2016.12.20 ?�정?�항
    // LON, LAT 값이 100보다 ?�을 경우 (* 10000) 처리
    // (?�이??문제�??�해 추�????�수)
    private Object _repairLonLatValue(Object floatObjectLonLat)
    {
        float lonLat;

        lonLat = Float.parseFloat((String)floatObjectLonLat);

        if (lonLat < 361)
        {
            return new Integer((int)(lonLat * 10000)).toString();
        }

        return lonLat;
    }
}