package kama.daemon.model.observation.proc;

import kama.daemon.common.db.DataProcessor;
import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.db.struct.ProcessorInfo;
import kama.daemon.common.util.*;
import kama.daemon.model.observation.adopt.UPP.UPPData;
import org.apache.commons.lang3.time.DateUtils;

import java.io.*;
import java.nio.file.Files;
import java.text.MessageFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

/**
 * @author chlee
 * Created on 2016-11-28.
 */
public class UPP_DataProcess extends DataProcessor
{
    private static final String DATAFILE_PREFIX = "upp";
    private static final int DB_COLUMN_COUNT = 10;
    private static final int FILE_DATE_INDEX_POS = 3; // UPP_RAW_47186_20160902000000
    private static final int[] DB_PRIMARY_KEY_INDEXES = {0, 1}; // TM, STN_ID
    private final int INSERT_QUERY = 1;
    private final int DELETE_QUERY = 3;
    private final int INSERT_QUERY_OLD_1 = 4;
    private final int INSERT_QUERY_OLD_2 = 5;
    private final int DELETE_QUERY_OLD = 6;

    public UPP_DataProcess(DaemonSettings settings)
    {
        super(settings, DATAFILE_PREFIX);
    }

    @Override
    protected void defineQueries()
    {
        defineQueryFormat(INSERT_QUERY, "INSERT INTO %%WORKSPACE%%.KMA_UPP(TM, STN_ID, PA, GH, TA, TD, WD, WS, LAT, LON) VALUES  (TO_DATE(''{0}'',''YYYY-MM-DD HH24:mi:ss''), ''{1}'', ''{2}'', ''{3}'', ''{4}'', ''{5}'', ''{6}'', ''{7}'', ''{8}'', ''{9}'')");
        defineQueryFormat(DELETE_QUERY, "DELETE FROM %%WORKSPACE%%.KMA_UPP WHERE TM = TO_DATE(''{0}'', ''YYYY-MM-DD HH24:mi:ss'') AND STN_ID = ''{1}''");

        // ******* 기존 AAMI 버전 3D 파싱용 *******
        defineQueryFormat(INSERT_QUERY_OLD_1, "INSERT INTO %%WORKSPACE_OLD_AAMI%%.UPP(TIME, HP, TEMP, WS, WD, LON, LAT, HT, RH, DEW, STNID, NUM) VALUES  (TO_DATE(''{0}'',''YYYY-MM-DD HH24:mi:ss''), ''{1}'', ''{2}'', ''{3}'', ''{4}'', ''{5}'', ''{6}'', ''{7}'', ''{8}'', ''{9}'', ''{10}'', ''{11}'')");
        //defineQueryFormat(INSERT_QUERY_OLD_2, "INSERT INTO %%WORKSPACE_OLD_AAMI%%.UPP_B(TIME, HP, TEMP, WS, WD, LON, LAT, HT, RH, DEW, STNID, NUM) VALUES  (TO_DATE(''{0}'',''YYYY-MM-DD HH24:mi:ss''), ''{1}'', ''{2}'', ''{3}'', ''{4}'', ''{5}'', ''{6}'', ''{7}'', ''{8}'', ''{9}'', ''{10}'', ''{11}'')");
        defineQueryFormat(INSERT_QUERY_OLD_2, "MERGE INTO %%WORKSPACE_OLD_AAMI%%.UPP_B d USING (SELECT TO_DATE(''{0}'',''YYYY-MM-DD HH24:mi:ss'') TIME, ''{10}'' STNID, ''{11}'' NUM FROM dual) s ON (d.TIME = s.TIME AND d.STNID = s.STNID AND d.NUM = s.NUM) WHEN NOT MATCHED THEN INSERT(TIME, HP, TEMP, WS, WD, LON, LAT, HT, RH, DEW, STNID, NUM) VALUES  (TO_DATE(''{0}'',''YYYY-MM-DD HH24:mi:ss''), ''{1}'', ''{2}'', ''{3}'', ''{4}'', ''{5}'', ''{6}'', ''{7}'', ''{8}'', ''{9}'', ''{10}'', ''{11}'')");
        defineQueryFormat(DELETE_QUERY_OLD, "DELETE %%WORKSPACE_OLD_AAMI%%.UPP WHERE STNID = ''{0}''");
    }

    /**
     * UPP의 기존 (기본) 포맷을 파싱하기 위한 함수
     *
     * @param dbManager
     * @param file
     * @param processorInfo
     * @param stationID
     * @throws Exception
     */
    private void processResourceVer1(DatabaseManager dbManager, File file, ProcessorInfo processorInfo, String stationID) throws Exception
    {
        Object[] bindArray;

        bindArray = new Object[DB_COLUMN_COUNT];
        Arrays.fill(bindArray, "");

        String initTime;
        int count;
        int[] indexArray = {1, 8, 2, 10, 5, 4, 7, 6}; // 데이터 파일 내 인덱스
        String query;

        initTime = null;
        count = 0;

        String content = new String(Files.readAllBytes(file.toPath()));
        content = content.replace("\r\r\n", "\r\n");
        String[] lines = content.split(Pattern.quote("\r\n"));

        for (int i = 0; i < lines.length; i++)
        {
            String line = lines[i];
            int lineNum = i + 1;

            if (lineNum == 1)
            {
                initTime = line.substring(31, 50); // TIME
            }
            else if (lineNum > 11)
            {
                String[] rawData;
                String[] spanTime;

                // parse additional timespan
                rawData = line.replaceAll("\\s+", "#").split("#");
                spanTime = rawData[0].split(":");

                // 데이터가 무효일 경우 라인 skip.
                if (rawData.length < 12 || spanTime.length < 2)
                {
                    continue;
                }

                count = Integer.parseInt(spanTime[0]) * 60 + Integer.parseInt(spanTime[1]);

                bindArray[0] = addSeconds(initTime, "yyyy-MM-dd HH:mm:ss", count); //TIME
                bindArray[1] = stationID;

                // insert rest of the columns (comparing to original columns from resource)
                for (int j = 2; j < 10; j++)
                {
                    bindArray[j] = rawData[indexArray[j - 2]];
                }

                // Remove previous duplicate data to update values
                query = buildPreDeleteQuery(retrieveQueryFormat(DELETE_QUERY), bindArray, DB_PRIMARY_KEY_INDEXES);
                dbManager.executeUpdate(query);

                // UPP 테이블 입력
                query = MessageFormat.format(retrieveQueryFormat(INSERT_QUERY), bindArray);
                dbManager.executeUpdate(query);
            }
        }

        Log.print("INFO : executing UPP_DataProcess.processOldVersionByFile()");
        // 기존 AAMI 데이터 입력
        processOldVersionByFile(dbManager, stationID, content);
    }

    private UPPData parseResourceVer2(Date startTime, String stationID, String line)
    {
        String[] token;
        token = line.split(" ");

        UPPData uppData = new UPPData();

        uppData.RecordTime = (Date)startTime.clone();
        uppData.RecordTime = DateUtils.addMinutes(uppData.RecordTime, Integer.parseInt(token[0]));
        uppData.RecordTime = DateUtils.addSeconds(uppData.RecordTime, Integer.parseInt(token[1]));

        uppData.StationID = stationID;
        uppData.PA = (int)Double.parseDouble(token[2]);
        uppData.GH = Integer.parseInt(token[7]);
        uppData.RH = Double.parseDouble(token[4]);
        uppData.TA = (int)Double.parseDouble(token[3]);
        uppData.TD = (int)Double.parseDouble(token[9]);
        uppData.WD = Integer.parseInt(token[6]);
        uppData.WS = (int)(1.94384449 * Double.parseDouble(token[5]));
        uppData.LAT = Double.parseDouble(token[11]);
        uppData.LON = Double.parseDouble(token[12]);

        return uppData;
    }

    /**
     * UPP의 다른 포맷을 파싱하기 위한 함수
     * Station ID 47155 (UPP_RAW_47155_20170106060000.txt)
     * @param dbManager
     * @param file
     * @param processorInfo
     * @param stationID
     * @throws Exception
     */
    private void processResourceVer2(DatabaseManager dbManager, File file, ProcessorInfo processorInfo, String stationID) throws Exception
    {
        String content = new String(Files.readAllBytes(file.toPath()));
        List<String> lines = Arrays.asList(content.split(Pattern.quote("\r\n")));
        String strTime;
        Date startTime;
        UPPData uppData;

        strTime = String.format("%s %s", lines.get(0).split("\t")[1], lines.get(1).split("\t")[1]);
        startTime = DateFormatter.parseDate(strTime, "dd/MM/yy HH:mm:ss");
        uppData = new UPPData();

        for (int i = 6; i < lines.size(); i++)
        {
            String line;
            String[] token;
            Object[] bindArray;
            String query;

            line = lines.get(i).trim().replaceAll(" +", " ");
            token = line.split(" ");

            // UPP ver2 파싱
            uppData = parseResourceVer2(startTime, stationID, line);
            bindArray = createDataObject(uppData);

            // Remove previous duplicate data to update values
            query = buildPreDeleteQuery(retrieveQueryFormat(DELETE_QUERY), bindArray, DB_PRIMARY_KEY_INDEXES);
            dbManager.executeUpdate(query);

            // UPP 테이블 입력
            query = MessageFormat.format(retrieveQueryFormat(INSERT_QUERY), bindArray);
            dbManager.executeUpdate(query);
        }

        Log.print("INFO : executing UPP_DataProcess.processOldVersionByFile()");
        // 기존 AAMI 데이터 입력
        processOldVersionByFileVer2(dbManager, startTime, stationID, content);
    }

    private Object[] createDataObject(UPPData uppData)
    {
        Object[] dataObject = new Object[DB_COLUMN_COUNT];
        int index = 0;

        dataObject[index++] = convertToDBText(uppData.RecordTime);
        dataObject[index++] = convertToDBText(uppData.StationID);
        dataObject[index++] = convertToDBText(uppData.PA);
        dataObject[index++] = convertToDBText(uppData.GH);
        dataObject[index++] = convertToDBText(uppData.TA);
        dataObject[index++] = convertToDBText(uppData.TD);
        dataObject[index++] = convertToDBText(uppData.WD);
        dataObject[index++] = convertToDBText(uppData.WS);
        dataObject[index++] = convertToDBText(uppData.LAT);
        dataObject[index++] = convertToDBText(uppData.LON);

        return dataObject;
    }

    /**
     * 각 (하나의) 파일에 대한 데이터 처리 함수
     * @param dbManager 데이터베이스 매니저
     * @param file 처리할 데이터 파일
     * @param processorInfo 처리할 데이터에 대한 부가적인 정보가 담긴 구조체
     * @throws Exception
     */
    @Override
    protected void processDataInternal(DatabaseManager dbManager, File file, ProcessorInfo processorInfo) throws Exception
    {
        String[] fileNameArray;
        String stationID;
        String firstLine = null;

        // UPP_RAW_47186_20160902000000.txt
        fileNameArray = file.getName().split("_");
        stationID = fileNameArray[2]; //STNID

        // AMOS 테이블 삭제
        //db.executeUpdate(MessageFormat.format(this.deleteQueryFormat, bindArray));

        try (BufferedReader br = new BufferedReader(new FileReader(file)))
        {
            firstLine = br.readLine();
        }

        if (firstLine.contains("Number of probe:"))
        {
            Log.print("INFO : executing UPP_DataProcess.processResourceVer1()");
            processResourceVer1(dbManager, file, processorInfo, stationID);
        }
        else if (firstLine.contains("Balloon release date"))
        {
            Log.print("INFO : executing UPP_DataProcess.processResourceVer2()");
            processResourceVer2(dbManager, file, processorInfo, stationID);
        }
        else
        {
            throw new DaemonException("Unsupported UPP data file.");
        }
    }

    //region AAMI 파싱 예전 버전
    private void processOldVersionByFileVer2(DatabaseManager dbManager, Date startTime, String stationID, String dataText) throws IOException
    {
        Object[] bindArray = new Object[12];
        LineNumberReader rdr = new LineNumberReader(new BufferedReader(new InputStreamReader(new ByteArrayInputStream(dataText.getBytes()))));
        SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        int count = 0;
        int step = 0;
        int index = 0;
        String initTime = DateFormatter.formatDate(startTime, "yyyy-MM-dd HH:mm:ss");

        String query;

        // ============================================================
        //              기존 3D 파싱용 (삭제하지 말것.)
        // ============================================================
        // truncate table by station ID (old AAMI)
        query = MessageFormat.format(retrieveQueryFormat(DELETE_QUERY_OLD), stationID);
        dbManager.executeUpdate(query);
        //                       3D 파싱용 끝
        // ============================================================

        for (int i = 0; i < 6; i++)
        {
            rdr.readLine();
        }

        for (String line; (line = rdr.readLine()) != null;)
        {
            String[] token;
            UPPData uppData;

            line = line.trim().replaceAll(" +", " ");
            token = line.split(" ");

            if (!line.equals(""))
            {
                // UPP ver2 파싱
                uppData = parseResourceVer2(startTime, stationID, line);

                bindArray[0] = convertToDBText(uppData.RecordTime); // TIME
                bindArray[1] = convertToDBText(uppData.PA); // HP
                bindArray[2] = convertToDBText(uppData.TA); // TEMP
                bindArray[3] = convertToDBText(uppData.WS); // WS
                bindArray[4] = convertToDBText(uppData.WD); // WD
                bindArray[5] = convertToDBText(uppData.LON); // LON
                bindArray[6] = convertToDBText(uppData.LAT); // LAT
                bindArray[7] = convertToDBText(uppData.GH); // HT
                bindArray[8] = convertToDBText(uppData.RH); // RH
                bindArray[9] = convertToDBText(uppData.TD); // DEW
                bindArray[10] = convertToDBText(stationID); // Station ID
                bindArray[11] = convertToDBText(((index++) + "").replace(",", "")); // NUM

                if (step % 60 == 0)
                {
                    // UPP 테이블 입력
                    query = MessageFormat.format(retrieveQueryFormat(INSERT_QUERY_OLD_1), bindArray);
                    dbManager.executeUpdate(query);

                    // UPP_B 테이블 입력
                    query = MessageFormat.format(retrieveQueryFormat(INSERT_QUERY_OLD_2), bindArray);
                    dbManager.executeUpdate(query);
                }
                else
                {
                    index--;
                }

                step++;
            }
        }

        rdr.close();
    }

    private void processOldVersionByFile(DatabaseManager dbManager, String stationID, String dataText) throws IOException
    {
        Object[] bindArray = new Object[12];
        LineNumberReader rdr = new LineNumberReader(new BufferedReader(new InputStreamReader(new ByteArrayInputStream(dataText.getBytes()))));
        SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        int count = 0;
        int step = 0;
        int index = 0;
        String initTime = null;

        String query;

        // ============================================================
        //              기존 3D 파싱용 (삭제하지 말것.)
        // ============================================================
        // truncate table by station ID (old AAMI)
        query = MessageFormat.format(retrieveQueryFormat(DELETE_QUERY_OLD), stationID);
        dbManager.executeUpdate(query);
        //                       3D 파싱용 끝
        // ============================================================

        for (String line; (line = rdr.readLine()) != null;)
        {
            if (rdr.getLineNumber() == 1)
            {
                initTime = line.substring(31, 50); // TIME
            }

            if (rdr.getLineNumber() > 11)
            {
                String[] rawData = line.replaceAll("\\s+", "#").split("#");

                // 데이터가 무효일 경우 라인 skip.
                if (rawData.length < 12)
                {
                    continue;
                }

                bindArray[0] = DaemonUtils.addSeconds(sdf2, initTime, count++); // TIME
                bindArray[1] = rawData[1]; // HP
                bindArray[2] = rawData[2]; // TEMP
                bindArray[3] = rawData[4]; // WS
                bindArray[4] = rawData[5]; // WD
                bindArray[5] = rawData[6]; // LON
                bindArray[6] = rawData[7]; // LAT
                bindArray[7] = rawData[8]; // HT
                bindArray[8] = rawData[3]; // RH
                bindArray[9] = rawData[10]; // DEW
                bindArray[10] = stationID; // Station ID
                bindArray[11] = ((index++) + "").replace(",", ""); // NUM

                if (step % 60 == 0)
                {
                    // UPP 테이블 입력
                    query = MessageFormat.format(retrieveQueryFormat(INSERT_QUERY_OLD_1), bindArray);
                    dbManager.executeUpdate(query);

                    // UPP_B 테이블 입력
                    query = MessageFormat.format(retrieveQueryFormat(INSERT_QUERY_OLD_2), bindArray);
                    dbManager.executeUpdate(query);
                }
                else
                {
                    index--;
                }

                step++;
            }
        }

        rdr.close();
    }
    //endregion

    @Override
    protected int getDateFileIndex()
    {
        return FILE_DATE_INDEX_POS;
    }

    private static String addSeconds(String dateStr, String dateFormat, int seconds)
    {
        Calendar cal;
        Date date;

        cal = null;

        try
        {
            cal = new GregorianCalendar(Locale.KOREA);
            date = DateFormatter.parseDate(dateStr, dateFormat);
            cal.setTime(date);
            cal.add(Calendar.SECOND, seconds);
        }
        catch(ParseException pe)
        {
            return "";
        }

        return DateFormatter.formatDate(cal.getTime(), dateFormat);
    }
}