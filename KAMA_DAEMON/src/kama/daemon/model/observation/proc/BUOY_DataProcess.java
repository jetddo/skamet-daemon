package kama.daemon.model.observation.proc;

import kama.daemon.common.db.DataProcessor;
import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.db.struct.ProcessorInfo;
import kama.daemon.common.util.DaemonException;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.common.util.DateFormatter;

import java.io.*;
import java.text.MessageFormat;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Date;

/**
 * @author chlee
 * Created on 2016-12-07.
 */
public class BUOY_DataProcess extends DataProcessor
{
    private static final String DATAFILE_PREFIX = "buoy";
    private static final int DB_COLUMN_COUNT = 17;
    private static final int FILE_DATE_INDEX_POS = 1; // VBKO60_2016080100
    private static final int[] DB_PRIMARY_KEY_INDEXES = { 0, 1 }; // TM, STN_ID
    private final int INSERT_QUERY = 1;
    private final int DELETE_QUERY = 3;

    public BUOY_DataProcess(DaemonSettings settings)
    {
        super(settings, DATAFILE_PREFIX);
    }

    @Override
    protected void defineQueries()
    {
        defineQueryFormat(INSERT_QUERY, "INSERT INTO %%WORKSPACE%%.SEA_BUOY(TM, STN_ID, WD1, WS1, WS1_GST, WD2, WS2, WS2_GST, PA, HM, TA, TW, WH_MAX, WH_SIG, WH_AVE, WP, WO) VALUES (TO_DATE(''{0}'', ''YYYY-MM-DD HH24:mi:ss''), ''{1}'', ''{2}'', ''{3}'', ''{4}'', ''{5}'', ''{6}'', ''{7}'', ''{8}'', ''{9}'', ''{10}'', ''{11}'', ''{12}'', ''{13}'', ''{14}'', ''{15}'', ''{16}'')");
        defineQueryFormat(DELETE_QUERY, "DELETE FROM %%WORKSPACE%%.SEA_BUOY WHERE TM = TO_DATE(''{0}'', ''YYYY-MM-DD HH24:mi:ss'') AND STN_ID = ''{1}''");
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
        String query;
        Object[] bindArray;

        query = null;
        bindArray = new Object[DB_COLUMN_COUNT];
        Arrays.fill(bindArray, "");

        try (BufferedReader br = new BufferedReader(new FileReader(file)))
        {
            for (String line; (line = br.readLine()) != null; )
            {
                String[] rawData;
                int index;


                if (line.indexOf("#") == -1)
                {
                    continue;
                }

                rawData = line.split("#");

                // Parse date from raw data file
                try
                {
                    String sDateTime;
                    sDateTime = rawData[1];
                    bindArray[0] = DateFormatter.changeFormat(sDateTime, "yyyyMMddHHmm", "yyyyMMddHHmmss");
                } catch (ParseException pe)
                {
                    throw new DaemonException("Error : unable to parse date");
                }

                // real sequence
                int[] seq = {1, 0, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
                int[] div = {1, 1, 1, 10, 10, 1, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 1};

                // Add value according to the raw data columns
                for (int i = 0; i < bindArray.length; i++)
                {
                    if (div[i] == 10)
                    {
                		// 실수 자료를 정수로 저장하여 수정
                    	// bindArray[seq[i]] = String.format("%d", Integer.parseInt(rawData[i]) / div[i]);
                        bindArray[seq[i]] = String.format("%f", Float.parseFloat(rawData[i]) / div[i]);
                    }
                    else
                    {
                        bindArray[seq[i]] = rawData[i];
                    }
                }

                // Remove previous duplicate data to update values
                query = buildPreDeleteQuery(retrieveQueryFormat(DELETE_QUERY), bindArray, DB_PRIMARY_KEY_INDEXES);
                dbManager.executeUpdate(query);

                // LGT 테이블 입력
                query = MessageFormat.format(retrieveQueryFormat(INSERT_QUERY), bindArray);
                dbManager.executeUpdate(query);
            }
        }
    }

    /**
     * 임시 추가 (2017/02/10)
     * 문제파일: VBKO60_201702081800]21229
     * @param file 데이터 파일
     * @return
     * @throws ParseException
     */
    @Override
    protected Date parseDateFromFileName(File file) throws ParseException
    {
        String format = "yyyyMMddHHmmssSS";
        Date dtFileDate;
        String[] fileNameToken;
        String dateFromFileName;

        fileNameToken = file.getName().split("_");
        dateFromFileName = fileNameToken[getDateFileIndex()].split("]")[0];

        // 파일 확장자 삭제
        if (dateFromFileName.contains("."))
        {
            dateFromFileName = dateFromFileName.substring(0, dateFromFileName.indexOf('.'));
        }

        // 날짜 파싱 (포맷이 길 경우, 포맷을 실제 날짜값에 맞게 뒷부분 제거)
        try
        {
            dtFileDate = DateFormatter.parseDate(dateFromFileName, format.substring(0, dateFromFileName.length()));
        }
        catch (ParseException pe)
        {
            return super.parseDateFromFileName(file);
        }

        return dtFileDate;
    }

    @Override
    protected int getDateFileIndex()
    {
        return FILE_DATE_INDEX_POS;
    }
}