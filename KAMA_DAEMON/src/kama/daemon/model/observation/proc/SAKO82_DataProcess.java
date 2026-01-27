package kama.daemon.model.observation.proc;

import kama.daemon.common.db.DataProcessor;
import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.db.struct.ProcessorInfo;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.model.observation.adopt.SAKO82.Airforce_METAR;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.text.MessageFormat;
import java.util.List;

/**
 * @author chlee
 * Created on 2016-11-28.
 * 공군 METAR
 */
public class SAKO82_DataProcess extends DataProcessor
{
    private static final String DATAFILE_PREFIX = "sako82";
    private static final int DB_COLUMN_COUNT = 6;
    private static final int FILE_DATE_INDEX_POS = 2; // SAKO82_RKTF_201609010059_111I
    private static final int[] DB_PRIMARY_KEY_INDEXES = { 0, 1, 2, 3, 4 }; // TM, STN_CD, MSG_TYPE, MSG_STS, INP_TYPE
    private final int INSERT_QUERY = 1;
    private final int DELETE_QUERY = 2;

    public SAKO82_DataProcess(DaemonSettings settings)
    {
        super(settings, DATAFILE_PREFIX);
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
        String[] fileNameArray;

        query = null;

        Airforce_METAR metar = new Airforce_METAR();
        List<String> lines = Files.readAllLines(file.toPath(), Charset.defaultCharset());
        String[] details;

        metar.RecordDate = processorInfo.FileDateFromNameOriginal;
        metar.StationCode = lines.get(2).split(" ")[1];
        metar.MsgType = lines.get(4);
        metar.MsgStatus = " ";
        metar.InputType = 0;

        details = lines.get(6).split(" ");
        metar.Visiblity = Integer.parseInt(details[3]);

        bindArray = createDataObject(metar);

        // Remove previous duplicate data to update values
        query = buildPreDeleteQuery(retrieveQueryFormat(DELETE_QUERY), bindArray, DB_PRIMARY_KEY_INDEXES);
        dbManager.executeUpdate(query);

        // MOR_METAR 테이블 입력
        query = MessageFormat.format(retrieveQueryFormat(INSERT_QUERY), bindArray);
        dbManager.executeUpdate(query);
    }

    private Object[] createDataObject(Airforce_METAR metar)
    {
        Object[] dataSet;
        int index = 0;

        dataSet = new Object[DB_COLUMN_COUNT];

        dataSet[index++] = convertToDBText(metar.RecordDate);
        dataSet[index++] = convertToDBText(metar.StationCode);
        dataSet[index++] = convertToDBText(metar.MsgType);
        dataSet[index++] = convertToDBText(metar.MsgStatus);
        dataSet[index++] = convertToDBText(metar.InputType);
        dataSet[index++] = convertToDBText(metar.Visiblity);

        return dataSet;
    }

    @Override
    protected void defineQueries()
    {
        defineQueryFormat(INSERT_QUERY, "INSERT INTO %%WORKSPACE%%.METAR(TM, STN_CD, MSG_TYPE, MSG_STS, INP_TYPE, INP_TM, VIS) VALUES (TO_DATE(''{0}'', ''YYYY-MM-DD HH24:mi:ss''), ''{1}'', ''{2}'', ''{3}'', ''{4}'', SYSDATE , ''{5}'')");
        defineQueryFormat(DELETE_QUERY, "DELETE FROM %%WORKSPACE%%.METAR WHERE (TM = TO_DATE(''{0}'', ''YYYY-MM-DD HH24:mi:ss'') AND STN_CD = ''{1}'' AND MSG_TYPE = ''{2}'' AND MSG_STS = ''{3}'' AND INP_TYPE = ''{4}'')");
    }

    @Override
    protected int getDateFileIndex()
    {
        return FILE_DATE_INDEX_POS;
    }
}