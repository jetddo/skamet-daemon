package kama.daemon.model.observation.adopt.AMOS;

import java.util.HashMap;

/**
 * @author chlee
 * Dictionary for data columns
 * 기존 파싱코드를 그대로 유지하고 새로운 구조의 테이블에 적용하기 위해 만든 어댑터 클래스.
 * (추후 관련 문서 참고하여 데이터 직접 파싱하는 방법으로 변경 필요.)
 */
public class AMOS_Dict
{
    private static AMOS_Dict instance = new AMOS_Dict() ;
    // Column indices from resource (.txt) file

    // Column indices from Resource file
    final String[] _resColumns = new String[] {"TM", "STN_ID", "RWY_DIR", "RWY_USE", "WD_2MIN_AVG", "WD_2MIN_MNM", "WD_2MIN_MAX", "WD_10MIN_AVG", "WD_10MIN_MNM", "WD_10MIN_MAX", "WSPD_2MIN_AVG", "WSPD_2MIN_MNM", "WSPD_2MIN_MAX", "WSPD_10MIN_AVG", "WSPD_10MIN_MNM", "WSPD_10MIN_MAX", "MOR_1MIN", "MOR_1MIN_MID", "MOR_10MIN", "MOR_10MIN_MID", "RVR_1MIN", "RVR_1MIN_MID", "RVR_10MIN", "RVR_10MIN_MID", "WW_CO", "WW_LTTR", "VIS_1MIN", "BASE_1LYR", "BASE_2LYR", "BASE_3LYR", "TMP", "DP", "HM", "RN_1DD", "QFE", "QNH", "QFF"};
    // Column indices from Oracle DB
    final String[] _dbColumns = new String[] {"TM", "STN_ID", "RWY_DIR", "RWY_USE", "WD_3SEC_AVG", "WD_1MIN_AVG", "WD_1MIN_MNM", "WD_1MIN_MAX", "WD_2MIN_AVG", "WD_2MIN_MNM", "WD_2MIN_MAX", "WD_10MIN_AVG", "WD_10MIN_MNM", "WD_10MIN_MAX", "WSPD_3SEC_AVG", "WSPD_1MIN_AVG", "WSPD_1MIN_MNM", "WSPD_1MIN_MAX", "WSPD_2MIN_AVG", "WSPD_2MIN_MNM", "WSPD_2MIN_MAX", "WSPD_10MIN_AVG", "WSPD_10MIN_MNM", "WSPD_10MIN_MAX", "WSPD_3SEC_MAX", "GUST_2MIN", "GUST_10MIN", "TAIL_2MIN", "TAIL_10MIN", "CRW_2MIN", "CRW_10MIN", "MOR_1MIN", "MOR_1MIN_MID", "MOR_10MIN", "MOR_10MIN_MID", "MOR_10MIN_MNM", "MOR_10MIN_MAX", "RVR_1MIN", "RVR_1MIN_MID", "RVR_10MIN", "RVR_10MIN_MID", "RVR_10MIN_MNM", "RVR_10MIN_MAX", "LGT_INTST_EG", "LGT_INTST_C", "BCL", "WW_CO", "WW_LTTR", "VIS_1MIN", "VIS_10MIN", "CLA_1LYR", "CLA_2LYR", "CLA_3LYR", "BASE_1LYR", "BASE_2LYR", "BASE_3LYR", "VER_VIS", "TMP", "DP", "HM", "RN_1MIN", "RN_30MIN", "RN_1HR", "RN_3HR", "RN_12HR", "RN_1DD", "PRSS", "QFE", "QNH", "QFF", "CAL_RN_1DD"};
    HashMap<String, Integer> _hmResColumns;
    HashMap<String, Integer> _hmDBColumns;

    private AMOS_Dict()
    {
        _hmResColumns = new HashMap<String, Integer>();
        _hmDBColumns = new HashMap<String, Integer>();

        for (int i = 0; i < _resColumns.length; i++)
        {
            _hmResColumns.put(_resColumns[i], i);
        }

        for (int i = 0; i < _dbColumns.length; i++)
        {
            _hmDBColumns.put(_dbColumns[i], i);
        }
    }

    public static AMOS_Dict getInstance()
    {
        return instance;
    }

    public int getIndexFromResToOracle(int index)
    {
        String colName;
        colName = _resColumns[index];

        return _hmDBColumns.get(colName);
    }

    public int getIndexFromOracleToRes(int index)
    {
        String colName;
        colName = _dbColumns[index];

        return _hmResColumns.get(colName);
    }
}