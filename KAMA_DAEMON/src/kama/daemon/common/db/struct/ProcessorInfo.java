package kama.daemon.common.db.struct;

import kama.daemon.main.DaemonMain;

import java.io.File;
import java.util.Date;

/**
 * @author chlee
 * Created on 2016-12-19.
 * 데이터 프로세서 각 파일 처리시에 필요한 정보를 포함하는 클래스.
 * (추후 필요한 정보가 있을시 추가 후 DataProcessor 클래스에서 입력하여 XXX_DataProcessor로 인자로 전달하면 됨.)
 */
public class ProcessorInfo
{
    public String FileSavePath; // 파일 저장 경로
    public String FileCorruptedTempPath; // 깨진 파일 임시 저장 경로
    public Date FileDateFromNameKST; // 파일명에 포함된 날짜 정보 (KST)
    public Date FileDateFromNameUTC; // 파일명에 포함된 날짜 정보 (UTC)
    public Date FileDateFromNameOriginal; // 파일명에 기입된 날짜 정보
    public String FileType; // 파일 타입. (e.g., AMDAR, AMOS...)
    public String ClassPrefix; // 클래스 prefix. (e.g., amdar, amos...)
    //public File DataFileToProcess; // 3차원 예측 데이터에서, 찾은 파일과 옮겨야 할 파일이 불일치할 경우, 이 파라메터를 사용 (e.g., UM_LOA, REA, VIS 프로세서)
    public File[] FilesToProcess; // 현재 큐에 있는 전체 파일 목록 (3차원 예측 데이터 처리시에 사용)

    public ProcessorInfo()
    {
    }
}