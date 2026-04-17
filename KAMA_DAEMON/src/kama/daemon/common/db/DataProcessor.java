package kama.daemon.common.db;

import kama.daemon.common.db.struct.ProcessorInfo;
import kama.daemon.common.util.*;
import kama.daemon.main.struct.StartupInfo;

import org.apache.commons.lang3.time.DateUtils;

import ucar.nc2.util.xml.Parse;

import java.io.File;
import java.text.MessageFormat;
import java.text.ParseException;
import java.util.*;
import java.util.regex.Pattern;

import static kama.daemon.common.util.DataFileStore.listAllFilesRecursively;
import static kama.daemon.common.util.DataFileStore.removeEmptyDirectoriesRecursively;

/**
 * @author chlee
 * Created on 2016-11-24. (modified version of jetddo)
 */
public abstract class DataProcessor
{
    private StartupInfo.MODEL_TYPE _modelType;
    private String _classPrefix;
    protected abstract void defineQueries();
    protected abstract int getDateFileIndex();

    /**
     * 내부적으로 파일 처리하는 함수
     * @param dbManager 데이터베이스 매니저
     * @param file 처리할 파일 (** 가끔 처리할 파일이 교체되는 경우가 있음.)
     * @param processorInfo 파일 처리에 필요한 부가적인 정보
     * @throws Exception
     */
    protected abstract void processDataInternal(DatabaseManager dbManager, File file, ProcessorInfo processorInfo) throws Exception;

    protected static final String DATE_FORMAT_JAVA_TO_DB = "yyyy-MM-dd HH:mm:ss";

    private DaemonSettings _settings;
    private String _workspaceName;
    private String _oldWorkspaceName; // AAMI용 기존 3D 파싱
    private HashMap<Integer, String> _hsQueryFormats;
    protected boolean insertHistory = true;
    protected boolean insertRealTable = false;
    /**
     * 클래스 생성자
     * @param settings 설정값
     * @param configPrefix config.properties 로부터 사용할 클래스 prefix (e.g., amos, asos...)
     */
    public DataProcessor(DaemonSettings settings, String configPrefix)
    {
        _init(settings, configPrefix);
    }

    /**
     * 클래스 생성시에 초기화 매서드
     * @param settings 설정값
     * @param configPrefix config.properties 로부터 사용할 클래스 prefix (e.g., amos, asos...)
     */
    private void _init(DaemonSettings settings, String configPrefix)
    {
        _initializeDerivedComponent();
        _settings = settings;
        settings.setConfigPrefix(configPrefix);

        // 예측/관측 모델에 따라 들어가는 DB workspace 이름이 다름.
        if (_modelType == StartupInfo.MODEL_TYPE.OBSERVATION)
        {
            _workspaceName = _settings.DB_workspace();
        }
        else
        {
            _workspaceName = _settings.DB_workspace_predict();
        }

        _oldWorkspaceName = _settings.DB_workspace_old_aami(); // AAMI용 기존 3D 파싱
        _hsQueryFormats = new HashMap<Integer, String>();

        defineQueries();
    }

    private void _initializeDerivedComponent()
    {
        String classFullName;

        classFullName = this.getClass().getName();
        _classPrefix = classFullName.split("\\.(?=[^\\.]+$)")[1];
        _classPrefix = _classPrefix.split(Pattern.quote("_DataProcess"))[0];

        // 관측/예측모델 구분하기
        if (classFullName.contains("kama.daemon.model.observation.proc"))
        {
            _modelType = StartupInfo.MODEL_TYPE.OBSERVATION;
            _classPrefix = classFullName.split("\\.")[5];
            _classPrefix = _classPrefix.split(Pattern.quote("_DataProcess"))[0];
        }
        else if (classFullName.contains("kama.daemon.model.prediction.proc"))
        {
            _modelType = StartupInfo.MODEL_TYPE.PREDICTION;
            _classPrefix = classFullName.split("\\.")[5];
            _classPrefix = _classPrefix.split(Pattern.quote("_DataProcess"))[0];
        }
        else if (classFullName.contains("kama.daemon.model.assessment.proc"))
        {
            _modelType = StartupInfo.MODEL_TYPE.ASSESSMENT;
            _classPrefix = classFullName.split("\\.")[5];
            _classPrefix = _classPrefix.split(Pattern.quote("_DataProcess"))[0];
        }
        else
        {
            throw new RuntimeException("Model type is not specified");
        }
    }

    protected final DatabaseManager openDBManager()
    {
        DatabaseManager dbManager;
        dbManager = DatabaseManager.getInstance();
        dbManager.setConfig(_settings);
        dbManager.setAutoCommit(false);

        return dbManager;
    }

    protected final void defineQueryFormat(int queryName, String query)
    {
        query = makeQuery(query);
        _hsQueryFormats.put(queryName, query);
    }

    protected final String retrieveQueryFormat(int queryName)
    {
        return _hsQueryFormats.get(queryName);
    }

    /**
     * workspace 설정 등을 반영하여 쿼리 자동 생성
     * @param query 쿼리 템플릿
     * @return 설정된 값을 반영한 쿼리
     */
    protected final String makeQuery(String query)
    {
        String sWorkspaceName;
        String sOldWorkspaceName;

        sWorkspaceName = _workspaceName;
        sOldWorkspaceName = _oldWorkspaceName;

        if (!sWorkspaceName.equals(""))
        {
            sWorkspaceName = String.format("%s.", sWorkspaceName);
        }

        if (!_oldWorkspaceName.equals(""))
        {
            sOldWorkspaceName = String.format("%s.", _oldWorkspaceName);
        }

        query = query.replace("%%WORKSPACE%%.", sWorkspaceName);
        query = query.replace("%%WORKSPACE_OLD_AAMI%%.", sOldWorkspaceName);

        return query;
    }

    /**
     * 데이터 파일 처리 (가장 중요한 매서드)
     */
    public final void processDataFile()
    {
        /***************************************
            Major tasks
            1. File exists in FTP path?
            2. If file exists, get DB instance, delete query, and commit
            3. For each files
               3.1. Extract date from file
               3.2. Get file type (config.type)
               3.3. Create output path (config.path\yyyy/MM/dd)
               3.4. Read resource file line by line
                    3.4.1. Insert data
                    3.4.2. Query (INSERT INTO)
               3.5. Move file to output path
               3.4.3. Commit or rollback
         ****************************************/

        File ftpPath;
        DatabaseManager dbManager;

        ftpPath = _settings.FTPLocalPath();

        if (ftpPath.exists() && ftpPath.isDirectory())
        {
            String fileFilter;
            File[] dataFiles;

            fileFilter = _settings.FileFilter();
            dataFiles = listAllFilesRecursively(ftpPath, Pattern.compile(fileFilter));

            // Sort retrieved data files
            Arrays.sort(dataFiles);

            if (dataFiles.length > 0)
            {
                Log.print("[{0}] INFO : File COUNT -> {1}", DateFormatter.formatDate(new Date(), "yyyy-MM-dd HH:mm:ss.SSS"), dataFiles.length);

                dbManager = openDBManager();

                processDataFiles(dbManager, dataFiles);

                if (dbManager != null)
                {
                    dbManager.safeClose();
                }

                // 빈 디렉토리 삭제
                //removeEmptyDirectoriesRecursively(_settings.FTPLocalPath());
            }
        }
    }

    protected final void processDataFiles(DatabaseManager dbManager, File[] dataFiles)
    {
        List<File> groupedFiles;
        String parentFileName = "";

        // 폴더별로 묶어야함
        groupedFiles = new ArrayList<>();

        if (dataFiles.length > 0)
        {
            parentFileName = dataFiles[0].getParentFile().getName();

            for (File file : dataFiles)
            {
                String currentParentFileName;

                currentParentFileName = file.getParentFile().getName();
              
                if (parentFileName.equals(currentParentFileName))
                {  
                    groupedFiles.add(file);
                }
                else
                {
                    File[] files = new File[groupedFiles.size()];
                    files = groupedFiles.toArray(files);

                    processGroupedDataFiles(dbManager, files);

                    groupedFiles = new ArrayList<>();
                    groupedFiles.add(file);
                    parentFileName = file.getParentFile().getName();
                }
            }

            File[] files = new File[groupedFiles.size()];
            files = groupedFiles.toArray(files);
            processGroupedDataFiles(dbManager, files);
        }
    }

    protected final void processGroupedDataFiles(DatabaseManager dbManager, File[] dataFiles)
    {
        ProcessorInfo processorInfo;
        Date dtFileDate;
       
        processorInfo = new ProcessorInfo();

        try
        {
            if (dataFiles.length > 0)
            {
                dtFileDate = parseDateFromParentDirName(dataFiles[0]);

                // 부모 디렉토리에 날짜가 존재할 경우
                if (dtFileDate != null)
                {
                    processorInfo = createBasicProcessorInfo(processorInfo, dtFileDate);
                }
                else
                {
                    try
                    {
                        Date dtFileDateChild = parseDateFromFileName(dataFiles[0]);
                     
                        processorInfo = createBasicProcessorInfo(processorInfo, dtFileDateChild);
                    }
                    catch (ParseException pe)
                    {
                        throw new DaemonException("Error : Unable to parse date: either parent folder or child file must contain date entry.");
                    }
                }
            }

            processorInfo.FilesToProcess = dataFiles;
            processorInfo.FileType = _settings.FileType();
            processorInfo.ClassPrefix = _classPrefix;
        }
        catch (Exception pe)
        {
            throw new DaemonException(String.format("Error : unable to convert date format.\n%s", pe.toString()), pe);
        }

        try
        {
            processDataByFileGroup(dbManager, dataFiles, processorInfo);
        }
        catch(Exception ex)
        {
            dbManager.rollback();
            throw new DaemonException(String.format("Error : %s_DataProcess.process::processGroupedDataFiles() -> %s", _classPrefix, ex.toString()), ex);
        }
    }

    /**
     * 선택된 파일을 처리하는 내부함수
     * @param dbManager
     * @param dataFiles
     * @return
     */
    protected void processDataByFileGroup(DatabaseManager dbManager, File[] dataFiles, ProcessorInfo processorInfo) throws Exception
    {
        for (File file : dataFiles)
        {
            String query;

            // 처리할 파일명 로그 print
            Log.print("[{0}] INFO : File NAME -> {1}", DateFormatter.formatDate(new Date(), "yyyy-MM-dd HH:mm:ss.SSS"), file.getAbsolutePath());

            try
            {
                try
                {
                    Date fileDate = parseDateFromFileName(file);
                    processorInfo = createBasicProcessorInfo(processorInfo, fileDate);

                    processorInfo.FilesToProcess = dataFiles;
                    
                    // Execute customized event
                    processDataInternal(dbManager, file, processorInfo);
                }
                catch (Exception e)
                {
                    throw new DaemonException(String.format("Error : %s_DataProcess.process -> %s", _classPrefix, e.toString()), e);
                }

                // if moving file succeeded
                if (DataFileStore.storeDateFile(file, processorInfo.FileSavePath))
                {
                	if(this.insertHistory) {
                		query = this.buildFileInfoQuery(processorInfo, new File(processorInfo.FileSavePath, file.getName()));
                        dbManager.executeUpdate(query);	
                	}
                    
                    dbManager.commit();
                }
                else
                {
                    throw new DaemonException("Error : unable to move resource files.");
                }
            }
            catch (DaemonException ex)
            {
                // 2017/04/14 깨진 데이터 파일 Temp 폴더로 이동
                DataFileStore.storeDateFile(file, processorInfo.FileCorruptedTempPath);

                dbManager.rollback();
            }
        }
    }

    /**
     * 데이터 프로세스에 필요한 기본 정보 입력
     * @param processorInfo
     * @param dtFileDate
     * @return
     * @throws ParseException
     */
    private ProcessorInfo createBasicProcessorInfo(ProcessorInfo processorInfo, Date dtFileDate) throws ParseException
    {
        if (_modelType == StartupInfo.MODEL_TYPE.OBSERVATION)
        {
            // 관측모델일 경우 DATA_STORE에 년/월/일까지만 path로 정하고,
            processorInfo.FileSavePath = String.format("%s/%s", _settings.FileSavePath(), DateFormatter.formatDate(dtFileDate, "yyyy/MM/dd"));
        }
        else if (_modelType == StartupInfo.MODEL_TYPE.PREDICTION)
        {
            // 예측모델일 경우 DATA_STORE에 년/월/일/시 까지 path로 정함
            processorInfo.FileSavePath = String.format("%s/%s", _settings.FileSavePath(), DateFormatter.formatDate(dtFileDate, "yyyy/MM/dd/HH"));
        }
        else if (_modelType == StartupInfo.MODEL_TYPE.ASSESSMENT)
        {
            // 예측모델일 경우 DATA_STORE에 년/월/일/시 까지 path로 정함
            processorInfo.FileSavePath = String.format("%s/%s", _settings.FileSavePath(), DateFormatter.formatDate(dtFileDate, "yyyy/MM/dd/HH"));
        }
        else
        {
            throw new DaemonException("Invalid model type detected while building DATA_STORE path.");
        }

        // 깨진 파일 임시 저장 경로 입력
        processorInfo.FileCorruptedTempPath = _settings.FileCorruptedTempPath();

        // 기입된 기본 날짜 입력
        processorInfo.FileDateFromNameOriginal = (Date)dtFileDate.clone();

        // 예측모델은 파일명에 적혀있는 시간값이 UTC 이기에, 9시간 더해주어 KST 로 표현함.
        if (_modelType == StartupInfo.MODEL_TYPE.PREDICTION)
        {
            processorInfo.FileDateFromNameKST = DateUtils.addHours((Date)dtFileDate.clone(), 9);
            processorInfo.FileDateFromNameUTC = dtFileDate;
        }
        else
        {
            processorInfo.FileDateFromNameKST = dtFileDate;
            processorInfo.FileDateFromNameUTC = DateUtils.addHours((Date)dtFileDate.clone(), -9); // Set UTC
        }

        return processorInfo;
    }

    /**
     * 테스트 후 서버에 남겨진 윈도우 파일 경로 전부 삭제
     */
    public final void truncateWindowsFileInfo()
    {
        DatabaseManager dbManager;

        dbManager = openDBManager();
        dbManager.executeUpdate("DELETE FROM " + this.getDatabaseOwner() + ".DMON_FILE_PROC_H WHERE FILE_STR_LOC NOT LIKE '/%'");
        dbManager.commit();

        if (dbManager != null)
        {
            dbManager.safeClose();
        }
    }

    /**
     * 데이터 파일을 분류하여 폴더에 이동 후,
     * 해당 파일 경로 정보를 DB 테이블에 입력하는 쿼리 생성
     * (3D 엔진에서 사용하는 정보)
     * @param processorInfo 데이터 파일 정보를 포함한 구조체
     * @return INSERT 쿼리문
     */
    protected final String buildFileInfoQuery(ProcessorInfo processorInfo, File file)
    {
        StringBuilder sb;
        File newFile;

        sb = new StringBuilder();
        newFile = new File(processorInfo.FileSavePath, file.getName());

        // DATA_STORE 폴더 내에 복사된 파일이 존재하지 않을 경우, 예외 throw.
        if (!newFile.exists())
        {
            throw new DaemonException("Error : the copied file does not exist.");
        }

        sb.append("INSERT INTO " + this.getDatabaseOwner() + ".DMON_FILE_PROC_H(DH_SEQ, FILE_DT, FILE_STR_LOC, FILE_CD, PROC_DT) ");
        sb.append("VALUES (AAMI.DMON_FILE_PROC_H_SEQ.NEXTVAL, TO_DATE('"+convertToDBText(processorInfo.FileDateFromNameOriginal)+"', 'YYYY-MM-DD HH24:mi:ss'), '");
        sb.append(newFile.getAbsolutePath()+"', '"+processorInfo.FileType+"', sysdate)");

        return sb.toString();
    }

    // Remove previous record just before insert (to prevent duplicates)
    protected final String buildPreDeleteQuery(String deleteQuery, Object[] records, int[] columnIndexes)
    {
        for (int i = 0; i < columnIndexes.length; i++)
        {
            deleteQuery = deleteQuery.replace(String.format("{%d}", i), records[columnIndexes[i]].toString());
        }

        // Pass MessageFormat to correct syntactic characters
        deleteQuery = MessageFormat.format(deleteQuery, 0);

        return deleteQuery;
    }

    /**
     * 파일명으로부터 날짜 정보 파싱하는 함수
     * @param file 파싱할 텍스트
     * @return 파싱된 날짜
     * @throws ParseException
     */
    protected Date parseDateFromFileName(File file) throws ParseException
    {
        // 날짜 포맷을 가장 길게 설정하고, 실제 파일 포맷에 맞게 뒷부분 잘라서 사용
        String defaultDateFormat = "yyyyMMddHHmmssSS";
        Date parentDate;

        // 부모 디렉토리로부터 날짜 파싱 시도
        parentDate = parseDateFromParentDirName(file);

        if (parentDate != null)
        {
            return parentDate;
        }

        //return DateFormatter.parseDateFromString(file.getName(), defaultDateFormat, 0);
        return DateFormatter.parseAnyDate(file.getName());
    }

    protected Date parseDateFromParentDirName(File file) throws ParseException
    {
        String defaultDateFormat = "yyyyMMddHHmmssSS";

        if (!file.getParentFile().equals(_settings.FTPLocalPath()))
        {
            File parentDir = file.getParentFile();
            String parentDirName = parentDir.getName();
            String dateFormat = defaultDateFormat.substring(0, parentDirName.length());
            Pattern p = Pattern.compile(String.format("(.*)[0-9]{%d}(.*)", dateFormat.length()));

            // 부모 디렉토리가 날짜값을 가지고 있을 경우, 해당 정보를 파싱.
            if (p.matcher(parentDirName).matches())
            {
                //return DateFormatter.parseDateFromString(parentDir.getName(), dateFormat, 0);
                return DateFormatter.parseAnyDate(parentDir.getName());
            }
        }

        return null;
    }

    protected final static List<File[]> classifyFiles(File[] files, String[] filters)
    {
        List<File[]> organizedFiles;
        List<File> lstOrgFile;
        List<File> lstFile;

        lstOrgFile = Arrays.asList(files);
        organizedFiles = new ArrayList<>();

        for (int i = 0; i < filters.length; i++)
        {
            lstFile = new ArrayList<>();

            for (File file : lstOrgFile)
            {
                // 파일 패턴이 일치할 경우
                if (file.getName().matches(filters[i]))
                {
                    lstFile.add(file);
                    lstOrgFile.remove(file);
                }
            }

            organizedFiles.add(lstFile.toArray(new File[lstFile.size()]));
        }

        return organizedFiles;
    }

    /**
     * 데이터 타입을 DB에 넣기 위한 text 로 변환
     * @param data 다양한 데이터 object
     * @return DB에 넣을 수 있는 text 포맷
     */
    protected static String convertToDBText(Object data)
    {
        if (data instanceof Date)
        {
            if ((Date)data == null)
            {
                return "";
            }

            return DateFormatter.formatDate((Date)data, DATE_FORMAT_JAVA_TO_DB);
        }
        else if (data instanceof Integer)
        {
            if ((int)data == Integer.MIN_VALUE)
            {
                return "";
            }

            return Integer.toString((int)data);
        }
        else if (data instanceof Double)
        {
            if (Double.isNaN((double)data))
            {
                return "";
            }

            return Double.toString((double)data);
        }
        else if (data instanceof Float)
        {
            if (Float.isNaN((float)data))
            {
                return "";
            }

            return Float.toString((float)data);
        }
        else if (data instanceof String)
        {
            if ((String)data == null)
            {
                return "";
            }

            return (String)data;
        }
        else if(data == null)
        {
            return "";
        }
        else
        {
            throw new RuntimeException("Conversion not supported.");
        }
    }
    
    protected String getDatabaseOwner() {
    	if(this.insertRealTable) {
    		return "AAMI";
    	} else {
    		return "AAMI_TEST";
    	}
    }
    
}