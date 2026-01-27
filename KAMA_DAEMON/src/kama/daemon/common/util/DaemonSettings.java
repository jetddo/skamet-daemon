package kama.daemon.common.util;

import kama.daemon.main.struct.StartupInfo;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.io.File;
import java.text.MessageFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author chlee
 * Created on 2016-11-24.
 * 데몬 configuration file (config.properties) 로부터 정보를 받아오는 클래스
 */
public class DaemonSettings
{
    public enum WORKSPACE_TYPE { REMOTE, LOCAL };

    protected Configuration _config;
    protected String _configPrefix;

    private static String _os_name = null;
    private static String _currentWorkingDirectory = null;

    public final boolean IsWindows = _isWindows();
    private GlobalConfig _globalConfig;

    private class GlobalConfig
    {
        public String db_url;
        public String db_user;
        public String db_password;
        public String db_workspace;
        public String db_workspace_old_aami;
        public String db_workspace_predict;
        public String global_ftpPath;
        public String global_storePath;
        public String global_corrupted_file_dir_name;

        public GlobalConfig()
        {
            // 기본 workspace 설정
            switchWorkspace(WORKSPACE_TYPE.REMOTE);

            // ftp 경로 및 파일 저장 경로는 windows 와 unix를 따로 설정.
            if (!_isWindows())
            {
                global_ftpPath = _config.getString("global.ftpPath.unix");
                global_storePath = _config.getString("global.storePath.unix");
            }
            else
            {
                global_ftpPath = _config.getString("global.ftpPath.windows");
                global_storePath = _config.getString("global.storePath.windows");
            }

            // 깨진 파일 임시 저장할 디렉토리명 가져오기
            global_corrupted_file_dir_name = _config.getString("global.corruptedFileDirName");
        }

        private void switchWorkspace(WORKSPACE_TYPE workspaceType)
        {
            if (workspaceType == WORKSPACE_TYPE.REMOTE)
            {
                db_url = _config.getString("db.url");
                db_user = _config.getString("db.user");
                db_password = _config.getString("db.password");
                db_workspace = _config.getString("db.workspace");
                db_workspace_old_aami = _config.getString("db.workspace_old_aami");
                db_workspace_predict = _config.getString("db.workspace_predict");
            }
            else if (workspaceType == WORKSPACE_TYPE.LOCAL)
            {
                db_url = _config.getString("localtest.db.url");
                db_user = _config.getString("localtest.db.user");
                db_password = _config.getString("localtest.db.password");
                db_workspace = _config.getString("localtest.db.workspace");
                db_workspace_old_aami = _config.getString("localtest.db.workspace_old_aami");
                db_workspace_predict = _config.getString("localtest.db.workspace_predict");
            }
            else
            {
                throw new RuntimeException("Invalid workspace type.");
            }
        }
    }

    public DaemonSettings(Configuration config)
    {
        _init(config, false);
    }

    public DaemonSettings(String configFilePath)
    {
        _init(new File(configFilePath), false);
    }

    public DaemonSettings(String configFilePath, StartupInfo startInfo, boolean isLocalTest)
    {
        _init(new File(configFilePath), isLocalTest);
    }

    public DaemonSettings(File configFile, StartupInfo startInfo, boolean isLocalTest)
    {
        _init(configFile, isLocalTest);
    }

    public DaemonSettings(Configuration config, boolean isLocalTest)
    {
        _init(config, isLocalTest);
    }

    private void _init(File configFile, boolean isLocalTest)
    {
        try
        {
            Configurations configs = new Configurations();
            Configuration config = configs.properties(configFile);
            _init(config, isLocalTest);
        }
        catch (ConfigurationException ce)
        {
            throw new RuntimeException(ce);
        }
    }

    private void _init(Configuration config, boolean isLocalTest)
    {
        _config = config;
        _configPrefix = null;
        _globalConfig = new GlobalConfig();
    }

    private static boolean _isWindows()
    {
        if (_os_name == null)
        {
            _os_name = System.getProperty("os.name");
        }

        return _os_name.startsWith("Windows");
    }

    public void switchWorkspace(WORKSPACE_TYPE workspaceType)
    {
        _globalConfig.switchWorkspace(workspaceType);
    }

    public void setConfigPrefix(String configPrefix)
    {
        _configPrefix = configPrefix;
    }

    public String setConfigPrefix()
    {
        return _configPrefix;
    }

    public String DB_url()
    {
        return _globalConfig.db_url;
    }

    public String DB_user()
    {
        return _globalConfig.db_user;
    }

    public String DB_password()
    {
        return _globalConfig.db_password;
    }

    public String DB_workspace()
    {
        return _globalConfig.db_workspace;
    }

    public String DB_workspace_predict()
    {
        return _globalConfig.db_workspace_predict;
    }

    public String DB_workspace_old_aami()
    {
        return _globalConfig.db_workspace_old_aami;
    }

    public File FTPLocalPath()
    {
        return new File(_globalConfig.global_ftpPath);
    }

    public void setFTPLocalPath(String ftpPath)
    {
        _globalConfig.global_ftpPath = ftpPath;
    }

    public void setOutputRootPath(String outputRootPath)
    {
        _globalConfig.global_storePath = outputRootPath;
    }

    public String OutputRootPath()
    {
        return _globalConfig.global_storePath;
    }

    public String FileFilter()
    {
        return getCustomConfigValue("filter");
    }

    public String FileSavePath()
    {
        return getCustomConfigValue("path");
    }

    // 파일이 깨졌을 경우 저장할 디렉토리 (global.storePath + "Temp")
    public String FileCorruptedTempPath()
    {
        String corruptedFilePath = new File(_globalConfig.global_storePath, _globalConfig.global_corrupted_file_dir_name).getPath();

        return corruptedFilePath;
    }

    public String FileType()
    {
        return getCustomConfigValue("type");
    }
    
    public String getCoordinatesLatPath() {
    	return getCustomConfigValue("coordinates.lat.path");
    }
    
    public String getCoordinatesLonPath() {
    	return getCustomConfigValue("coordinates.lon.path");
    }

    private String getCustomConfigValue(String configSuffix)
    {
        String confName;
        String confValue;
        String[] nestedConfNames;
        Pattern pattern;
        Matcher matcher;

        if (_configPrefix == null)
        {
            throw new RuntimeException("Error : prefix is undefined in the Settings.");
        }

        confName = MessageFormat.format("{0}.{1}", _configPrefix, configSuffix);
        confValue = _config.getProperty(confName).toString();

        pattern = Pattern.compile("\\$\\{.*\\}");
        matcher = pattern.matcher(confValue);

        if (matcher.find())
        {
            nestedConfNames = getNestedConfigNames(confValue);

            if (nestedConfNames.length > 0)
            {
                for (int i = 0; i < nestedConfNames.length; i++)
                {
                    // Temporary hard coded for pre-sets
                    if (nestedConfNames[i].equals("global.storePath"))
                    {
                        confValue = ((String) confValue).replace("${global.storePath}", OutputRootPath());
                    }
                    else
                    {
                        confValue = ((String) confValue).replace(String.format("${%s}", nestedConfNames[i]), _config.getString(nestedConfNames[i]));
                    }
                }
            }
        }

        return confValue;
    }

    private String[] getNestedConfigNames(String line)
    {
        String[] result;

        result = getRegexResult("\\$\\{.*\\}", line);

        for (int i = 0; i < result.length; i++)
        {
            result[i] = result[i].substring(2, result[i].length() - 1);
        }

        return result;
    }

    private String[] getRegexResult(String regex, String line)
    {
        String[] result = null;
        int size = 0;

        // the pattern we want to search for
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(line);

        if (matcher.find())
        {
            size = matcher.groupCount() + 1;
            result = new String[size];

            for(int i = 0; i < size; i++)
            {
                result[i] = matcher.group(i);
            }
        }

        return result;
    }

    public static String getCurrentWorkingDirectory()
    {
        if (_currentWorkingDirectory == null)
        {
            _currentWorkingDirectory = new File("").getAbsolutePath();

            File userDir = new File(_currentWorkingDirectory);

            if (userDir.getName().equalsIgnoreCase("bin"))
            {
                userDir = userDir.getParentFile();
                _currentWorkingDirectory = userDir.getAbsolutePath();
            }
        }

        return _currentWorkingDirectory;
    }
}