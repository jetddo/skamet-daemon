package kama.daemon.main;

import java.io.File;
import java.util.Date;
import java.util.EnumSet;
import java.util.TimeZone;

import kama.daemon.common.db.DataProcessor;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.common.util.DaemonUtils;
import kama.daemon.common.util.DateFormatter;
import kama.daemon.common.util.Log;
import kama.daemon.main.struct.StartupInfo;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;

/**
 * @author chlee
 * Created on 2016-11-25.
 */
public class DaemonMain
{
    // 로그 레벨 설정 (현재 전체 출력)
    // 설정 예시: LOG_LEVEL = EnumSet.of(Log.LOG_TYPE.DEFAULT, Log.LOG_TYPE.RUNTIME_EXCEPTION)
    private static final EnumSet<Log.LOG_TYPE> LOG_LEVEL = EnumSet.allOf(Log.LOG_TYPE.class);

    // 로컬 테스트용 변수 (command line argument 없이 테스트하기 위한 용도)
    private static final String DEFAULT_CLS_TYPE = "OBSERVATION"; // 관측: OBSERVATION, 예측: PREDICTION
    private static final String DEFAULT_CLS_PREFIX = "LGT"; // 클레스 prefix (e.g., AMOS, AMDAR, UPP...)
    private static final boolean IS_LOCAL_TEST = false; // 로컬 DB 에서 테스트할 경우 true

    public static void main(String[] args)
    {
        // 강제로 TimeZone 서울 표준시로 맞춤
        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Seoul"));

        Configurations configs = new Configurations();
        Date start_time, end_time;

        // 로그 레벨 설정
        Log.setupLogDisplay(LOG_LEVEL);

        // 시작 시간 기록
        start_time = new Date(System.currentTimeMillis());
        Log.print("{0} -> START", DateFormatter.formatDate(start_time, "yyyy-MM-dd HH:mm:ss.SSS"));

        try
        {
            Configuration config;
            DataProcessor proc;
            String procName;
            StartupInfo startupInfo;

            // Get command line argument
            if (args.length >= 2)
            {
                startupInfo = new StartupInfo(args);
            }
            else
            {
                startupInfo = new StartupInfo(DEFAULT_CLS_TYPE, DEFAULT_CLS_PREFIX);
            }

            config = configs.properties(new File(DaemonUtils.getConfigFilePath()));

            if (startupInfo.ModelType == StartupInfo.MODEL_TYPE.OBSERVATION)
            {
                procName = String.format("kama.daemon.model.observation.proc.%s_DataProcess", startupInfo.ClassPrefix);
            }
            else if (startupInfo.ModelType == StartupInfo.MODEL_TYPE.PREDICTION)
            {
                procName = String.format("kama.daemon.model.prediction.proc.%s_DataProcess", startupInfo.ClassPrefix);
            }
            else if (startupInfo.ModelType == StartupInfo.MODEL_TYPE.ASSESSMENT)
            {
                procName = String.format("kama.daemon.model.assessment.proc.%s_DataProcess", startupInfo.ClassPrefix);
            }
            else
            {
                throw new RuntimeException("Error : Undefined model type.");
            }

            // If class is not found
            if (!isClassExists(procName))
            {
                throw new RuntimeException(String.format("Error : Unable to find the specified class %s", procName));
            }

            proc = null;

            try
            {
                Class[] clsArgs;

                clsArgs = new Class[1];
                clsArgs[0] = DaemonSettings.class;

                proc = (DataProcessor)Class.forName(procName).getDeclaredConstructor(clsArgs).newInstance(new DaemonSettings(config, IS_LOCAL_TEST));
            }
            catch(Exception e)
            {
                Log.print(String.format("Error : unable to instantiate class %s -> %s", procName, e.toString()));
                System.exit(1);
            }

            if (proc != null)
            {
                proc.processDataFile();
            }
        }
        catch(ConfigurationException e)
        {
            Log.print("Error : DaemonMain.main -> "+e);
        }

        // 종료 시간 기록
        end_time = new Date(System.currentTimeMillis());
        Log.print("{0} -> END (Elapsed Time: {1,number,0.###} sec)",
                DateFormatter.formatDate(end_time, "yyyy-MM-dd HH:mm:ss.SSS"),
                (float) (end_time.getTime() - start_time.getTime()) / 1000.0);
    }

    public static boolean isClassExists(String className)
    {
        try
        {
            Class.forName(className);
        }
        catch (ClassNotFoundException ignored)
        {
            return false;
        }

        return true;
    }
}