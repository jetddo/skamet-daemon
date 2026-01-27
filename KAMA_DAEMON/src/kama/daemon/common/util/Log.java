package kama.daemon.common.util;

import java.text.MessageFormat;
import java.util.EnumSet;

/**
 * @author chlee
 * Created on 2016-11-22.
 * 로그처리 관련 클래스
 */
public class Log
{
    public enum LOG_TYPE { DEFAULT, RUNTIME_EXCEPTION, DEBUG };

    private static EnumSet<LOG_TYPE> _logToPrint = EnumSet.of(LOG_TYPE.DEFAULT, LOG_TYPE.RUNTIME_EXCEPTION); //EnumSet.allOf(LOG_TYPE.class);

    public static void setupLogDisplay(EnumSet<LOG_TYPE> logToPrint)
    {
        _logToPrint = logToPrint;
    }

    public static void print(String format, Object... args)
    {
        print(LOG_TYPE.DEFAULT, format, args);
    }

    public static void print(LOG_TYPE logType, String format, Object... args)
    {
        // 설정한 로그 정보만 출력
        if (_logToPrint.contains(logType))
        {
            System.out.println(MessageFormat.format(format, args));
        }
    }

    public static void printStackTrace(Exception ex)
    {
        // 설정한 로그 정보만 출력
        if (_logToPrint.contains(LOG_TYPE.RUNTIME_EXCEPTION))
        {
            ex.printStackTrace(System.out);
        }
    }
}