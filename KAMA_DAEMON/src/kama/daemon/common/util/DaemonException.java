package kama.daemon.common.util;

/**
 * @author chlee
 * Created on 2016-11-25.
 * 데몬에서 오류 발생시 throw 할 런타임 예외 클래스
 * (예외 발생시 로그 출력 관련 관여함.)
 */
public class DaemonException extends RuntimeException
{
    private static final boolean PRINT_STACKTRACE = false;

    public DaemonException()
    {
        super();
    }

    public DaemonException(String message)
    {
        super(message);
        _initialize(message, this, true);
    }

    public DaemonException(String message, boolean printStackTrace)
    {
        super(message);
        _initialize(message, this, printStackTrace);
    }

    public DaemonException(String message, Exception ex, boolean printStackTrace)
    {
        super(message, ex);
        _initialize(message, ex, printStackTrace);
    }

    public DaemonException(String message, Exception ex)
    {
        super(message, ex);
        _initialize(message, ex, true);
    }

    private void _initialize(String message, Exception ex, boolean printStackTrace)
    {
        Log.print(Log.LOG_TYPE.RUNTIME_EXCEPTION, message);
        Log.printStackTrace(ex);

        // 안정화 이후 삭제 예정 (2017/01/11)
        if (PRINT_STACKTRACE && printStackTrace)
        {
            ex.printStackTrace(System.out);
        }
    }
}