package kama.daemon.common.util;

import java.io.File;

/**
 * @author chlee
 * Created on 2017-01-19.
 * JUnit Test에서 필요한 정보 (테스트용 리소스 파일 경로 등)을 받아올 수 있도록 하는 클래스
 */
public class JUnitTestUtil
{
    /**
     * 현재 작업 폴더 가져오기
     * @param path 상대경로
     * @return 작업폴더를 포함한 절대 경로
     */
    public static String resourceTestDir(String path)
    {
        return String.format("%s/res/test/%s", DaemonSettings.getCurrentWorkingDirectory(), path);
    }

    /**
     * 임시 작업 폴더 가져오기
     * @param path 상대경로
     * @return 임시 작업 폴더를 포함한 절대 경로
     */
    public static String tempOutputDir(String path)
    {
        String strPath = String.format("%s/res/test/temp", DaemonSettings.getCurrentWorkingDirectory(), path);

        File tempDir = new File(strPath);

        if (!tempDir.exists())
        {
            tempDir.mkdirs();
        }

        return makePath(String.format("%s/%s", strPath, path));
    }

    /**
     * 테스트용 FTP 데이터 파일 저장경로
     * @param path 상대경로
     * @return 임시 작업 폴더를 포함한 절대 경로
     */
    public static String tempFtpDir(String path)
    {
        String strPath = String.format("%s/res/test/ftpdata", DaemonSettings.getCurrentWorkingDirectory(), path);

        File tempDir = new File(strPath);

        if (!tempDir.exists())
        {
            tempDir.mkdirs();
        }

        return makePath(String.format("%s/%s", strPath, path));
    }

    /**
     * 테스트용 데이터 파일 분류 경로
     * @param path 상대경로
     * @return 임시 작업 폴더를 포함한 절대 경로
     */
    public static String tempFileStoreDir(String path)
    {
        String strPath = String.format("%s/res/test/datastore", DaemonSettings.getCurrentWorkingDirectory(), path);

        File tempDir = new File(strPath);

        if (!tempDir.exists())
        {
            tempDir.mkdirs();
        }

        return makePath(String.format("%s/%s", strPath, path));
    }

    /**
     * 경로 정리 후 리턴
     * @param path 문자열로 된 경로
     * @return 정리된 경로
     */
    private static String makePath(String path)
    {
        return new File(path).getAbsolutePath();
    }
}