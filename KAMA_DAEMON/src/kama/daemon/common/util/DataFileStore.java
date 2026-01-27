package kama.daemon.common.util;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.RegexFileFilter;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author jetddo
 * @author chlee
 */
public class DataFileStore
{
    /**
     * 디렉토리 내에 있는 파일들을 전부 list 하기 위한 매서드
     * @param directory 디렉토리
     * @param regEx 패턴에 매치되는 파일 전부 list up.
     * @return 패턴에 매치되는 파일 배열
     */
    public static File[] listAllFilesRecursively(File directory, Pattern regEx)
    {
        List<File> fileList;
        File[] files;
        fileList = new ArrayList<>();

        listAllFilesRecursivelyInternal(directory, fileList, regEx);
        files = new File[fileList.size()];

        return fileList.toArray(files);
    }

    /**
     * listAllFilesRecursively의 내부함수
     * @param directory 디렉토리
     * @param regEx 패턴에 매치되는 파일 전부 list up.
     * @return 패턴에 매치되는 파일 배열
     */
    private static void listAllFilesRecursivelyInternal(File directory, List<File> fileList, Pattern regEx)
    {
        File[] files = directory.listFiles();

        for (File file : files)
        {
            if (file.isDirectory())
            {
                listAllFilesRecursivelyInternal(file, fileList, regEx);
            }
            else
            {
                Matcher m = regEx.matcher(file.getName());
                if (m.matches())
                {
                    fileList.add(file);
                }
            }
        }
    }

    /**
     * 비어있는 디렉토리 삭제하는 매서드
     * @param directory 루트 디렉토리
     */
    public static void removeEmptyDirectoriesRecursively(File directory)
    {
        File[] files = directory.listFiles();

        // 디렉토리 안에 디렉토리 있는지 확인
        for (File file : files)
        {
            if (file.isDirectory())
            {
                removeEmptyDirectoriesRecursivelyInternal(file);
            }
        }
    }

    /**
     * 비어있는 디렉토리 삭제하는 내부 매서드
     * @param directory 확인할 디렉토리
     */
    private static void removeEmptyDirectoriesRecursivelyInternal(File directory)
    {
        File[] files = directory.listFiles();

        // 디렉토리 안에 디렉토리 있는지 확인
        for (File file : files)
        {
            if (file.isDirectory())
            {
                removeEmptyDirectoriesRecursivelyInternal(file);
            }
        }

        // 디렉토리가 비어있는 경우 삭제
        if (directory.list().length == 0)
        {
            directory.delete();
        }
    }

    /**
     * 데이터 파일 처리 후 파일 분류하여 특정 폴더로 옮기는 함수
     * @param f 분류/저장할 데이터 파일
     * @param storePath 데이터 파일 저장 루트 경로
     * @return 파일 이동 성공시 true
     */
    public static boolean storeDateFile(File f, String storePath)
    {
        File saveFolder = new File(storePath);

        if (!saveFolder.exists() || saveFolder.isFile())
        {
            saveFolder.mkdirs();
        }

        try
        {
            File dest = new File(storePath + "/" + f.getName());

            // 같은 이름의 파일이 있을 경우 지움
            if (dest.exists() && dest.isFile())
            {
                dest.delete();
            }

            try
            {
                FileUtils.moveFile(f, new File(storePath + "/" + f.getName()));
            }
            catch (IOException ex)
            {
                FileUtils.copyFile(f, new File(storePath + "/" + f.getName()));
                f.deleteOnExit();
            }
        }
        catch (IOException e)
        {
            Log.print("Error : storeDataFile.storeDataFile -> " + e);
            return false;
        }

        return true;
    }

    /**
     * 여러개의 파일을 한꺼번에 옮기고, 성공여부 판별하는 함수
     * (추후 최적화 필요할 수도있음. 3차원 nc 데이터 여러개 복사할때 필요하여 구현하였음.)
     * 2017/02/15
     * @param files 분류/저장할 데이터 파일들
     * @param storePath 데이터 파일 저장 루트 경로
     * @return 파일 이동 성공시 true
     */
    public static boolean storeDateFiles(File[] files, String storePath)
    {
        boolean rtnBool;

        rtnBool = true;

        for (File f : files)
        {
            rtnBool &= storeDateFile(f, storePath);
        }

        return rtnBool;
    }
}