package kama.daemon.main.test;

import kama.daemon.common.db.DataProcessor;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.common.util.JUnitTestUtil;
import kama.daemon.common.util.Log;
import kama.daemon.main.struct.StartupInfo;
import kama.daemon.model.observation.proc.AMDAR_DataProcess;
import org.apache.commons.configuration2.builder.ConfigurationBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.output.TeeOutputStream;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author chlee
 * Created on 2017-01-13.
 */
public class DataProcessorTest
{
    private enum PROCESSOR_TYPE { OBSERVATION, PREDICTION };

    //region "Data Processors"
    @Test
    public void AMDAR_DataProcessTest()
    {
        testDataProcessor("AMDAR", PROCESSOR_TYPE.OBSERVATION);
    }

    @Test
    public void AMOS_DataProcessTest()
    {
        testDataProcessor("AMOS", PROCESSOR_TYPE.OBSERVATION);
    }

    @Test
    public void ASOS_DataProcessTest()
    {
        testDataProcessor("ASOS", PROCESSOR_TYPE.OBSERVATION);
    }

    @Test
    public void AWOS_DataProcessTest()
    {
        testDataProcessor("AWOS", PROCESSOR_TYPE.OBSERVATION);
    }

    @Test
    public void AWS_DataProcessTest()
    {
        testDataProcessor("AWS", PROCESSOR_TYPE.OBSERVATION);
    }

    @Test
    public void BUOY_DataProcessTest()
    {
        testDataProcessor("BUOY", PROCESSOR_TYPE.OBSERVATION);
    }

    @Test
    public void CFQCZ_DataProcessTest()
    {
        testDataProcessor("CFQCZ", PROCESSOR_TYPE.OBSERVATION);
    }

    @Test
    public void CNQCZ_DataProcessTest()
    {
        testDataProcessor("CNQCZ", PROCESSOR_TYPE.OBSERVATION);
    }

    @Test
    public void coms_DataProcessTest()
    {
        testDataProcessor("coms", PROCESSOR_TYPE.OBSERVATION);
    }

    @Test
    public void COQVR_DataProcessTest()
    {
        testDataProcessor("COQVR", PROCESSOR_TYPE.OBSERVATION);
    }

    @Test
    public void hiway_DataProcessTest()
    {
        testDataProcessor("hiway", PROCESSOR_TYPE.OBSERVATION);
    }

    @Test
    public void HWPU_DataProcessTest()
    {
        testDataProcessor("HWPU", PROCESSOR_TYPE.OBSERVATION);
    }

    @Test
    public void LAU_DataProcessTest()
    {
        testDataProcessor("LAU", PROCESSOR_TYPE.OBSERVATION);
    }

    @Test
    public void LGT_DataProcessTest()
    {
        testDataProcessor("LGT", PROCESSOR_TYPE.OBSERVATION);
    }

    @Test
    public void LWPU_DataProcessTest()
    {
        testDataProcessor("LWPU", PROCESSOR_TYPE.OBSERVATION);
    }

    @Test
    public void NAVY_DataProcessTest()
    {
        testDataProcessor("NAVY", PROCESSOR_TYPE.OBSERVATION);
    }

    @Test
    public void SAKO82_DataProcessTest()
    {
        testDataProcessor("SAKO82", PROCESSOR_TYPE.OBSERVATION);
    }

    @Test
    public void UPP_DataProcessTest()
    {
        testDataProcessor("UPP", PROCESSOR_TYPE.OBSERVATION);
    }

    @Test
    public void WPF_DataProcessTest()
    {
        testDataProcessor("WPF", PROCESSOR_TYPE.OBSERVATION);
    }

    @Test
    public void LDAPS_ISOB_DataProcessTest()
    {
        testDataProcessor("LDAPS_ISOB", PROCESSOR_TYPE.PREDICTION);
    }

    @Test@Ignore
    public void LDAPS_SGL_DataProcessTest()
    {
        // 샘플 데이터 변경 예정 (2017/03/13)
        testDataProcessor("LDAPS_SGL", PROCESSOR_TYPE.PREDICTION);
    }

    @Test
    public void RDAPS_ISOB_DataProcessTest()
    {
        testDataProcessor("RDAPS_ISOB", PROCESSOR_TYPE.PREDICTION);
    }

    @Test
    public void RDAPS_SGL_DataProcessTest()
    {
        testDataProcessor("RDAPS_SGL", PROCESSOR_TYPE.PREDICTION);
    }

    @Test
    public void LFOG_DataProcessTest()
    {
        testDataProcessor("LFOG", PROCESSOR_TYPE.PREDICTION);
    }

    @Test@Ignore
    public void RFOG_DataProcessTest()
    {
        testDataProcessor("RFOG", PROCESSOR_TYPE.PREDICTION);
    }

    @Test
    public void UKMO_DataProcessTest()
    {
        testDataProcessor("UKMO", PROCESSOR_TYPE.PREDICTION);
    }

    @Test
    public void WAFC_DataProcessTest()
    {
        testDataProcessor("WAFC", PROCESSOR_TYPE.PREDICTION);
    }

    /**
     * 2017/02/02 추가 (AIREP 관측모델)
     */
    @Test
    public void AIREP_DataProcessTest()
    {
        testDataProcessor("AIREP", PROCESSOR_TYPE.OBSERVATION);
    }

    /**
     * 2017/02/15 추가 (3차원용 UM 예측모델)
     */
    @Test
    public void UM_LOA_DataProcessTest()
    {
        testDataProcessor("UM_LOA", PROCESSOR_TYPE.PREDICTION);
    }

    /**
     * 2017/02/15 추가 (3차원용 UM 예측모델)
     */
    @Test
    public void UM_REA_DataProcessTest()
    {
        testDataProcessor("UM_REA", PROCESSOR_TYPE.PREDICTION);
    }

    /**
     * 2017/02/15 추가 (3차원용 UM 예측모델)
     */
    @Test
    public void UM_VIS_DataProcessTest()
    {
        testDataProcessor("UM_VIS", PROCESSOR_TYPE.PREDICTION);
    }
    //endregion

    private void testDataProcessor(String classPrefix, PROCESSOR_TYPE processorType)
    {
        String consoleText;

        Log.print(String.format("JUnit : Initiating test for %s_DataProcess", classPrefix));

        try
        {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            TeeOutputStream dupStream = new TeeOutputStream(System.out, baos);

            PrintStream ps = new PrintStream(dupStream);

            PrintStream old = System.out;
            System.setOut(ps);

            // 테스트 개발중 (2017/01/13)
            DaemonSettings settings = new DaemonSettings(workingDir("conf/config.properties"));

            settings.setFTPLocalPath(JUnitTestUtil.tempFtpDir(""));
            settings.setOutputRootPath(JUnitTestUtil.tempFileStoreDir(""));

            // 리소스 파일 전부 복사
            copyResourceFiles(JUnitTestUtil.resourceTestDir(""), JUnitTestUtil.tempFtpDir(""));

            Class[] clsArgs;
            DataProcessor proc;

            clsArgs = new Class[1];
            clsArgs[0] = DaemonSettings.class;

            String procName;

            if (processorType == PROCESSOR_TYPE.OBSERVATION)
            {
                procName = String.format("kama.daemon.model.observation.proc.%s_DataProcess", classPrefix);
                proc = (DataProcessor) Class.forName(procName).getDeclaredConstructor(clsArgs).newInstance(settings);
            }
            else if (processorType == PROCESSOR_TYPE.PREDICTION)
            {
                procName = String.format("kama.daemon.model.prediction.proc.%s_DataProcess", classPrefix);
                proc = (DataProcessor) Class.forName(procName).getDeclaredConstructor(clsArgs).newInstance(settings);
            }
            else
            {
                throw new RuntimeException("Invalid data processor.");
            }

            String errorMsg;
            boolean testResult;
            errorMsg = String.format("Unable to complete %s", proc.getClass().getName());
            proc.processDataFile();

            baos.flush();
            consoleText = baos.toString();

            // 기존 콘솔 출력 돌려놓기
            System.out.flush();
            System.setOut(old);

            if (consoleText.isEmpty())
            {
                // 샘플 데이터 부재일 수 있음. 출력값 없음.
                Assert.fail("No output. There could be no sample data provided.");
            }

            Log.print("JUnit : Removing temporary file records on Oracle database.");
            // 테스트 후 DB에 저장된 무효 데이터파일 경로, 파일명 삭제
            proc.truncateWindowsFileInfo();

            // commit message 가 있음과 동시에 rollback message 가 없어야 test pass.
            Assert.assertTrue(errorMsg, consoleText.contains("INFO : DBManager.commit() complete.") &&
                        !consoleText.contains("INFO : DBManager.rollback() complete."));

            verifySQLException(consoleText);

            Log.print("JUnit : Test complete.");
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            Assert.fail();
        }
    }

    private void verifySQLException(String consoleText)
    {
        String text;

        // 예외 처리할 SQL Exception 을 console text에서 삭제
        text = consoleText.replace("SQLIntegrityConstraintViolationException: ORA-", "");

        Assert.assertTrue("Unhandled SQL exception detected", !text.contains("Exception: ORA-"));
    }

    private void copyResourceFiles(String storedPath, String newPath) throws IOException
    {
        File sourceDir = new File(storedPath);
        File destDir = new File(newPath);

        if(sourceDir.isDirectory())
        {
            File[] content = sourceDir.listFiles();

            for (File file : content)
            {
                File destFile;

                destFile = new File(String.format("%s/%s", destDir.getAbsolutePath(), file.getName()));

                if (!file.isDirectory())
                {
                    if (!destFile.exists() || (file.length() != destFile.length()))
                    {
                        //destFile.delete();
                        FileUtils.copyFileToDirectory(file, destDir);
                    }
                }
                else if (file.isDirectory() && file.getName().matches("[0-9]{0,}")) // 날짜정보를 포함한 디렉토리도 테스트 대상이므로 복사 필요
                {
                    File newChildDirectory;
                    newChildDirectory = new File(String.format("%s/%s", newPath, file.getName()));

                    FileUtils.forceMkdir(newChildDirectory);
                    copyResourceFiles(file.getAbsolutePath(), newChildDirectory.getAbsolutePath());
                }
            }
        }
    }

    /**
     * 현재 작업 폴더 가져오기
     * @param path 상대경로
     * @return 작업폴더를 포함한 절대 경로
     */
    private String workingDir(String path)
    {
        return String.format("%s/%s", DaemonSettings.getCurrentWorkingDirectory(), path);
    }
}