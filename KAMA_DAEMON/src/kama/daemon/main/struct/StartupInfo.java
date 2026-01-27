package kama.daemon.main.struct;

/**
 * Created by chlee on 2017-01-26.
 */

import kama.daemon.common.db.DataProcessor;

import java.util.regex.Pattern;

/**
 * Command line argument를 파싱하기 위한 class
 */
public class StartupInfo
{
    public enum MODEL_TYPE { OBSERVATION, PREDICTION, ASSESSMENT };

    public String ClassPrefix;
    public MODEL_TYPE ModelType;

    public StartupInfo(Class classTemplate)
    {
        generateStartupInfo(classTemplate.getName());
    }

    public StartupInfo(String... args)
    {
        if (args.length > 1)
        {
            switch (args[0].toUpperCase())
            {
                case "OBSERVATION":
                    ModelType = MODEL_TYPE.OBSERVATION;
                    break;
                case "PREDICTION":
                    ModelType = MODEL_TYPE.PREDICTION;
                    break;
                case "ASSESSMENT":
                    ModelType = MODEL_TYPE.ASSESSMENT;
                    break;
                default:
                    ModelType = MODEL_TYPE.OBSERVATION;
                    break;
            }

            ClassPrefix = args[1].trim();
        }
    }

    /**
     * 테스트용으로 사용시에, command line argument 를 직접 추출하도록 하는 생성자
     * @param dataProcessor 작업할 data processor
     */
    public void generateStartupInfo(DataProcessor dataProcessor)
    {
        String className;
        className = dataProcessor.getClass().getName();

        generateStartupInfo(className);
    }

    /**
     * 테스트용으로 사용시에, command line argument 를 직접 추출하도록 하는 생성자
     * @param className 작업할 data processor 클래스명 (package 명 포함)
     */
    public void generateStartupInfo(String className)
    {

    }
}