package kama.daemon.model.observation.adopt.AIREP;

import kama.daemon.common.util.JUnitTestUtil;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;

/**
 * @author chlee
 * Created on 2017-02-02.
 */
public class AIREP_DataTest
{
    @Test
    public void loadAIREPData() throws Exception
    {
        AIREP_Data data;
        data = AIREP_Data.loadAIREPData(new File(JUnitTestUtil.resourceTestDir("AIREP_RKSI_20170126135838")));
    }

}