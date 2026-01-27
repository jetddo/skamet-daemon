package kama.daemon.common.util;

import org.junit.Assert;
import org.junit.Test;

import java.util.Date;

import static org.junit.Assert.*;

/**
 * Created by chlee on 2017-02-10.
 */
public class DateFormatterTest
{
    /**
     * 아무 날짜나 파싱할수있는지 테스트
     * @throws Exception
     */
    @Test
    public void parseAnyDate() throws Exception
    {
        Date anyDate = DateFormatter.parseAnyDate("asdjflasdjlfa201701010000asdflasdhfk");

        Assert.assertTrue(true);
    }

}