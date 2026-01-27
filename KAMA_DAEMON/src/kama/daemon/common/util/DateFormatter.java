package kama.daemon.common.util;

import org.apache.commons.lang3.time.DateUtils;
import ucar.nc2.util.xml.Parse;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author chlee
 * Created on 2016-11-22.
 * 날짜 관련 함수 (Date format 정리 등)
 */
public class DateFormatter
{
    private DateFormatter()
    {

    }

    /**
     * 복잡한 텍스트 안에서 정규식 사용하여 날짜 파싱
     * @param text 파싱할 텍스트
     * @return 파싱된 날짜
     * @throws ParseException
     */
    public static Date parseAnyDate(String text) throws ParseException
    {
        // 날짜 포맷을 가장 길게 설정하고, 실제 파일 포맷에 맞게 뒷부분 잘라서 사용
        String defaultDateFormat = "yyyyMMddHHmmssSS";

        // 가장 긴 포맷부터 하나씩 짤라서 확인
        for (int dateLength = 16; dateLength >= 8; dateLength--)
        {
            String dateFormat = defaultDateFormat.substring(0, dateLength);
            String regEx = String.format("(.*)[0-9]{%d}(.*)", dateLength);
            Pattern ptnSplit = Pattern.compile(regEx);
            Matcher m = ptnSplit.matcher(text);

            // 패턴 match 할 경우
            if (m.matches())
            {
                String regExSplit = String.format("[0-9]{%d}", dateLength);
                Pattern p = Pattern.compile(regExSplit);
                String[] ops = p.split(text);

                String result = text;

                // 정규식에 match 된 문자열을 제외한 문자열 전부 삭제
                for (int i = 0; i < ops.length; i++)
                {
                    result = result.replaceAll(Pattern.quote(ops[i]), "");
                }

                return DateFormatter.parseDate(result, dateFormat);
            }
        }

        // 날짜 파싱 불가능할시 ParseException throw
        throw new ParseException(String.format("Unable to parse date: %s", text), 0);
    }

    /**
     * 특정 포맷으로부터 날짜 파싱해주는 함수
     * @param text 날짜 string
     * @param format 포맷 (e.g., yyyyMMdd, yyyy-MM-dd HH:mm:ss...)
     * @return 파싱된 Date 클래스
     * @throws ParseException
     */
    public static Date parseDate(String text, String format) throws ParseException
    {
        SimpleDateFormat sdf;

        sdf = new SimpleDateFormat(format);

        return sdf.parse(text);
    }

    /**
     * 파일명으로부터 날짜 정보 파싱하는 함수
     * @param text 파싱할 텍스트
     * @param format 파일명에 입력되어 있는 날짜 포맷
     * @param fileDateIndex 날짜값이 있는 위치 인덱스
     * @return 파싱된 날짜
     * @throws ParseException
     */
    public static Date parseDateFromString(String text, String format, int fileDateIndex) throws ParseException
    {
        Date dtFileDate;
        String[] fileNameToken;
        String dateFromFileName;

        fileNameToken = text.split("_");
        dateFromFileName = fileNameToken[fileDateIndex];

        // 파일 확장자 삭제
        if (dateFromFileName.contains("."))
        {
            dateFromFileName = dateFromFileName.substring(0, dateFromFileName.indexOf('.'));
        }

        // 파일 포맷보다 실제 파일명에 표기된 날짜값이 길 경우, 표기된 날짜 값의 뒷부분 제거
        if (dateFromFileName.length() > format.length())
        {
            dateFromFileName = dateFromFileName.substring(0, format.length());
        }

        try
        {
            // 날짜 파싱 (포맷이 길 경우, 포맷을 실제 날짜값에 맞게 뒷부분 제거)
            dtFileDate = DateFormatter.parseDate(dateFromFileName, format.substring(0, dateFromFileName.length()));
        }
        catch (ParseException pe)
        {
            try
            {
                // 파싱 오류 발생시에, 정규식 사용하여 파일명으로부터 파싱 시도.
                dtFileDate = DateFormatter.parseAnyDate(text);
            }
            catch (ParseException pe2)
            {
                // 파싱 오류 재발생시, 기존 ParseException throw 할 것.
                throw pe;
            }
        }

        return dtFileDate;
    }

    /**
     * 특정 날짜 포맷으로부터 다른 포맷으로 변경해주는 함수
     * @param text1 기존 날짜 문자열
     * @param format1 기존 날짜 포맷 (e.g., yyyyMMdd, yyyy-MM-dd HH:mm:ss)
     * @param format2 변경할 날짜 포맷 (e.g., yyyyMMdd, yyyy-MM-dd HH:mm:ss)
     * @return 변경된 날짜 포맷 문자열
     * @throws ParseException
     */
    public static String changeFormat(String text1, String format1, String format2) throws ParseException
    {
        SimpleDateFormat sdf1;
        SimpleDateFormat sdf2;
        Date tempDate;

        sdf1 = new SimpleDateFormat(format1);
        sdf2 = new SimpleDateFormat(format2);

        tempDate = sdf1.parse(text1);

        return sdf2.format(tempDate);
    }

    /**
     * 날짜를 특정 포맷으로 만들어 string으로 반환.
     * @param date 날짜
     * @param format 문자열 날짜 포맷 (e.g., yyyyMMdd, yyyy-MM-dd HH:mm:ss)
     * @return 날짜 포맷 문자열
     */
    public static String formatDate(Date date, String format)
    {
        SimpleDateFormat sdf;

        sdf = new SimpleDateFormat(format);

        return sdf.format(date);
    }

    /**
     * KST => UTC 로 변경해주는 함수 (사용하지 않음. 추후 삭제 예정)
     * Java에서 내장된 timezone과는 별개로 작동.
     * 작성일: 2017/01/26
     * @param date KST 날짜
     * @return
     */
    public static Date getUTCFromKST(Date date)
    {
        Date copyDate;

        // UTC 표현을 위해 그냥 9시간 빼주었음.
        // (TimeZone.setDefault 설정 건드리는것보다 낫다고 판단...)
        copyDate = (Date)date.clone();
        copyDate = DateUtils.addHours(copyDate, -9);

        return copyDate;
    }

    /**
     * add days to date in java
     * @param date
     * @param days
     * @return
     */
    public static Date addDays(Date date, int days) {
        GregorianCalendar cal = new GregorianCalendar();
        cal.setTime(date);
        cal.add(Calendar.DATE, days);

        return cal.getTime();
    }

    /**
     * subtract days to date in java
     * @param date
     * @param days
     * @return
     */
    public static Date subtractDays(Date date, int days) {
        GregorianCalendar cal = new GregorianCalendar();
        cal.setTime(date);
        cal.add(Calendar.DATE, -days);

        return cal.getTime();
    }
}