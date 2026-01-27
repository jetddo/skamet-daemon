package kama.daemon.model.observation.adopt.RDR.data;

/**
 * @author chlee
 * Created on 2016-12-16.
 * xy격자와 경위도 정보를 동시에 포함하는 클래스 (기존 c 코드와의 호환성을 위해 만든 클래스)
 */
public class GridInfo
{
    public float lon;
    public float lat;
    public float x;
    public float y;
}