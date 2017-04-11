package com.yiguan.kafka.service;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TimeService {

    private long currentTimeHourLowerLimit = 0L;
    private long currentTimeHourUpperLimit = 0L;
    private long currentTimeDayLowerLimit = 0L;
    private long currentTimeDayUpperLimit = 0L;
    private String currentTimeYYYYMMDD = null;
    private Long endTime = 1462809600000L;// "2016-5-10 0:0:0"

    public Long getEndTime() {
        return endTime;
    }

    public long getCurrentTimeDayLowerLimit() {
        return currentTimeDayLowerLimit;
    }

    public long getCurrentTimeDayUpperLimit() {
        return currentTimeDayUpperLimit;
    }

    private static TimeService instance = new TimeService();

    public static TimeService getInstance() {
        return instance;
    }

    public long getCurrentTimeHourLowerLimit() {
        return currentTimeHourLowerLimit;
    }

    public long getCurrentTimeHourUpperLimit() {
        return currentTimeHourUpperLimit;
    }

    public String getCurrentTimeYYYYMMDDHHMMSS() {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return format.format(new Date());
    }

    private TimeService() {
        resetHourLimits();
        resetDayLimits();

    }

    public void resetHourLimits() {
        try {
            currentTimeHourLowerLimit = getDate(System.currentTimeMillis(), "yyyy-MM-dd HH ").getTime();
            currentTimeHourUpperLimit = getDate(addHour(System.currentTimeMillis(), 1).getTime(), "yyyy-MM-dd HH ").getTime();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    public void resetDayLimits() {
        try {
            currentTimeDayLowerLimit = getDate(System.currentTimeMillis(), "yyyy-MM-dd").getTime();
            currentTimeDayUpperLimit = getDate(addHour(System.currentTimeMillis(), 24).getTime(), "yyyy-MM-dd").getTime();

            innerGetCurrentTimeYYYYMMDD();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    private void innerGetCurrentTimeYYYYMMDD() {
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        currentTimeYYYYMMDD = format.format(new Date(currentTimeDayLowerLimit));
    }

    public String getCurrentTimeYYYYMMDD() {
        if (System.currentTimeMillis() > currentTimeDayUpperLimit) {
            synchronized (instance) {
                resetDayLimits();
            }
        }
        return currentTimeYYYYMMDD;
    }

    public Date getNow() {
        return new Date();
    }

    public Date getDate(long timestamp, String timeformat) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat(timeformat);
        String dateStr = format.format(new Date(timestamp));
        return format.parse(dateStr);
    }

    public Date addHour(long timestamp, int hour) {
        return new Date(timestamp + hour * 3600 * 1000);
    }
}
