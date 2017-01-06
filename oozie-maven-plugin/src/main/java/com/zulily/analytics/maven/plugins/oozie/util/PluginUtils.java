package com.zulily.analytics.maven.plugins.oozie.util;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;

public class PluginUtils {

    public static DateTime parseDateTime(String startDate, String startHour) {
        if (startDate == null || startHour == null)
            return DateTime.parse(DateTime.now(DateTimeZone.UTC).toString("YYYY-MM-dd") + "T01:05Z");
        return DateTime.parse(startDate + "T" + startHour + "Z");
    }

    public static DateTime addDuration(DateTime startTime, String duration) {
        return startTime.plus(Period.parse(duration));
    }
}
