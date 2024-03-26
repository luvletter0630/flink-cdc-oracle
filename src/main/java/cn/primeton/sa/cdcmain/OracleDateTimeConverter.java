package cn.primeton.sa.cdcmain;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import lombok.extern.slf4j.Slf4j;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.SchemaBuilder;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Consumer;

/**
 * @author liwj
 * @date 2024/3/25 18:01
 * @description:
 */
@Slf4j
public class OracleDateTimeConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {

    private DateTimeFormatter dateFormatter = DateTimeFormatter.ISO_DATE;
    private DateTimeFormatter timeFormatter = DateTimeFormatter.ISO_TIME;
    private DateTimeFormatter datetimeFormatter = DateTimeFormatter.ISO_DATE_TIME;
    private DateTimeFormatter timestampFormatter = DateTimeFormatter.ISO_DATE_TIME;

    private ZoneId timestampZoneId = ZoneId.systemDefault();
    private static final String DEFAULT_DATE = "yyyy-MM-dd";
    private static final String DEFAULT_TIME = "HH:mm:ss";
    private static final String DEFAULT_DATETIME = "yyyy-MM-dd HH:mm:ss";
    private static final String DEFAULT_TIMESTAMP_ZONE = "UTC+8";

    @Override
    public void configure(Properties props) {
        readProps(props, "format.date", p -> dateFormatter = DateTimeFormatter.ofPattern(p));
        readProps(props, "format.time", p -> timeFormatter = DateTimeFormatter.ofPattern(p));
        readProps(props, "format.datetime", p -> datetimeFormatter = DateTimeFormatter.ofPattern(p));
        readProps(props, "format.timestamp", p -> timestampFormatter = DateTimeFormatter.ofPattern(p));
        readProps(props, "format.timestamp.zone", z -> timestampZoneId = ZoneId.of(z));
    }

    private void readProps(Properties properties, String settingKey, Consumer<String> callback) {
        String settingValue = (String) properties.get(settingKey);
        if (settingValue == null || settingValue.length() == 0) {
            if (settingKey.equals("format.date")) {
                settingValue = DEFAULT_DATE;
            } else if (settingKey.equals("format.time")) {
                settingValue = DEFAULT_TIME;
            } else if (settingKey.equals("format.datetime")) {
                settingValue = DEFAULT_DATETIME;
            } else if (settingKey.equals("format.timestamp")) {
                settingValue = DEFAULT_DATETIME;
            } else if (settingKey.equals("format.timestamp.zone")) {
                settingValue = DEFAULT_TIMESTAMP_ZONE;
            } else {
                return;
            }
        }
        try {
            callback.accept(settingValue.trim());
        } catch (IllegalArgumentException | DateTimeException e) {
            log.error("MySqlDateTimeConverter The \"{}\" setting is illegal:{}", settingKey, settingValue);
            throw e;
        }
    }

    @Override
    public void converterFor(RelationalColumn column, CustomConverter.ConverterRegistration<SchemaBuilder> registration) {
        String sqlType = column.typeName();
        SchemaBuilder schemaBuilder = null;
        CustomConverter.Converter converter = null;
        if ("DATE".equalsIgnoreCase(sqlType)) {
            schemaBuilder = SchemaBuilder.string().optional().name("com.darcytech.debezium.date.string");
            converter = this::convertDate;
        }
        if ("TIME".equalsIgnoreCase(sqlType)) {
            schemaBuilder = SchemaBuilder.string().optional().name("com.darcytech.debezium.time.string");
            converter = this::convertTime;
        }
        if ("DATETIME".equalsIgnoreCase(sqlType)) {
            schemaBuilder = SchemaBuilder.string().optional().name("com.darcytech.debezium.datetime.string");
            converter = this::convertDateTime;
        }
        if ("TIMESTAMP".equalsIgnoreCase(sqlType)) {
            schemaBuilder = SchemaBuilder.string().optional().name("com.darcytech.debezium.timestamp.string");
            converter = this::convertTimestamp;
        }
        if (schemaBuilder != null) {
            registration.register(schemaBuilder, converter);
            log.info("MySqlDateTimeConverter register converter for sqlType {} to schema {}", sqlType, schemaBuilder.name());
        }
    }

    private String convertDate(Object input) {
        if (Objects.isNull(input)) {
            return null;
        }
        if (input instanceof LocalDate) {
            return dateFormatter.format((LocalDate) input);
        }
        if (input instanceof Integer) {
            LocalDate date = LocalDate.ofEpochDay((Integer) input);
            return dateFormatter.format(date);
        }
        log.error("MySqlDateTimeConverter convertDate is fail getClass:{}、value:{}", input.getClass(), input.toString());
        return Optional.ofNullable(input).map(p -> p.toString()).orElse(null);
    }

    private String convertTime(Object input) {
        if (Objects.isNull(input)) {
            return null;
        }
        if (input instanceof Duration) {
            Duration duration = (Duration) input;
            long seconds = duration.getSeconds();
            int nano = duration.getNano();
            LocalTime time = LocalTime.ofSecondOfDay(seconds).withNano(nano);
            return timeFormatter.format(time);
        }
        log.error("MySqlDateTimeConverter convertTime is fail getClass:{}、value:{}", input.getClass(), input.toString());
        return Optional.ofNullable(input).map(p -> p.toString()).orElse(null);
    }

    private String convertDateTime(Object input) {
        if (Objects.isNull(input)) {
            return null;
        }
        if (input instanceof LocalDateTime) {
            return datetimeFormatter.format((LocalDateTime) input);
        }
        if (input instanceof java.sql.Timestamp) {
            Date date = (java.sql.Timestamp) input;
            LocalDateTime localDateTime = dateToLocalDateTime(date);
            return datetimeFormatter.format(localDateTime);
        }
        log.error("MySqlDateTimeConverter convertDateTime is fail getClass:{}、value:{}", input.getClass(), input.toString());
        return Optional.ofNullable(input).map(p -> p.toString()).orElse(null);
    }

    private String convertTimestamp(Object input) {
        if (Objects.isNull(input)) {
            return null;
        }
        if (input instanceof ZonedDateTime) {
            // mysql的timestamp会转成UTC存储，这里的zonedDatetime都是UTC时间
            ZonedDateTime zonedDateTime = (ZonedDateTime) input;
            LocalDateTime localDateTime = zonedDateTime.withZoneSameInstant(timestampZoneId).toLocalDateTime();
            return timestampFormatter.format(localDateTime);
        }
        log.error("MySqlDateTimeConverter convertTimestamp is fail getClass:{}、value:{}", input.getClass(), input.toString());
        return Optional.ofNullable(input).map(p -> p.toString()).orElse(null);
    }

    /**
     * 将 Date 转为 LocalDateTime
     *
     * @param date
     * @return java.time.LocalDateTime;
     */
    public LocalDateTime dateToLocalDateTime(Date date) {
        return date.toInstant().atZone(timestampZoneId).toLocalDateTime();
    }
}
