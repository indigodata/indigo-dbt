{% macro func_geoip2_country() -%}
    create or replace function geoip2_country(ip String)
    returns string
    language java
    handler = 'X.x'
    imports = ('@warehouse_db.public.geoip_jars/geoip2-4.0.1.jar'
            , '@warehouse_db.public.geoip_jars/maxmind-db-3.0.0.jar'
            , '@warehouse_db.public.geoip_jars/jackson-annotations-2.14.2.jar'
            , '@warehouse_db.public.geoip_jars/jackson-core-2.14.2.jar'
            , '@warehouse_db.public.geoip_jars/jackson-databind-2.14.2.jar')
    as $$
    import java.io.File;
    import java.net.InetAddress;

    import com.snowflake.snowpark_java.types.SnowflakeFile;
    import com.maxmind.geoip2.model.*;
    import com.maxmind.geoip2.DatabaseReader;
    import com.maxmind.geoip2.exception.AddressNotFoundException;

    class X {
        DatabaseReader _reader;
        
        public String x(String ip) throws Exception {
            if (null == _reader) {
                // lazy initialization
                _reader = new DatabaseReader.Builder(SnowflakeFile.newInstance("@warehouse_db.public.geoip_jars/GeoLite2-Country.mmdb", false).getInputStream()).build();
            }
            try {
                return _reader.country(InetAddress.getByName(ip)).getCountry().getIsoCode();
            } catch (AddressNotFoundException e) {
                return null;
            }
        }
    }
    $$;
{%- endmacro %}

{% macro func_geoip2_city() -%}
    create or replace function geoip2_city(ip String)
    returns string
    language java
    handler = 'X.x'
    imports = ('@warehouse_db.public.geoip_jars/geoip2-4.0.1.jar'
            , '@warehouse_db.public.geoip_jars/maxmind-db-3.0.0.jar'
            , '@warehouse_db.public.geoip_jars/jackson-annotations-2.14.2.jar'
            , '@warehouse_db.public.geoip_jars/jackson-core-2.14.2.jar'
            , '@warehouse_db.public.geoip_jars/jackson-databind-2.14.2.jar')
    as $$
    import java.io.File;
    import java.net.InetAddress;

    import com.snowflake.snowpark_java.types.SnowflakeFile;
    import com.maxmind.geoip2.model.*;
    import com.maxmind.geoip2.DatabaseReader;
    import com.maxmind.geoip2.exception.AddressNotFoundException;

    class X {
        DatabaseReader _reader;
        
        public String x(String ip) throws Exception {
            if (null == _reader) {
                // lazy initialization
                _reader = new DatabaseReader.Builder(SnowflakeFile.newInstance("@warehouse_db.public.geoip_jars/GeoLite2-City.mmdb", false).getInputStream()).build();
            }
            try {
                CityResponse r = _reader.city(InetAddress.getByName(ip));
                return r.getCity().getName();
            } catch (AddressNotFoundException e) {
                return null;
            }
        }
    }
    $$;
{%- endmacro %}

{% macro func_geoip2_subdivision() -%}
    create or replace function geoip2_subdivision(ip String)
    returns string
    language java
    handler = 'X.x'
    imports = ('@warehouse_db.public.geoip_jars/geoip2-4.0.1.jar'
            , '@warehouse_db.public.geoip_jars/maxmind-db-3.0.0.jar'
            , '@warehouse_db.public.geoip_jars/jackson-annotations-2.14.2.jar'
            , '@warehouse_db.public.geoip_jars/jackson-core-2.14.2.jar'
            , '@warehouse_db.public.geoip_jars/jackson-databind-2.14.2.jar')
    as $$
    import java.io.File;
    import java.net.InetAddress;

    import com.snowflake.snowpark_java.types.SnowflakeFile;
    import com.maxmind.geoip2.model.*;
    import com.maxmind.geoip2.DatabaseReader;
    import com.maxmind.geoip2.exception.AddressNotFoundException;

    class X {
        DatabaseReader _reader;
        
        public String x(String ip) throws Exception {
            if (null == _reader) {
                // lazy initialization
                _reader = new DatabaseReader.Builder(SnowflakeFile.newInstance("@warehouse_db.public.geoip_jars/GeoLite2-City.mmdb", false).getInputStream()).build();
            }
            try {
                CityResponse r = _reader.city(InetAddress.getByName(ip));
                return r.getMostSpecificSubdivision().getIsoCode();
            } catch (AddressNotFoundException e) {
                return null;
            }
        }
    }
    $$;
{%- endmacro %}
