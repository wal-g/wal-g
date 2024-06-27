CREATE FUNCTION check_table_compression(reloid oid) RETURNS void AS $$
BEGIN
    IF NOT (WITH errors AS (
                SELECT substring(errors_line, '%Errors #"%#",%', '#') errors_num
                    FROM regexp_split_to_table(orioledb_tbl_compression_check(5, reloid),
                                               '\n') AS errors_line
                WHERE errors_line LIKE '%Errors%'
            )
            SELECT COUNT(*) = 0 FROM errors WHERE errors_num != '0') THEN
        RAISE EXCEPTION 'CHECK FAILED';
    END IF;
END;
$$ LANGUAGE plpgsql;

SELECT c.relname, check_table_compression(reloid)
    FROM orioledb_table ot
    JOIN pg_class c ON c.oid = ot.reloid;