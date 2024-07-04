CREATE FUNCTION check_table(reloid oid) RETURNS void AS $$
BEGIN
    IF NOT (SELECT orioledb_tbl_check(reloid)) THEN
        RAISE EXCEPTION 'CHECK FAILED';
    END IF;
END;
$$ LANGUAGE plpgsql;

SELECT c.relname, orioledb_tbl_check(reloid), check_table(reloid)
    FROM orioledb_table ot
    JOIN pg_class c ON c.oid = ot.reloid;