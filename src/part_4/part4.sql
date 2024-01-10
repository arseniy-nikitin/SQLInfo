CREATE DATABASE test;
\c test;


------------------------TASK_1----------------------------
-- Create a stored procedure that, without destroying
-- the database, destroys all those tables in the current
-- database whose names begin with the phrase 'TableName'.

CREATE TABLE IF NOT EXISTS table_name_1
(
  id       SERIAL PRIMARY KEY,
  column_1 varchar(10)
);

CREATE TABLE IF NOT EXISTS table_name_2
(
  id       SERIAL PRIMARY KEY,
  column_1 varchar(10)
);

CREATE TABLE IF NOT EXISTS table_name_3
(
  id       SERIAL PRIMARY KEY,
  column_1 varchar(10)
);

CREATE OR REPLACE PROCEDURE destroy_tables() AS
$$
DECLARE
  v_table_name TEXT;
BEGIN
  FOR v_table_name IN
    SELECT table_name
    FROM information_schema.tables
    WHERE table_name LIKE 'table_name%'
    LOOP
      EXECUTE format('DROP TABLE IF EXISTS %I CASCADE;', v_table_name);
    END LOOP;
END;
$$ LANGUAGE plpgsql;

CALL destroy_tables();


------------------------TASK_2----------------------------
-- Create a stored procedure with an output parameter that
-- outputs a list of names and parameters of all scalar
-- user's SQL functions in the current database. Do not
-- output function names without parameters. The names and
-- the list of parameters must be in one string. The output
-- parameter returns the number of functions found.

CREATE OR REPLACE FUNCTION fnc_non_scalar() RETURNS VOID AS
$$
BEGIN
  RAISE NOTICE 'Hi, I am non scalar function!';
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION fnc_scalar_1(num INTEGER) RETURNS INTEGER AS
$$
BEGIN
  RETURN num * num;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION fnc_scalar_2(num1 INTEGER, num2 INTEGER) RETURNS INTEGER AS
$$
BEGIN
  RETURN num1 * num2;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE get_scalar_functions(OUT function_count INTEGER) AS
$$
DECLARE
  row     RECORD;
  message TEXT    := '';
  count   INTEGER := 0;
BEGIN
  FOR row IN
    SELECT pgp.proname                             AS name,
           string_agg(pgp.proargtypes::TEXT, ', ') AS parameters
    FROM information_schema.routines isr
           INNER JOIN pg_proc pgp ON isr.routine_name = pgp.proname
    WHERE isr.specific_schema NOT IN ('pg_catalog', 'information_schema')
      AND upper(isr.routine_type::TEXT) = 'FUNCTION'
      AND upper(pgp.prorettype::TEXT) != 'VOID'
      AND pgp.pronargs != 0
      AND pgp.provariadic = 0
    GROUP BY pgp.proname
    LOOP
      message := concat(message, row.name, ' ', row.parameters, E'\n');
      count := count + 1;
    END LOOP;
  IF message != '' THEN
    RAISE NOTICE '%', message;
  END IF;
  function_count := count;
END;
$$ LANGUAGE plpgsql;

CALL get_scalar_functions(function_count := NULL);


------------------------TASK_3----------------------------
-- Create a stored procedure with output parameter, which
-- destroys all SQL DML triggers in the current database. The
-- output parameter returns the number of destroyed triggers.

CREATE TABLE IF NOT EXISTS trigger_table
(
  id       SERIAL PRIMARY KEY,
  column_1 varchar(10)
);

CREATE OR REPLACE FUNCTION trigger_function() RETURNS TRIGGER AS
$$
BEGIN
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_1
  AFTER INSERT
  ON trigger_table
  FOR EACH ROW
EXECUTE FUNCTION trigger_function();

CREATE TRIGGER trigger_2
  AFTER UPDATE
  ON trigger_table
  FOR EACH ROW
EXECUTE FUNCTION trigger_function();

CREATE TRIGGER trigger_3
  AFTER DELETE
  ON trigger_table
  FOR EACH ROW
EXECUTE FUNCTION trigger_function();

CREATE OR REPLACE PROCEDURE destroy_triggers(OUT trigger_count INTEGER) AS
$$
DECLARE
  row   RECORD;
  count INTEGER := 0;
BEGIN
  FOR row IN
    SELECT ist.trigger_name,
           ist.trigger_schema,
           ist.event_object_table
    FROM information_schema.triggers ist
    WHERE ist.event_manipulation IN ('INSERT', 'UPDATE', 'DELETE')
    LOOP
      EXECUTE format('DROP TRIGGER IF EXISTS %I ON %I.%I CASCADE;',
                     row.trigger_name,
                     row.trigger_schema,
                     row.event_object_table);
      count := count + 1;
    END LOOP;
  trigger_count := count;
END;
$$ LANGUAGE plpgsql;

CALL destroy_triggers(trigger_count := NULL);


------------------------TASK_4----------------------------
-- Create a stored procedure with an input parameter that
-- outputs names and descriptions of object types (only
-- stored procedures and scalar functions) that have a
-- string specified by the procedure parameter.

CREATE OR REPLACE PROCEDURE find_objects(IN target TEXT) AS
$$
DECLARE
  row     RECORD;
  message TEXT := '';
BEGIN
  FOR row IN
    SELECT pgp.proname     AS name,
           pgd.description AS description
    FROM pg_proc pgp
           INNER JOIN information_schema.routines isr ON pgp.proname = isr.routine_name
           FULL JOIN pg_description pgd on pgd.objoid = pgp.oid
    WHERE pgp.proname LIKE concat('%', target, '%')
      AND (
          upper(isr.routine_type::TEXT) = 'PROCEDURE'
        OR (
            (
                  upper(isr.routine_type::TEXT) = 'FUNCTION'
                AND upper(pgp.prorettype::TEXT) != 'VOID'
                AND pgp.pronargs != 0
                AND pgp.provariadic = 0)
            )
      )
    LOOP
      message := concat(message, row.name, ' ', row.description, E'\n');
    END LOOP;
  IF message != '' THEN
    RAISE NOTICE '%', message;
  END IF;
END;
$$ LANGUAGE plpgsql;

CALL find_objects('find_objects');
CALL find_objects('_scalar');
CALL find_objects('pg_relation_size');