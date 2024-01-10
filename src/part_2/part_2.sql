------------------------TASK_1----------------------------
-- Написать процедуру добавления P2P проверки
-- Параметры: ник проверяемого, ник проверяющего, название задания, статус P2P проверки, время.
-- Если задан статус "начало", добавить запись в таблицу Checks (в качестве даты использовать сегодняшнюю).
-- Добавить запись в таблицу P2P.
-- Если задан статус "начало", в качестве проверки указать
-- только что добавленную запись, иначе указать проверку с
-- незавершенным P2P этапом.

CREATE OR REPLACE PROCEDURE p2p_check (inspector varchar(255), inspected varchar(255),
                                        ch_project varchar(255), p2p_status check_status,
                                        ch_date timestamp with time zone )
AS
$$ BEGIN

    CASE p2p_status
    WHEN 'Start'::check_status THEN
        INSERT INTO checks (id, peer, task, date)
        VALUES ((SELECT MAX(id) + 1 FROM checks),
                inspected,
                ch_project,
                ch_date);
        INSERT INTO p2p (id, "check", checking_peer, state, time)
        VALUES ((SELECT MAX(id) + 1 FROM p2p),
                (SELECT MAX(id)  FROM checks WHERE peer = inspected
                                               AND task = ch_project),
                inspector,
                p2p_status,
                ch_date);
    ELSE
        INSERT INTO p2p (id, "check", checking_peer, state, time)
        VALUES ((SELECT MAX(id) + 1 FROM p2p),
                (SELECT MAX(id)  FROM checks WHERE peer = inspected
                                               AND task = ch_project),
                inspector,
                p2p_status,
                ch_date
               );
    END CASE;
END; $$
LANGUAGE plpgsql;



SET TIME ZONE '+3';
begin;

call p2p_check('nuipydcsyu'::varchar, 'frfctzkiss'::varchar, 'C1'::varchar, 'Start'::check_status,
    now()::timestamp );
SELECT * FROM p2p
ORDER BY 1 desc
limit 5;
SELECT * FROM checks

ORDER BY 1 desc
limit 5;
SELECT * from transferred_points;

rollback;
commit;

SELECT * FROM peers
order by 2 desc
limit 10;


------------------------TASK_2----------------------------
-- Написать процедуру добавления проверки Verter'ом
-- Параметры: ник проверяемого, название задания, статус проверки Verter'ом, время.
-- Добавить запись в таблицу Verter (в качестве проверки указать
-- проверку соответствующего задания с самым поздним (по времени) успешным P2P этапом)

CREATE OR REPLACE PROCEDURE verter_check(inspected varchar(255), ch_project varchar(255),
                                         ch_status check_status, ch_time timestamp)
AS
$$
BEGIN
    IF (SELECT state
        FROM p2p
        WHERE "check" = (SELECT max(id)
                         FROM checks
                         WHERE peer = 'frfctzkiss'
                           AND task = 'C1')
        ORDER BY id desc
        LIMIT 1)
        = 'Success'::check_status THEN
        WITH max_check_id AS (SELECT max(id)
                              FROM checks
                              WHERE peer = inspected
                                AND task = ch_project),
             max_check_time AS (SELECT max(ch_time)
                                FROM p2p
                                WHERE state = ch_status
                                  AND id = (SELECT * FROM max_check_id))
        INSERT
        INTO verter (id, "check", state, time)
        VALUES ((SELECT max(id) + 1 FROM verter),
                (SELECT * FROM max_check_id),
                ch_status,
                ch_time);
    END IF;
END;
$$
    LANGUAGE plpgsql;



begin;

SET TIME ZONE '+4';
call p2p_check('nuipydcsyu'::varchar, 'frfctzkiss'::varchar,
               'C1'::varchar, 'Start'::check_status,
               now()::timestamp);
call p2p_check('nuipydcsyu'::varchar, 'frfctzkiss'::varchar,
               'C1'::varchar, 'Success'::check_status,
               now()::timestamp);

call verter_check('frfctzkiss'::varchar, 'C1'::varchar, 'Start'::check_status,
                  now()::timestamp);
SELECT *
FROM p2p
ORDER BY 1 desc
limit 5;
SELECT *
FROM verter
order by 1 desc
limit 20;

SELECT *
FROM checks
--          where peer = 'smxowlrblo'
ORDER BY 1 desc
limit 5;
SELECT *
from transferred_points;

rollback ;
commit;


------------------------TASK_3----------------------------
-- Написать триггер: после добавления записи со статутом "начало"
-- в таблицу P2P, изменить соответствующую запись в таблице TransferredPoints
--
CREATE OR REPLACE FUNCTION fnc_update_transferred_points()
RETURNS TRIGGER AS
$$ BEGIN
    IF 'Start'::check_status = (SELECT state FROM p2p
                                   WHERE "check" = NEW."check" -- 13931
                                   ORDER BY state desc
                                   LIMIT 1)
    THEN
    UPDATE transferred_points tp SET points_amount = points_amount + 1
    WHERE checking_peer = NEW.checking_peer
    AND checked_peer = (SELECT peer
                        FROM checks
                        WHERE id = NEW."check");
    END IF;
    RETURN NULL;
END; $$
LANGUAGE plpgsql;

CREATE TRIGGER tg_insert_p2p
AFTER INSERT ON p2p
FOR EACH ROW
EXECUTE FUNCTION fnc_update_transferred_points();


-- проверка
begin;
-- SET TIME ZONE '+4';
SELECT * FROM transferred_points
    WHERE checking_peer = 'iosfiypdje'
      AND checked_peer = 'gdlzzcthpd';

SELECT * FROM p2p
WHERE checking_peer = 'iosfiypdje'
order by 2 desc ;
SELECT * FROM checks
WHERE peer = 'gdlzzcthpd'
order by id desc ;

call p2p_check('iosfiypdje'::varchar, 'gdlzzcthpd'::varchar,
               'C1'::varchar, 'Start'::check_status,
               now()::timestamp);
call p2p_check('iosfiypdje'::varchar, 'gdlzzcthpd'::varchar,
               'C1'::varchar, 'Failure'::check_status,
               now()::timestamp);

SELECT * FROM transferred_points
WHERE checking_peer = 'iosfiypdje'
AND checked_peer = 'gdlzzcthpd';


rollback ;
commit;


------------------------TASK_4----------------------------
-- Написать триггер: перед добавлением записи в таблицу XP,
-- проверить корректность добавляемой записи
-- Запись считается корректной, если:
--
-- Количество XP не превышает максимальное доступное для проверяемой задачи
-- Поле Check ссылается на успешную проверку
-- Если запись не прошла проверку, не добавлять её в таблицу.

CREATE OR REPLACE FUNCTION fnc_check_valid_xp()
RETURNS TRIGGER AS
$$ BEGIN
    IF NEW.xp_amount > (SELECT max_xp FROM tasks
                        WHERE title LIKE (SELECT task FROM checks
                                                   WHERE id = NEW."check"))
        AND
       'Success'::check_status = (SELECT state FROM verter
                    WHERE "check" = NEW."check"
                    ORDER BY id DESC
                    LIMIT 1)
    THEN
        INSERT INTO xp (id, "check", xp_amount)
        VALUES ((SELECT max(id) + 1 FROM xp),
                NEW."check",
                NEW.xp_amount);
    END IF;
    RETURN NEW;
END; $$
LANGUAGE plpgsql;

CREATE TRIGGER trg_fnc_check_valid_xp
BEFORE INSERT ON xp
FOR EACH ROW
EXECUTE FUNCTION fnc_check_valid_xp();

select * from tasks
-- WHERE title = 'C1'
limit 5;

select * from xp
order by 1 desc
limit 10;


-- проверка
begin;
SET TIME ZONE '+4';
call p2p_check('nuipydcsyu'::varchar, 'frfctzkiss'::varchar,
               'C1'::varchar, 'Start'::check_status,
               now()::timestamp);
call p2p_check('nuipydcsyu'::varchar, 'frfctzkiss'::varchar,
               'C1'::varchar, 'Success'::check_status,
               now()::timestamp);

call verter_check('frfctzkiss'::varchar, 'C1'::varchar, 'Start'::check_status,
                  now()::timestamp);
call verter_check('frfctzkiss'::varchar, 'C1'::varchar, 'Success'::check_status,
                  now()::timestamp);

insert into xp (id, "check", xp_amount) VALUES ((SELECT max(id) +1 FROM xp), (SELECT max(id) FROM checks), 175);

select * from xp
order by 1 desc
limit 10;

rollback;
commit;