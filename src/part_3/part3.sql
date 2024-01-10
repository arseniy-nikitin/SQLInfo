------------------------TASK_1----------------------------
-- Write a function that returns the TransferredPoints table in a more human-readable form
-- Peer's nickname 1, Peer's nickname 2, number of transferred peer points.
-- The number is negative if peer 2 received more points from peer 1.

DROP FUNCTION IF EXISTS get_transferred_points();

CREATE OR REPLACE FUNCTION get_transferred_points()
    RETURNS TABLE
            (
                peer1         VARCHAR(255),
                peer2         VARCHAR(255),
                points_amount BIGINT
            )
AS
$$
BEGIN
    RETURN QUERY
        WITH amount_1 AS (SELECT TP.id,
                                 TP.checking_peer,
                                 TP.checked_peer,
                                 SUM(TP.points_amount) AS point_sum
                          FROM transferred_points TP
                          GROUP BY TP.id, TP.checking_peer, TP.checked_peer),
             amount_2 AS (SELECT TP.id,
                                 TP.checking_peer,
                                 TP.checked_peer,
                                 SUM(TP.points_amount) AS point_sum
                          FROM transferred_points TP
                          GROUP BY TP.id, TP.checking_peer, TP.checked_peer)
        SELECT a1.checking_peer                         AS peer1,
               a1.checked_peer                          AS peer2,
               a1.point_sum - COALESCE(a2.point_sum, 0) AS points_amount
        FROM amount_1 a1
                 LEFT JOIN amount_2 a2 ON a1.checked_peer = a2.checking_peer AND
                                          a1.checking_peer = a2.checked_peer AND
                                          a1.id != a2.id;
END
$$
    LANGUAGE plpgsql;

SELECT *
FROM get_transferred_points();


------------------------TASK_2----------------------------
-- Write a function that returns a table of the following form: user name, name of the checked task, number of XP received
-- Include in the table only tasks that have successfully passed the check (according to the Checks table).
-- One task can be completed successfully several times. In this case, include all successful checks in the table.

DROP FUNCTION IF EXISTS get_passed_projects();

CREATE OR REPLACE FUNCTION get_passed_projects()
    RETURNS TABLE
            (
                peer VARCHAR(255),
                task VARCHAR(255),
                xp   INT
            )
AS
$$
BEGIN
    RETURN QUERY
        SELECT ch.peer     AS Peer,
               ch.task     AS Task,
               x.xp_amount AS XP
        FROM checks ch
                 JOIN xp x on ch.id = x."check";
END
$$
    LANGUAGE plpgsql;

SELECT *
FROM get_passed_projects();


------------------------TASK_3----------------------------
-- 3) Написать функцию, определяющую пиров, которые не выходили из кампуса в течение всего дня
-- Параметры функции: день, например 12.05.2022.
-- Функция возвращает только список пиров.

DROP FUNCTION IF EXISTS get_peers_inside_campus(date);

CREATE OR REPLACE FUNCTION get_peers_inside_campus(pdate DATE)
    RETURNS TABLE
            (
                peer_nickname VARCHAR(255)
            )
AS
$$
BEGIN
    RETURN QUERY
        WITH counting_tt AS (SELECT tt2.peer,
                                    tt2.date,
                                    COUNT(CASE WHEN state = 1 THEN 1 END) AS entered,
                                    COUNT(CASE WHEN state = 2 THEN 1 END) AS out
                             FROM time_tracking tt2
                             GROUP BY 1, 2)
        SELECT tt.peer AS peer_nickname
        FROM counting_tt tt
        WHERE tt.date = pdate
          AND tt.entered > 0
          AND tt.out = 0;
END
$$
    LANGUAGE plpgsql;

SELECT *
FROM get_peers_inside_campus('2020-01-03');


------------------------TASK_4----------------------------
-- 4) Calculate the change in the number of peer points of each peer using the TransferredPoints table
-- Output the result sorted by the change in the number of points.
-- Output format: peer's nickname, change in the number of peer points

DROP FUNCTION IF EXISTS get_change_in_peers_prp();

CREATE OR REPLACE FUNCTION get_change_in_peers_prp()
    RETURNS TABLE
            (
                peer_nickname VARCHAR(255),
                points_change BIGINT
            )
AS
$$
BEGIN
    RETURN QUERY
        WITH earned_prp AS (SELECT checking_peer      AS peer_nickname,
                                   SUM(points_amount) AS earned
                            FROM transferred_points
                            GROUP BY 1),
             spent_prp AS (SELECT checked_peer       AS peer_nickname,
                                  SUM(points_amount) AS spent
                           FROM transferred_points
                           GROUP BY 1)
        SELECT ep.peer_nickname,
               COALESCE(ep.earned, 0) - COALESCE(sp.spent, 0) AS points_change
        FROM earned_prp ep
                 FULL JOIN spent_prp sp ON ep.peer_nickname = sp.peer_nickname
        ORDER BY 2 DESC;
END
$$
    LANGUAGE plpgsql;

SELECT *
FROM get_change_in_peers_prp();


------------------------TASK_5----------------------------
-- 5) Calculate the change in the number of peer points of each peer
-- using the table returned by the first function from Part 3
-- Output the result sorted by the change in the number of points.
-- Output format: peer's nickname, change in the number of peer points

DROP FUNCTION IF EXISTS calc_change_peers_prp_from_fnc();

CREATE OR REPLACE FUNCTION calc_change_peers_prp_from_fnc()
    RETURNS TABLE
            (
                peer          VARCHAR(255),
                points_change NUMERIC
            )
AS
$$
BEGIN
    RETURN QUERY
        WITH earned_prp AS (SELECT peer1              AS peer_nickname,
                                   SUM(points_amount) AS earned
                            FROM get_transferred_points()
                            GROUP BY peer1)
        SELECT peer_nickname AS peer,
               earned        AS points_change
        FROM earned_prp
        ORDER BY 2 DESC;
END
$$
    LANGUAGE plpgsql;

SELECT *
FROM calc_change_peers_prp_from_fnc();

------------------------TASK_6----------------------------
-- 6) Find the most frequently checked task for each day
-- If there is the same number of checks for some tasks in a certain day,
-- output all of them.
-- Output format: day, task name

DROP FUNCTION IF EXISTS get_most_checked_task_for_day();

CREATE OR REPLACE FUNCTION get_most_checked_task_for_day()
    RETURNS TABLE
            (
                day  DATE,
                task VARCHAR(255)
            )
AS
$$
BEGIN
    RETURN QUERY
        WITH check_count AS (SELECT ch.date,
                                    ch.task,
                                    COUNT(*)                                                        AS checks_count,
                                    DENSE_RANK() OVER (PARTITION BY ch.date ORDER BY COUNT(*) DESC) AS rank
                             FROM checks ch
                             GROUP BY ch.date, ch.task)
        SELECT cte.date AS day,
               cte.task
        FROM check_count cte
        WHERE rank = 1;
END
$$
    LANGUAGE plpgsql;

SELECT *
FROM get_most_checked_task_for_day();

------------------------TASK_7----------------------------
-- 7) Find all peers who have completed the whole given block of tasks and the completion date of the last task
-- Procedure parameters: name of the block, for example “CPP”.
-- The result is sorted by the date of completion.
-- Output format: peer's name, date of completion of the block (i.e. the last completed task from that block)

DROP PROCEDURE IF EXISTS get_passed_whole_block_peers(VARCHAR);

CREATE OR REPLACE PROCEDURE get_passed_whole_block_peers(pblock_name VARCHAR(255))
    LANGUAGE plpgsql
AS
$$
BEGIN
        DROP TABLE IF EXISTS tmp_passed_whole_block_peers;
        CREATE TEMPORARY TABLE tmp_passed_whole_block_peers AS
        WITH final_project AS ( -- Получаем последний проект искомого блока
            SELECT title
            FROM tasks
            WHERE SUBSTRING(title FROM '^[A-Za-z]+') = pblock_name
            ORDER BY 1 DESC
            LIMIT 1)
        SELECT gpp.peer,
               ch.date AS day
        FROM get_passed_projects() gpp
                 JOIN final_project fp ON gpp.task = fp.title
                 JOIN checks ch ON gpp.peer = ch.peer AND
                                   fp.title = ch.task;
END
$$;

CALL get_passed_whole_block_peers('SQL');

SELECT *
FROM tmp_passed_whole_block_peers;


------------------------TASK_8----------------------------
-- 8) Determine which peer each student should go to for a check.
-- You should determine it according to the recommendations of the peer's friends,
-- i.e. you need to find the peer with the greatest number of friends who recommend to be checked by him.
-- Output format: peer's nickname, nickname of the checker found

DROP FUNCTION IF EXISTS get_recommended_peer();

CREATE OR REPLACE FUNCTION get_recommended_peer()
    RETURNS TABLE
            (
                peer             VARCHAR(255),
                recommended_peer VARCHAR(255)
            )
    LANGUAGE plpgsql
AS
$$
BEGIN
    RETURN QUERY
        WITH peers_friends AS (SELECT p.nickname AS peer,
                                      f2.peer2   AS friend
                               FROM peers p
                                        JOIN friends f2 on p.nickname = f2.peer1),
             friends_recommendations AS (SELECT pf.peer                   AS peer,
                                                pf.friend                 AS friend,
                                                r.recommended_peer,
                                                COUNT(r.recommended_peer) AS recommendations_count
                                         FROM recommendations r
                                                  JOIN peers_friends AS pf
                                                       ON r.peer = pf.friend AND
                                                          r.recommended_peer != pf.peer
                                         GROUP BY pf.peer, pf.friend, r.recommended_peer),
             cte_tmp AS (SELECT fr.peer,
                                fr.recommended_peer,
                                SUM(fr.recommendations_count) AS rec_count
                         FROM friends_recommendations fr
                         GROUP BY fr.peer, fr.recommended_peer),
             ranked_recommendations AS (SELECT ct.peer,
                                               ct.recommended_peer,
                                               ct.rec_count,
                                               ROW_NUMBER() OVER (PARTITION BY ct.peer ORDER BY rec_count DESC) AS rn
                                        FROM cte_tmp ct)
        SELECT rr.peer             AS peer,
               rr.recommended_peer AS recommended_peer
        FROM ranked_recommendations rr
        WHERE rn = 1;

END
$$;

SELECT *
FROM get_recommended_peer();


------------------------TASK_9----------------------------
-- 9) Determine the percentage of peers who:
--
-- Started only block 1
-- Started only block 2
-- Started both
-- Have not started any of them
--
-- A peer is considered to have started a block if he has at least one check
-- of any task from this block (according to the Checks table)
-- Procedure parameters: name of block 1, for example SQL, name of block 2, for example A.
-- Output format: percentage of those who started only the first block,
-- percentage of those who started only the second block,
-- percentage of those who started both blocks,
-- percentage of those who did not started any of them

DROP PROCEDURE IF EXISTS get_started_blocks_peers(VARCHAR, VARCHAR, OUT NUMERIC, OUT NUMERIC, OUT NUMERIC, OUT NUMERIC);

CREATE OR REPLACE PROCEDURE get_started_blocks_peers(
    pblock_1 VARCHAR(255),
    pblock_2 VARCHAR(255),
    OUT started_block1 DECIMAL,
    OUT started_block2 DECIMAL,
    OUT started_both_blocks DECIMAL,
    OUT didnt_start_any_block DECIMAL)
    LANGUAGE plpgsql
AS
$$
BEGIN
        -- Получаем общее количество пиров
    WITH total_peers AS (SELECT COUNT(*)::DECIMAL AS peers_counter FROM peers),
         -- Получаем количество пиров, начавших ТОЛЬКО первый блок
         only_1_b AS (SELECT ch.peer
                      FROM checks ch
                      WHERE ch.peer IN (SELECT ch.peer
                                        FROM checks ch
                                        WHERE SUBSTRING(task FROM '^[A-Za-z]+') = pblock_1)
                        AND ch.peer NOT IN (SELECT ch.peer
                                            FROM checks ch
                                            WHERE SUBSTRING(task FROM '^[A-Za-z]+') = pblock_2)),
         -- Получаем количество пиров, начавших ТОЛЬКО второй блок
         only_2_b AS (SELECT ch.peer
                      FROM checks ch
                      WHERE ch.peer IN (SELECT ch.peer
                                        FROM checks ch
                                        WHERE SUBSTRING(task FROM '^[A-Za-z]+') = pblock_2)
                        AND ch.peer NOT IN (SELECT ch.peer
                                            FROM checks ch
                                            WHERE SUBSTRING(task FROM '^[A-Za-z]+') = pblock_1)),
         -- Получаем количество пиров, начавших и первый и второй блок
         started_both_blocks AS (SELECT ch.peer
                                 FROM checks ch
                                 WHERE ch.peer IN (SELECT ch.peer
                                                   FROM checks ch
                                                   WHERE SUBSTRING(task FROM '^[A-Za-z]+') = pblock_1)
                                   AND ch.peer NOT IN (SELECT ch.peer
                                                       FROM checks ch
                                                       WHERE SUBSTRING(task FROM '^[A-Za-z]+') = pblock_2)),
         -- Получаем количество пиров, не приступавших ни к одному из блоков
         not_in_any_blocks AS (SELECT ch.peer
                               FROM checks ch
                               WHERE ch.peer NOT IN (SELECT ch.peer
                                                     FROM checks ch
                                                     WHERE SUBSTRING(task FROM '^[A-Za-z]+') = pblock_1)
                                 AND ch.peer NOT IN (SELECT ch.peer
                                                     FROM checks ch
                                                     WHERE SUBSTRING(task FROM '^[A-Za-z]+') = pblock_2))
    SELECT ROUND((SELECT COUNT(peer)::DECIMAL AS peers_counter FROM only_1_b) / tp.peers_counter * 100,
                 2) AS started_block1,
           ROUND((SELECT COUNT(peer)::DECIMAL AS peers_counter FROM only_2_b) / tp.peers_counter * 100,
                 2) AS started_block2,
           ROUND((SELECT COUNT(peer)::DECIMAL AS peers_counter FROM started_both_blocks) / tp.peers_counter * 100,
                 2) AS started_both_blocks,
           ROUND((SELECT COUNT(peer)::DECIMAL AS peers_counter FROM not_in_any_blocks) / tp.peers_counter * 100,
                 2) AS didnt_start_any_block
    INTO started_block1, started_block2, started_both_blocks, didnt_start_any_block
    FROM total_peers tp;
END
$$;

CALL get_started_blocks_peers('SQL', 'CPP',
    started_block1 := NULL,
    started_block2 := NULL,
    started_both_blocks := NULL,
    didnt_start_any_block := NULL);


------------------------TASK_10----------------------------
-- Determine the percentage of peers who have ever successfully passed a check on their birthday
-- Also determine the percentage of peers who have ever failed a check on their birthday. \
-- Output format: percentage  of peers who have ever successfully passed a check on their birthday,
--     percentage of peers who have ever failed a check on their birthday

DROP FUNCTION IF EXISTS get_birthday_checks();

CREATE OR REPLACE FUNCTION get_birthday_checks()
    RETURNS TABLE
            (
                successful_checks DECIMAL,
                failure_checks    DECIMAL
            )
    LANGUAGE plpgsql
AS
$$
BEGIN
    RETURN QUERY
        WITH total_peers AS (SELECT COUNT(*)::DECIMAL AS peers_counter
                             FROM peers),
             successful_ch AS (SELECT p.nickname AS peer
                               FROM checks ch
                                        JOIN p2p ON ch.id = p2p."check"
                                        LEFT JOIN verter v ON ch.id = v."check"
                                        JOIN peers p ON ch.peer = p.nickname
                               WHERE p2p.state = 'Success'
                                 AND COALESCE(v.state, 'Success') = 'Success'
                                 AND EXTRACT(MONTH FROM ch.date) = EXTRACT(MONTH FROM p.birthday)
                                 AND EXTRACT(DAY FROM ch.date) = EXTRACT(DAY FROM p.birthday)),
             failure_ch AS (SELECT ch.peer
                            FROM checks ch
                                     JOIN p2p ON ch.id = p2p."check"
                                     LEFT JOIN verter v on ch.id = v."check"
                                     JOIN peers p ON ch.peer = p.nickname
                            WHERE ((p2p.state = 'Failure' OR v.state = 'Failure') OR
                                   (p2p.state = 'Failure' AND v.state IS NULL))
                              AND EXTRACT(MONTH FROM ch.date) = EXTRACT(MONTH FROM p.birthday)
                              AND EXTRACT(DAY FROM ch.date) = EXTRACT(DAY FROM p.birthday))
        SELECT ROUND((SELECT COUNT(peer)::DECIMAL FROM successful_ch) / tp.peers_counter * 100,
                     2) AS successful_checks,
               ROUND((SELECT COUNT(peer)::DECIMAL FROM failure_ch) / tp.peers_counter * 100,
                     2) AS failure_checks
        FROM total_peers tp;
END
$$;

SELECT *
FROM get_birthday_checks();


------------------------TASK_11----------------------------
-- Determine all peers who did the given tasks 1 and 2, but did not do task 3
-- Procedure parameters: names of tasks 1, 2 and 3.
-- Output format: list of peers

DROP PROCEDURE IF EXISTS get_given_tasks(VARCHAR, VARCHAR, VARCHAR);

CREATE OR REPLACE PROCEDURE get_given_tasks(ptask_1 VARCHAR(255), ptask_2 VARCHAR(255), ptask_3 VARCHAR(255))
    LANGUAGE plpgsql
AS
$$
BEGIN
    DROP TABLE IF EXISTS tmp_given_tasks;
    CREATE TEMPORARY TABLE tmp_given_tasks AS
    WITH successful_ch AS (SELECT p.nickname AS peer, ch.task
                           FROM checks ch
                                    JOIN p2p ON ch.id = p2p."check"
                                    LEFT JOIN verter v ON ch.id = v."check"
                                    JOIN peers p ON ch.peer = p.nickname
                           WHERE p2p.state = 'Success'
                             AND COALESCE(v.state, 'Success') = 'Success')
    SELECT DISTINCT sc.peer
    FROM successful_ch sc
    WHERE sc.peer IN (SELECT sc2.peer FROM successful_ch sc2 WHERE sc2.task = ptask_1)
      AND sc.peer IN (SELECT sc2.peer FROM successful_ch sc2 WHERE sc2.task = ptask_2)
      AND sc.peer NOT IN (SELECT sc2.peer FROM successful_ch sc2 WHERE sc2.task = ptask_3);
END
$$;

CALL get_given_tasks('SQL1', 'A3', 'CPP1');
SELECT *
FROM tmp_given_tasks;


------------------------TASK_12----------------------------
-- Using recursive common table expression, output the number of preceding tasks for each task
-- I. e. How many tasks have to be done, based on entry conditions, to get access to the current one.
-- Output format: task name, number of preceding tasks

DROP PROCEDURE IF EXISTS get_preceding_task_number();

CREATE OR REPLACE PROCEDURE get_preceding_task_number()
    LANGUAGE plpgsql
AS
$$
BEGIN
    DROP VIEW IF EXISTS v_preceding_task_number;
    CREATE VIEW v_preceding_task_number AS
    WITH RECURSIVE next_task_r AS (SELECT title AS parent,
                                          0     AS count
                                   FROM tasks
                                   WHERE parent_task = 'None'
                                   UNION
                                   SELECT title,
                                          ntr.count + 1
                                   FROM tasks
                                            JOIN next_task_r ntr ON parent_task = ntr.parent)
    SELECT parent, count
    FROM next_task_r;
END
$$;

CALL get_preceding_task_number();

SELECT *
FROM v_preceding_task_number;


------------------------TASK_13----------------------------
-- Find "lucky" days for checks. A day is considered "lucky" if it has at least N consecutive successful checks
-- Parameters of the procedure: the N number of consecutive successful checks .
-- The time of the check is the start time of the P2P step.
-- Successful consecutive checks are the checks with no unsuccessful checks in between.
-- The amount of XP for each of these checks must be at least 80% of the maximum.
-- Output format: list of days

CREATE OR REPLACE PROCEDURE get_lucky_days(
    IN count_success_check integer,
    OUT days VARCHAR(10)[]
) AS
$$
DECLARE
    lucky_days RECORD;
BEGIN
    FOR lucky_days IN
        WITH valid_checks AS (
            SELECT
                time,
                c.date,
                CASE
                    WHEN xp.xp_amount >= 0.8 * (SELECT max_xp
                                                FROM tasks
                                                WHERE tasks.title = c.task)
                        THEN true
                    ELSE false
                    END AS status
            FROM checks c
                     JOIN p2p p on c.id = p."check"
                     JOIN tasks ON c.task = tasks.title
                     LEFT JOIN xp ON c.id = xp."check"
            WHERE p.state IN ('Success', 'Failure')
        ), date_many_successful AS (
            SELECT date AS date_success,
                   status
            FROM valid_checks
            WHERE status = true
            GROUP BY date_success, status
            HAVING count(status) > count_success_check
        ), check_status AS (
            SELECT date,
                   status,
                   lead(status) OVER (ORDER BY date,time) AS lead
            FROM valid_checks
        ),  valid_check_status AS (
            SELECT date,
                   status,
                   lead
            FROM check_status
            WHERE date IN (SELECT date_success FROM date_many_successful)
              AND status = true AND lead = true
        ), lucky_check AS (
            SELECT date,
                   count(status) AS lucky_check
            FROM valid_check_status
            GROUP BY date
        )
        SELECT date FROM lucky_check
        WHERE lucky_check >= count_success_check
        ORDER BY 1
        LOOP
            days := array_append(days, to_char(lucky_days.date, 'YYYY-MM-DD'));
        END LOOP;
END;
$$
    LANGUAGE plpgsql;

CALL get_lucky_days(1 , days := NULL);


------------------------TASK_14----------------------------
-- 14) Find the peer with the highest amount of XP
-- Output format: peer's nickname, amount of XP

DROP PROCEDURE IF EXISTS get_most_experienced_peer(OUT VARCHAR, OUT INTEGER);

CREATE OR REPLACE PROCEDURE get_most_experienced_peer(
    OUT peer_nickname VARCHAR(255),
    OUT xp INT
)
    LANGUAGE plpgsql
AS
$$
BEGIN
    WITH xp_amount AS (SELECT ch.peer,
                              SUM(x.xp_amount) AS xp_amount
                       FROM checks ch
                                JOIN xp x ON ch.id = x."check"
                       GROUP BY ch.peer
                       ORDER BY 2 DESC)
    SELECT xa.peer, xa.xp_amount
    INTO peer_nickname, xp
    FROM xp_amount xa
    LIMIT 1;
END
$$;

CALL get_most_experienced_peer(peer_nickname := NULL, xp := NULL);


------------------------TASK_15----------------------------
-- Determine the peers that came before the given time
-- at least N times during the whole time

CREATE OR REPLACE PROCEDURE get_peers_by_time(
    IN trg_time TIME,
    IN visits_num INTEGER,
    OUT list_of_peers VARCHAR(10)[]
) AS
$$
DECLARE
    row RECORD;
BEGIN
    FOR row IN
        SELECT peer,
               COUNT(time)
        FROM time_tracking
        WHERE time < trg_time
          AND state = 1
        GROUP BY peer
        HAVING COUNT(time) >= visits_num
        LOOP
            list_of_peers := array_append(list_of_peers, row.peer);
        END LOOP;
END;
$$ LANGUAGE plpgsql;

CALL get_peers_by_time('6:00:00', 3, list_of_peers := NULL);


------------------------TASK_16----------------------------
-- Determine the peers who left the campus more than
-- M times during the last N days

CREATE OR REPLACE PROCEDURE get_peers_by_date(
    IN days INTEGER,
    IN visits_num INTEGER,
    OUT list_of_peers VARCHAR(10)[]
) AS
$$
DECLARE
    row RECORD;
BEGIN
    FOR row IN
        SELECT peer,
               COUNT(date)
        FROM time_tracking
        WHERE date BETWEEN (current_date - INTERVAL '1 day' * days) AND current_date
          AND state = 2
        GROUP BY peer
        HAVING COUNT(date) > visits_num
        LOOP
            list_of_peers := array_append(list_of_peers, row.peer);
        END LOOP;
END;
$$ LANGUAGE plpgsql;

CALL get_peers_by_date(325, 1, list_of_peers := NULL);


------------------------TASK_17----------------------------
-- For each month, count how many times people born in that
-- month came to campus during the whole time (we'll call this
-- the total number of entries). For each month, count the
-- number of times people born in that month have come to
-- campus before 12:00 in all time (we'll call this the number
-- of early entries). For each month, count the percentage of
-- early entries to campus relative to the total number of
-- entries. Output format: month, percentage of early entries

CREATE OR REPLACE FUNCTION get_early_entries_percentage()
  RETURNS TABLE
          (
            month                    VARCHAR(10),
            early_entries_percentage INTEGER
          )
AS
$$
BEGIN
  RETURN QUERY
    SELECT to_char(tt.date, 'Month')::VARCHAR(10) AS month,
           (COUNT(tt.date)::NUMERIC
             / (SELECT count(DISTINCT tt2.date) FROM time_tracking tt2)
             * 100)::INTEGER                      AS early_entries_percentage
    FROM peers p
           INNER JOIN time_tracking tt ON p.nickname = tt.peer
    WHERE to_char(p.birthday, 'Month') = to_char(date, 'Month')
      AND tt.state = 1
      AND time < '12:00:00'
    GROUP BY month;
END;
$$ LANGUAGE plpgsql;

SELECT *
FROM get_early_entries_percentage();