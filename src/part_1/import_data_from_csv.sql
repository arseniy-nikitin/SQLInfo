-- import_data_from_csv.sql

CALL import_data_from_csv('peers',
                          '/Users/danialle/Projects/SQLProjects/SQL2_Info21_v1.0-1/src/dataset_sql/peers.csv',
                          ';');
CALL import_data_from_csv('tasks',
                          '/Users/danialle/Projects/SQLProjects/SQL2_Info21_v1.0-1/src/dataset_sql/tasks.csv',
                          ';');
CALL import_data_from_csv('checks',
                          '/Users/danialle/Projects/SQLProjects/SQL2_Info21_v1.0-1/src/dataset_sql/checks.csv',
                          ';');
CALL import_data_from_csv('p2p',
                          '/Users/danialle/Projects/SQLProjects/SQL2_Info21_v1.0-1/src/dataset_sql/p2p.csv',
                          ';');
CALL import_data_from_csv('verter',
                          '/Users/danialle/Projects/SQLProjects/SQL2_Info21_v1.0-1/src/dataset_sql/verter.csv',
                          ';');
CALL import_data_from_csv('transferred_points',
                          '/Users/danialle/Projects/SQLProjects/SQL2_Info21_v1.0-1/src/dataset_sql/transferred_points.csv',
                          ';');
CALL import_data_from_csv('friends',
                          '/Users/danialle/Projects/SQLProjects/SQL2_Info21_v1.0-1/src/dataset_sql/friends.csv',
                          ';');
CALL import_data_from_csv('recommendations',
                          '/Users/danialle/Projects/SQLProjects/SQL2_Info21_v1.0-1/src/dataset_sql/recommendations.csv',
                          ';');
CALL import_data_from_csv('xp',
                          '/Users/danialle/Projects/SQLProjects/SQL2_Info21_v1.0-1/src/dataset_sql/xp.csv',
                          ';');
CALL import_data_from_csv('time_tracking',
                          '/Users/danialle/Projects/SQLProjects/SQL2_Info21_v1.0-1/src/dataset_sql/time_tracking.csv',
                          ';');
