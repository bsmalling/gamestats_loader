/*
DROP TABLE `round_events`;
DROP TABLE `player_rounds`;
DROP TABLE `performance`;
DROP TABLE `matches`;

SET FOREIGN_KEY_CHECKS = 0;
TRUNCATE TABLE `round_events`;
TRUNCATE TABLE `player_rounds`;
TRUNCATE TABLE `performance`;
TRUNCATE TABLE `matches`;
SET FOREIGN_KEY_CHECKS = 1;

GRANT DROP ON TABLE `matches` TO 'appuser'@'%';
GRANT DROP ON TABLE `performance` TO 'appuser'@'%';
GRANT DROP ON TABLE `player_rounds` TO 'appuser'@'%';
GRANT DROP ON TABLE `round_events` TO 'appuser'@'%';
*/

CREATE TABLE `matches`(
   `match_key` INT NOT NULL AUTO_INCREMENT,
   `match_id` VARCHAR(100) NOT NULL,
   `match_time` DATETIME,
   `team1_name` VARCHAR(100) NOT NULL,
   `team2_name` VARCHAR(100) NOT NULL,
   `game_mode` VARCHAR(50) NOT NULL,
   `map_name` VARCHAR(100) NOT NULL,
   `winner_name` VARCHAR(100) NOT NULL,
   `team1score` INT NOT NULL,
   `team2score` INT NOT NULL,
   `atk_at_start` VARCHAR(100) NOT NULL,
   `team1_atk_wins` INT NOT NULL,
   `team1_def_wins` INT NOT NULL,
   `team2_atk_wins` INT NOT NULL,
   `team2_def_wins` INT NOT NULL,
   `team1_score_at_half` INT NOT NULL,
   `team2_score_at_half` INT NOT NULL,
   PRIMARY KEY (match_key)
);
ALTER TABLE `matches` ADD INDEX match_id_index (match_id);

CREATE TABLE `performance`(
	`match_key` INT NOT NULL,
	`team_name` VARCHAR(100) NOT NULL,
	`player_name` VARCHAR(100) NOT NULL,
	`player_rating` FLOAT,
	`atk_rating` FLOAT,
	`def_rating` FLOAT,
	`k_d` VARCHAR(20),
	`entry` VARCHAR(20),	
	`trade_diff` INT,
	`kost` FLOAT,
	`kpr` FLOAT,
	`srv` FLOAT,
	`hs` FLOAT,
	`atk_op` VARCHAR(100),
	`def_op` VARCHAR(100),
	`kills` INT,
	`refrags` INT,
	`headshots` INT,
	`underdog_kills` INT,
	`onevx` INT,
	`multikill_rounds` INT,
	`deaths` INT,
	`traded_deaths` INT,
	`traded_by_enemy` INT,
	`opening_kills` INT,
	`opening_deaths` INT,
	`entry_kills` INT,
	`entry_deaths` INT,
	`planted_defuser` INT,
	`disabled_defuser` INT,
	`teamkills` INT,
	`teamkilled` INT,
	`ingame_points` INT,
	`last_alive` INT,
	PRIMARY KEY (`match_key`, `team_name`, `player_name`)
);
ALTER TABLE `performance` ADD CONSTRAINT fk_performance_match_key FOREIGN KEY (match_key) REFERENCES `matches` (match_key);
ALTER TABLE `performance` ADD INDEX player_name_index (player_name);

CREATE TABLE `player_rounds`(
	`match_key` INT NOT NULL,
	`team_name` VARCHAR(100) NOT NULL,
	`player_name` VARCHAR(100) NOT NULL,
	`round` INT NOT NULL,
    `map` VARCHAR(100),
    `site` VARCHAR(100),
    `side` VARCHAR(50),
    `result` VARCHAR(50),
    `round_time` BIGINT,
    `victory_type` VARCHAR(100),
    `operator` VARCHAR(100),
    `time_spent_alive` INT,
    `kills` INT,
	`refrags` INT,
    `headshots` INT,
    `underdog_kills` INT,
    `onevx` INT,
    `death` INT,
    `traded_death` INT,
    `refragged_by` VARCHAR(100),
    `traded_by_enemy` INT,
    `opening_kill` INT,
    `opening_death` INT,
    `entry_kill` INT,
    `entry_death` INT,
    `planted_defuser` INT,
    `disabled_defuser` INT,
    `teamkills` INT,
    `teamkilled` INT,
    `ingame_points` INT,
    `last_alive` INT,
    `drones_deployed` INT,
    `drone_survived_prep_phase` BOOLEAN,
    `drone_found_bomb` BOOLEAN,
    `total_droning_distance` INT,
    `total_time_drone_piloting` INT,
    `yellow_ping_leading_to_kill` BOOLEAN,
	PRIMARY KEY (`match_key`, `team_name`, `player_name`, `round`)
);
ALTER TABLE `player_rounds` ADD CONSTRAINT fk_player_rounds_match_key FOREIGN KEY (match_key) REFERENCES `matches` (match_key);
ALTER TABLE `player_rounds` ADD INDEX player_name_index (player_name);

CREATE TABLE `round_events`(
	`match_key` INT NOT NULL,
    `round` BIGINT NOT NULL,
	`time_into_round` BIGINT NOT NULL,
    `vod_time` TIME,
    `team_name` VARCHAR(100),
    `type` VARCHAR(50),
    `actor_name` VARCHAR(100),
    `victim_name` VARCHAR(100),
    `blue_team_alive` INT,
    `orange_team_alive` INT,
    `kill_subtype` VARCHAR(50)
	/* PRIMARY KEY (`match_key`, `round`, `time_into_round`) */
);
ALTER TABLE `round_events` ADD CONSTRAINT fk_round_events_match_key FOREIGN KEY (match_key) REFERENCES `matches` (match_key);
ALTER TABLE `round_events` ADD INDEX round_events_index (match_key, round);