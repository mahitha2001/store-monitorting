create database stores;

use stores;

create table timezones (
	store_id BIGINT NOT NULL PRIMARY KEY,
    timezone_str VARCHAR(45) default 'America/Chicago'
);

LOAD DATA INFILE '/Users/mahitha0x01/Downloads/bq-results-20230125-202210-1674678181880.csv' 
INTO TABLE timezones
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n' 
IGNORE 1 LINES (store_id, timezone_str);

create table daysOfWeek(
	dayID INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    dayOfWeek varchar(10) NOT NULL UNIQUE
);
INSERT INTO daysOfWeek(dayOfWeek) values 
('Monday'),
('Tuesday'),
('Wednesday'),
('Thursday'),
('Friday'),
('Saturday'),
('Sunday');
alter table daysOfWeek modify column dayID INT AUTO_INCREMENT;
set SQL_SAFE_UPDATES = 0;
update daysOfWeek set dayID=dayID-1;

drop table menu_hours;

CREATE TABLE menu_hours (
    store_id BIGINT NOT NULL,
    dayOfWeek INT NOT NULL,
    start_time_local TIME(6) NOT NULL DEFAULT '00:00:00',
    end_time_local TIME(6) NOT NULL DEFAULT '24:00:00',
    FOREIGN KEY (dayOfWeek) REFERENCES daysOfWeek(dayID)
);

alter table menu_hours
add constraint fk_mh_store_id
foreign key (store_id) references timezones(store_id);

create table store_status(
	store_id BIGINT NOT NULL,
    status ENUM('active', 'inactive') not null,
    timestamp_utc DATETIME(6) not null
);

alter table store_status
add constraint fk_ss_store_id
foreign key (store_id) references timezones(store_id);

alter table menu_hours drop constraint fk_mh_store_id;

-- load all data from menu hours.csv
LOAD DATA INFILE '/Users/mahitha0x01/Downloads/Menu hours.csv' 
INTO TABLE menu_hours
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n' 
IGNORE 1 LINES (store_id, dayOfWeek, start_time_local, end_time_local);


-- before adding any value into store_status

-- check if store_id exists in timezones and menu hours table
-- if it exists in both ok!!
-- if it doesn't exist in timezones table: add the store_id with default timezone_str 
-- if it doesn't exist in menu hours (check for all 7 days)

-- for this we need to add a before insert trigger to store_status table 

delimiter //
CREATE TRIGGER `store_status_BEFORE_INSERT` 
BEFORE INSERT ON `store_status` 
FOR EACH ROW 
BEGIN
    DECLARE tz_count INT;
    DECLARE mh_count INT;
    DECLARE day_count INT;
    DECLARE temp INT;
    DECLARE max_dayID INT DEFAULT 6;

    DECLARE done INT DEFAULT FALSE;
    DECLARE cur CURSOR FOR
        SELECT dayOfWeek
        FROM menu_hours
        WHERE store_id = NEW.store_id
        ORDER BY dayOfWeek ASC;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    SELECT COUNT(store_id) INTO tz_count FROM timezones WHERE store_id = NEW.store_id;
    IF tz_count = 0 THEN
        INSERT INTO timezones (store_id) VALUES (NEW.store_id);
    END IF;

    SELECT COUNT(store_id) INTO mh_count FROM menu_hours WHERE store_id = NEW.store_id;
    IF mh_count < 7 THEN
        set temp=0;
        WHILE temp <= max_dayID DO
            INSERT IGNORE INTO menu_hours (store_id, dayOfWeek) VALUES (NEW.store_id, temp);
            SET temp = temp + 1;
        END WHILE;
    END IF;
END; //
delimiter ;

LOAD DATA INFILE '/Users/mahitha0x01/Downloads/store status.csv' 
INTO TABLE store_status
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n' 
IGNORE 1 LINES (store_id, status, @timestamp_utc)
SET timestamp_utc = convert(substring(@timestamp_utc, 1,length(@timestamp_utc)-4), datetime(6));









