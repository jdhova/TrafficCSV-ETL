

## UK RAW TRAFFIC CSV DATA SET => SSMS DATABASE WITH ETL SSIS
## AUTHOR  JUDE OKAGU  https://roadtraffic.dft.gov.uk/downloads  Raw count data - major and minor roads





### ------------------------------DIM ROAD CATEGORY------------------------------------4

```
GO

IF NOT EXISTS(SELECT *
                FROM UK_Traffic.INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE' and TABLE_NAME = 'roadcategory'
            )
exec('CREATE TABLE dim.roadcategory (
    [Key_road_category] int NOT NULL,
    [Key_road_type] int,
    [Road_category] varchar(255),
    [Catergory_description] varchar(255),
    [Road_type] varchar(255), 
    Constraint PK_roadcategory PRIMARY KEY (Key_road_category ASC)
)'
);
GO
IF NOT EXISTS(SELECT *
                FROM UK_Traffic.INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE' and TABLE_NAME = 'dim_roadcategory'
            )
exec('CREATE TABLE err.dim_roadcategory (
    [Key_road_category] int,
    [Key_road_type] int,
    [Road_category] varchar(255),
    [Catergory_description] varchar(255),
    [Road_type] varchar(255),
    [Local] datetime,
    [UTC] datetime,
    [Offset] int,
    [ErrorCode] int,
    [ErrorColumn] int
)'
);
GO


```
### --------------------------------road CATEGORY CODE-------------------------------------

```
Select distinct (case
            when Road_category = 'MB' then 2000
            when Road_category = 'MCU' then 3000
            when Road_category = 'PA' then 4000
            when Road_category = 'PM' then 5000
            when Road_category = 'TA' then 6000            
            when Road_category = 'TM' then 7000        end) as 'Key_road_category'
        ,(case 
                When Road_type = 'Major' then 1
                When Road_type = 'Minor' then 0
            end) as 'Key_road_type'
        ,Road_category
        ,(case
            when Road_category = 'MB' then 'Class B road'
            when Road_category = 'MCU' then 'Class C road or Unclassified road'
            when Road_category = 'PA' then 'Class A Principal road'
            when Road_category = 'PM' then 'M or Class A Principal Motorway'
            when Road_category = 'TA' then 'Class A Trunk road'
            when Road_category = 'TM' then 'M or Class A Trunk Motorway'
        end) as 'Catergory_description'
      ,Road_type
      ,GETDATE () as 'Local'
      ,GETUTCDATE () as 'UTC'
      ,DATEDIFF(hh, GETUTCDATE(), GETDATE()) as 'Offset'
From [stg].[traffic]
Order by 1


```
### ------------------------------DIM region STARTS HERE------------------------------------


```
IF NOT EXISTS(SELECT *
                FROM UK_Traffic.INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE' and TABLE_NAME = 'region'
            )
exec('CREATE TABLE dim.region (
    [Key_Local_authority_id] int NOT NULL,
	[Region_id]varchar(2550) NULL,
	[Region_name] varchar(2550) NULL,
	[Region_ons_code] varchar (2550)  NULL,
	[Local_authority_id] int NOT NULL,
	[Local_authority_name] varchar(2550)  NULL,
	[Local_authority_code] varchar(2550)  NULL,

    Constraint PK_region PRIMARY KEY (Key_Local_authority_id ASC)
)'
);
GO
IF NOT EXISTS(SELECT *
                FROM UK_Traffic.INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE' and TABLE_NAME = 'dim_region'
            )
exec('CREATE TABLE err.dim_region (
    [Key_Local_authority_id] int NOT NULL,

	[Region_id][varchar](2550) NULL,
	[Region_name][varchar](2550) NULL,
	[Region_ons_code][varchar](2550)  NULL,
	[Local_authority_id] int NOT NULL,
	[Local_authority_name][varchar](2550)  NULL,
	[Local_authority_code][varchar](2550)  NULL,

    [Local] datetime,
    [UTC] datetime,
    [Offset] int,
    [ErrorCode] int,
    [ErrorColumn] int
)'
);
GO



```
### -------------------------------------Region Code here-----------------------------------------------------


```

SELECT DISTINCT 
Local_authority_id as Key_Local_authority_id
,[Region_id]
      ,[Region_name]
      ,[Region_ons_code]
      ,[Local_authority_id]
      ,[Local_authority_name]
      ,[Local_authority_code]
      ,GETDATE() as 'Local'
      ,GETDATE() as 'UTC'
     ,DATEDIFF (hh, GETUTCDATE(), GETDATE()) as 'Offset'
  FROM [stg].[traffic]
  order by [Region_id] asc


```
### -----------------------------------DIM Direction STARTS here-------------------------------------------------------


```
IF NOT EXISTS(SELECT *
                FROM UK_Traffic.INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE' and TABLE_NAME = 'direction'
            )
exec('CREATE TABLE dim.direction (
    [Key_Direction_of_travel] [int] NOT NULL,
	[Direction_of_travel][varchar](50) NULL,
	[Direction_of_travel_full][varchar](255) NULL,
	

    Constraint PK_direction PRIMARY KEY (Key_Direction_of_travel ASC)
)'
);
GO
IF NOT EXISTS(SELECT *
                FROM UK_Traffic.INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE' and TABLE_NAME = 'dim_direction'
            )
exec('CREATE TABLE err.dim_direction (
   	[Key_Direction_of_travel] [int] NOT NULL,
	[Direction_of_travel][varchar](50) NULL,
	[Direction_of_travel_full][varchar](255) NULL,
	[Local][datetime] NULL,
	[UTC][datetime] NULL,
	[offset][int] NULL,
	[ErrorCode][int] NULL,
	[ErrorColumn][int] NULL)'
);
GO


```
### -----------------------------------CODE Direction STARTS here-------------------------------------------------------


```
SELECT CASE
	WHEN Direction_of_travel = 'N' THEN 100
	WHEN Direction_of_travel = 'S' THEN 200
	WHEN Direction_of_travel = 'E' THEN 300
	WHEN Direction_of_travel = 'W' THEN 400
	WHEN Direction_of_travel = 'C' THEN 500
	WHEN Direction_of_travel = 'J' THEN 600
	END  as 'Key_Direction_of_travel'
	,Direction_of_travel
	,CASE
	WHEN Direction_of_travel = 'N' THEN 'North'
	WHEN Direction_of_travel = 'S' THEN 'South'
	WHEN Direction_of_travel = 'E' THEN 'East'
	WHEN Direction_of_travel = 'W' THEN 'West'
	WHEN Direction_of_travel = 'C' THEN 'Combined'
	WHEN Direction_of_travel = 'J' THEN 'Junction'
	END  as 'Direction_of_travel_full'
	,GETDATE() as 'Local'
	,GETDATE() as 'UTC'
	,DATEDIFF (hh, GETUTCDATE(), GETDATE()) as 'Offset'
FROM [UK_Traffic].[stg].[traffic]
GROUP BY Direction_of_travel
ORDER BY 1


```
### -----------------------------DIM Hour of day Starts HERE------------------------------------------------------------


```
IF NOT EXISTS(SELECT *
                FROM UK_Traffic.INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE' and TABLE_NAME = 'hourOfDay'
            )
exec('CREATE TABLE dim.hourOfDay (
    
    [Key_hourOfDay] [int] NOT NULL,
	[hourOfDay] [int] NULL,
	[hourOfDay_full][varchar](255) NULL,
	
	
    Constraint PK_hourOfDay PRIMARY KEY (Key_hourOfDay ASC)


)'
);
GO
IF NOT EXISTS(SELECT *
                FROM UK_Traffic.INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE' and TABLE_NAME = 'dim_hourOfDay'
            )
exec('CREATE TABLE err.dim_hourOfDay (
    [Key_hourOfDay] [int] NOT NULL,
	[hourOfDay] [int] NULL,
	[hourOfDay_full][varchar](255) NULL,

	[Local][datetime] NULL,
	[UTC][datetime] NULL,
	[offset][int] NULL,
	[ErrorCode][int] NULL,
	[ErrorColumn][int] NULL)'
);
GO


```
### ----------------------------------------CODE Hour of Day Starts HERE---------------------------------------------------


```
SELECT 
ROW_NUMBER() OVER(ORDER BY [hour]) + 100 as 'Key_hourOfDay'
	,[hour]  as 'hourOfDay'
	,CASE
	WHEN [hour] = '0' THEN 'Between 00am Midnight and 1am'
	WHEN [hour] = '1' THEN 'Between 1am and 2am'
	WHEN [hour] = '2' THEN 'Between 2am and 3am'
	WHEN [hour] = '3' THEN 'Between 3am and 4am'
	WHEN [hour] = '4' THEN 'Between 4am and 5am'
	WHEN [hour] = '5' THEN 'Between 5am and 6am'

	WHEN [hour] = '6' THEN 'Between 6am and 7am'
	WHEN [hour] = '7' THEN 'Between 7am and 8am'
	WHEN [hour] = '8' THEN 'Between 8am and 9am'
	WHEN [hour] = '9' THEN 'Between 9am and 10am'
	WHEN [hour] = '10' THEN 'Between 10am and 11am'
	WHEN [hour] = '11' THEN 'Between 11am and 12pm'

	WHEN [hour] = '12' THEN 'Between 12pm and 1pm'
	WHEN [hour] = '13' THEN 'Between 1pm and 2pm'
	WHEN [hour] = '14' THEN 'Between 2pm and 3am'
	WHEN [hour] = '15' THEN 'Between 3pm and 4pm'
	WHEN [hour] = '16' THEN 'Between 5pm and 6pm'
	WHEN [hour] = '17' THEN 'Between 5pm and 6pm'
	WHEN [hour] = '18' THEN 'Between 6pm and 7pm'
	WHEN [hour] = '19' THEN 'Between 7pm and 8pm'
	WHEN [hour] = '20' THEN 'Between 8pm and 9pm'
	WHEN [hour] = '21' THEN 'Between 9pm and 10pm'
	WHEN [hour] = '22' THEN 'Between 10pm and 11pm'
	WHEN [hour] = '23' THEN 'Between 11pm and 00am Midnight'

	END  as 'hourOfDay_full'
	,GETDATE() as 'Local'
	,GETDATE() as 'UTC'
	,DATEDIFF (hh, GETUTCDATE(), GETDATE()) as 'Offset'
      
  FROM [UK_Traffic].[stg].[traffic]
  GROUP BY [hour]


```
  ### ---------------------------Calendar------------------------------------------------------------------
  
```
  IF NOT EXISTS (SELECT TABLE_NAME 
      FROM [UK_Traffic].INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_TYPE = 'BASE TABLE' and TABLE_NAME = 'Calendar'
   )
   exec('CREATE TABLE [dim].[Calendar](
  [kDate] [int] NOT NULL,
  [Date] [datetime] NOT NULL,
  [Day] [int] NULL,
  [DayName] [nvarchar](30) NULL,
  [Week] [int] NULL,
  [ISOWeek] [int] NULL,
  [DayOfWeek] [int] NULL,
  [Month] [int] NULL,
  [MonthName] [nvarchar](30) NULL,
  [Quarter] [int] NULL,
  [Year] [int] NULL,
  [FirstOfMonth] [date] NULL,
  [LastOfYear] [date] NULL,
  [DayOfYear] [int] NULL )'
);   

GO
IF NOT EXISTS (select * 
from UK_Traffic.sys.key_constraints
where name = 'pK_Calendar'
)
exec('ALTER TABLE dim.Calendar 
  ADD CONSTRAINT pK_Calendar  PRIMARY KEY (kDate)
')
; 
--TRUNCATE TABLE dim.Calendar
 
DECLARE @StartDate  date = '20000101'; DECLARE @CutoffDate date = DATEADD(DAY, -1, DATEADD(YEAR, 25, @StartDate)); ;WITH seq(n) AS 
(
  SELECT 0 UNION ALL SELECT n + 1 FROM seq
  WHERE n < DATEDIFF(DAY, @StartDate, @CutoffDate)
),
d(d) AS 
(
  SELECT DATEADD(DAY, n, @StartDate) FROM seq
),
src AS
(
  SELECT
    kDate        = Cast(Replace(Convert(varchar(10), d), '-','') as INT),
    Date         = CONVERT(date, d),
    Day          = DATEPART(DAY,       d),
    DayName      = DATENAME(WEEKDAY,   d),
    Week         = DATEPART(WEEK,      d),
    ISOWeek      = DATEPART(ISO_WEEK,  d),
    DayOfWeek    = DATEPART(WEEKDAY,   d),
    Month        = DATEPART(MONTH,     d),
    MonthName    = DATENAME(MONTH,     d),
    Quarter      = DATEPART(Quarter,   d),
    Year         = DATEPART(YEAR,      d),
    FirstOfMonth = DATEFROMPARTS(YEAR(d), MONTH(d), 1),
    LastOfYear   = DATEFROMPARTS(YEAR(d), 12, 31),
    DayOfYear    = DATEPART(DAYOFYEAR, d)
  FROM d
)
INSERT INTO dim.Calendar
SELECT * 
FROM src
ORDER BY Date
OPTION (MAXRECURSION 0);


```
### ---------------------------------------------------------------fact DDL 255 CORRECT--------------------------------------------------------------------------------------


```

IF NOT EXISTS (SELECT *
      FROM UK_Traffic.INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_TYPE = 'BASE TABLE' and TABLE_NAME = 'traffic'
) exec('CREATE TABLE f.Fact] (
    [Count_point_id] varchar(255),
    [kDate] int,
    [key_Direction_of_Travel] int,
    [Key_hourOfDay] int,
    [Key_Local_authority_id] int,
    [Key_road_category] int,
    [Latitude] varchar(255),
    [Longitude] varchar(255),
    [Pedal_cycles] varchar(255),
    [Two_wheeled_motor_vehicles] varchar(255),
    [Cars_and_taxis] varchar(255),
    [Buses_and_coaches] varchar(255),
    [LGVs] varchar(255),
    [HGVs_2_rigid_axle] varchar(255),
    [HGVs_3_rigid_axle] varchar(255),
    [HGVs_4_or_more_rigid_axle] varchar(255),
    [HGVs_3_or_4_articulated_axle] varchar(255),
    [HGVs_5_articulated_axle] varchar(255),
    [HGVs_6_articulated_axle] varchar(255),
    [ErrorCode] int,
    [ErrorColumn] int
)'
);

--Truncate Table [f].[Fact]


```
### ---------------------------------------------------------------fact table DDL less than 255 here wrong----------------------------------------------------------------------------


```
IF NOT EXISTS (SELECT TABLE_NAME 
      FROM UK_Traffic.INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_TYPE = 'BASE TABLE' and TABLE_NAME = 'traffic'
) exec('CREATE TABLE [f].[traffic] (
    [Count_point_id] bigint,
    [kDate] int,
    [k_Direction_of_Travel] smallint,
    [k_Hour] bigint,
    [k_Local_Authority_ID] bigint,
    [k_Road_Category] bigint,
    [Latitude] varchar(50),
    [Longitude] varchar(50),
    [Pedal_cycles] bigint,
    [Two_wheeled_motor_vehicles] bigint,
    [Cars_and_taxis] bigint,
    [Buses_and_coaches] bigint,
    [LGVs] bigint,
    [HGVs_2_rigid_axle] bigint,
    [HGVs_3_rigid_axle] bigint,
    [HGVs_4_or_more_rigid_axle] bigint,
    [HGVs_3_or_4_articulated_axle] bigint,
    [HGVs_5_articulated_axle] bigint,
    [HGVs_6_articulated_axle] bigint
)'
);

Truncate Table [f].[traffic]

```

### -----------------------------------fact table code here----------------------------------------------------------------------------------------

```
SELECT [Count_point_id]
      ,cal.kDate
      ,dir.key_Direction_of_Travel
      ,hrs.Key_hourOfDay 
      ,loc.Key_Local_authority_id
      ,cat.Key_road_category
      ,[Latitude]
      ,[Longitude]
      ,[Pedal_cycles]
      ,[Two_wheeled_motor_vehicles]
      ,isnull([Cars_and_taxis], 0) as Cars_and_taxis
      ,isnull([Buses_and_coaches], 0) as Buses_and_coaches
      ,isnull([LGVs], 0) as LGVs
      ,isnull([HGVs_2_rigid_axle], 0) as HGVs_2_rigid_axle
      ,isnull([HGVs_3_rigid_axle], 0) as HGVs_3_rigid_axle
      ,isnull([HGVs_4_or_more_rigid_axle], 0) as HGVs_4_or_more_rigid_axle
      ,isnull([HGVs_3_or_4_articulated_axle], 0) as HGVs_3_or_4_articulated_axle
      ,isnull([HGVs_5_articulated_axle], 0) as HGVs_5_articulated_axle
      ,isnull([HGVs_6_articulated_axle], 0) as HGVs_6_articulated_axle
FROM [UK_Traffic].[stg].[traffic] tra
     inner join dim.direction  dir
      on tra.Direction_of_travel = dir.Direction_of_travel
    inner join dim.hourOfDay hrs
     on tra.[hour]  = hrs.hourOfDay 
     inner join dim.region loc
      on tra.Local_authority_id = loc.Local_authority_id
     inner join dim.roadcategory cat
      on tra.road_category = cat.road_category
     inner join dim.Calendar cal 
      on tra.Count_date = cal.[Date]

```

### -------------------------------------------- DROP KEYS---------------------------------------------------------------------------------------------------------------------



```

ALTER TABLE f.fact
DROP CONSTRAINT if exists fkDate
GO
ALTER TABLE f.fact
DROP CONSTRAINT if exists fk_direction
GO
ALTER TABLE f.fact
DROP CONSTRAINT if exists fk_hourOfDay
GO
ALTER TABLE f.fact
DROP CONSTRAINT if exists fk_region
GO
ALTER TABLE f.fact
DROP CONSTRAINT if exists fk_roadcategory
GO

```
### ---------------------------ADD KEYS-------------------------------------


```

ALTER TABLE f.fact
ADD CONSTRAINT fkDate FOREIGN KEY ([kDate])
REFERENCES [dim].[Calendar]([kDate]);


ALTER TABLE f.fact
ADD CONSTRAINT fk_direction FOREIGN KEY ([Key_Direction_of_travel])
REFERENCES [dim].[Direction]([Key_Direction_of_travel]);
GO
ALTER TABLE f.fact
ADD CONSTRAINT fk_hourOfDay FOREIGN KEY (Key_hourOfDay)
REFERENCES dim.[hourOfDay](Key_hourOfDay);
GO
ALTER TABLE f.fact
ADD CONSTRAINT fk_region FOREIGN KEY (key_Local_authority_id)
REFERENCES dim.[region](key_Local_authority_id);  
GO
ALTER TABLE f.fact
ADD CONSTRAINT fk_roadcategory FOREIGN KEY (Key_road_category)
REFERENCES dim.[Roadcategory] (Key_road_category);  


```
