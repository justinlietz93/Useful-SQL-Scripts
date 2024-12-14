/******************************************************************************
					   
Object description:
Comprehensive Table Comparison 4.2.0	

<Date>		<Developer Name>	<Description of Change>
2024-12-13	Justin Lietz		Created Script

Server:  xxxxx
Database: xxxxx
******************************************************************************/


DECLARE @SchemaName sysname          = 'dbo';
DECLARE @TableNameAfter sysname      = 'after';    -- Replace with your 'after' table name
DECLARE @TableNameBefore sysname     = 'before';   -- Replace with your 'before' table name

DECLARE @KeyColumns NVARCHAR(MAX)     = 'keys'  -- ( Unique record identifiers )
DECLARE @ExcludeColumns NVARCHAR(MAX) = ''  -- ( This excludes columns from comparison )
DECLARE @ClearNulls INT			      = 0  -- ( 1 Sets NULL = '' )
										   -- ( 0 Sets NULL != '' )

										   

------------------------------------------------------------------------------------------------------------------------
----------------------- END USER INPUTS --------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
























------------------------------------------------------------------------------------------------------------------------
----------------------- BEGIN SCRIPT -----------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------


BEGIN

	------------------------------------------------------------------------------------------------------------------------
    -- Create column storage tables
   ------------------------------------------------------------------------------------------------------------------------
	DECLARE @KeyColumnsTable TABLE (ColumnName sysname);
	-- Parse @KeyColumns into a table variable
	INSERT INTO @KeyColumnsTable (ColumnName)
	SELECT LTRIM(RTRIM(value))
	FROM STRING_SPLIT(@KeyColumns, ',');

	-- Raise error if no key columns provided
    IF NOT EXISTS (SELECT 1 FROM @KeyColumnsTable)
    BEGIN
        RAISERROR('No key columns provided. Please specify @KeyColumns to uniquely identify and join records.', 16, 1);
        RETURN;
    END

	-- Parse @ExcludeColumns into a table variable
    DECLARE @ExcludeColumnsTable TABLE (ColumnName sysname);
    INSERT INTO @ExcludeColumnsTable (ColumnName)
    SELECT LTRIM(RTRIM(value))
    FROM STRING_SPLIT(@ExcludeColumns, ',');


    ------------------------------------------------------------------------------------------------------------------------
    -- Build column names list from both tables, excluding key columns
   ------------------------------------------------------------------------------------------------------------------------
    DECLARE @AfterColumns TABLE (ColumnName sysname, DataType sysname);
    INSERT INTO @AfterColumns (ColumnName, DataType)
    SELECT COLUMN_NAME, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = @SchemaName
      AND TABLE_NAME = @TableNameAfter
      AND COLUMN_NAME NOT IN (SELECT ColumnName FROM @KeyColumnsTable)
	  AND COLUMN_NAME NOT IN (SELECT ColumnName FROM @ExcludeColumnsTable);

    DECLARE @BeforeColumns TABLE (ColumnName sysname, DataType sysname);
    INSERT INTO @BeforeColumns (ColumnName, DataType)
    SELECT COLUMN_NAME, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = @SchemaName
      AND TABLE_NAME = @TableNameBefore
      AND COLUMN_NAME NOT IN (SELECT ColumnName FROM @KeyColumnsTable)
	  AND COLUMN_NAME NOT IN (SELECT ColumnName FROM @ExcludeColumnsTable);

    -- Join columns from both tables on ColumnName
    DECLARE @ColumnNamesTable TABLE (ColumnName sysname, DataType sysname);
    INSERT INTO @ColumnNamesTable (ColumnName, DataType)
    SELECT a.ColumnName, a.DataType
    FROM @AfterColumns a
    JOIN @BeforeColumns b ON a.ColumnName = b.ColumnName; -- Include columns regardless of data type

    -- Identify columns with mismatched data types
    DECLARE @MismatchedColumns TABLE (ColumnName sysname, DataTypeAfter sysname, DataTypeBefore sysname);
    INSERT INTO @MismatchedColumns (ColumnName, DataTypeAfter, DataTypeBefore)
    SELECT a.ColumnName, a.DataType, b.DataType
    FROM @AfterColumns a
    JOIN @BeforeColumns b ON a.ColumnName = b.ColumnName AND a.DataType <> b.DataType;

    -- Error message for mismatched data types
    IF EXISTS (SELECT 1 FROM @MismatchedColumns)
    BEGIN
        PRINT 'Warning: The following columns have mismatched data types between the after and before tables:';
        SELECT ColumnName, DataTypeAfter, DataTypeBefore FROM @MismatchedColumns;
    END

    -- Throw error if no columns were found
    IF NOT EXISTS (SELECT 1 FROM @ColumnNamesTable)
    BEGIN
        RAISERROR('No matching columns found between the tables %s.%s.%s and %s.%s.%s. 
				   First verify that you are running this script in the database where the tables exist.
                   Also ensure that both tables have columns with the same names, excluding key columns.', 
                   16, 1, @SchemaName, @TableNameAfter, @SchemaName, @TableNameBefore);
        RETURN;
    END

	SELECT  ServerName   = @@SERVERNAME
		   ,DatabaseName = DB_NAME()
		   ,BeforeTable  = @SchemaName + '.' + @TableNameBefore
		   ,AfterTable   = @SchemaName + '.' + @TableNameAfter


    ------------------------------------------------------------------------------------------------------------------------
    -- Build clauses for key columns
    ------------------------------------------------------------------------------------------------------------------------
    DECLARE @KeyColumnSelectList NVARCHAR(MAX) = '';
    DECLARE @KeyColumnSelectListWithAlias NVARCHAR(MAX) = '';
    DECLARE @KeyColumnJoinConditions NVARCHAR(MAX) = '';
    DECLARE @KeyColumnGroupBy NVARCHAR(MAX) = '';
	DECLARE @KeyColumnOrderBy NVARCHAR(MAX) = '';
    DECLARE @ColumnOrderBy NVARCHAR(MAX) = '';

    -- Build the key column lists
    SELECT
        @KeyColumnSelectList = @KeyColumnSelectList + '[' + ColumnName + '],' + CHAR(13) + CHAR(10),
        @KeyColumnSelectListWithAlias = @KeyColumnSelectListWithAlias + 'a.[' + ColumnName + '] AS [' + ColumnName + '],' + CHAR(13) + CHAR(10),
        @KeyColumnJoinConditions = @KeyColumnJoinConditions + 'a.[' + ColumnName + '] = b.[' + ColumnName + '] AND ' + CHAR(13) + CHAR(10),
        @KeyColumnGroupBy = @KeyColumnGroupBy + 'a.[' + ColumnName + '],' + CHAR(13) + CHAR(10),
        @KeyColumnOrderBy = @KeyColumnOrderBy + 'a.[' + ColumnName + '],' + CHAR(13) + CHAR(10)
    FROM @KeyColumnsTable;

    -- Remove trailing commas and 'AND' from the strings
    IF LEN(@KeyColumnSelectList) >= 3
        SET @KeyColumnSelectList = LEFT(@KeyColumnSelectList, LEN(@KeyColumnSelectList) - 3);
    IF LEN(@KeyColumnSelectListWithAlias) >= 3
        SET @KeyColumnSelectListWithAlias = LEFT(@KeyColumnSelectListWithAlias, LEN(@KeyColumnSelectListWithAlias) - 3);
    IF LEN(@KeyColumnJoinConditions) >= 7
        SET @KeyColumnJoinConditions = LEFT(@KeyColumnJoinConditions, LEN(@KeyColumnJoinConditions) - 7);
    IF LEN(@KeyColumnGroupBy) >= 3
        SET @KeyColumnGroupBy = LEFT(@KeyColumnGroupBy, LEN(@KeyColumnGroupBy) - 3);
	IF LEN(@KeyColumnOrderBy) >= 3
        SET @KeyColumnOrderBy = LEFT(@KeyColumnOrderBy, LEN(@KeyColumnOrderBy) - 3);
    IF LEN(@ColumnOrderBy) >= 3
        SET @ColumnOrderBy = LEFT(@ColumnOrderBy, LEN(@ColumnOrderBy) - 3);


    ------------------------------------------------------------------------------------------------------------------------
    -- Build the SUM(CASE ...) statements used for counting differences and matches
    ------------------------------------------------------------------------------------------------------------------------
    DECLARE @ColumnsDiffSelect NVARCHAR(MAX) = '';
    DECLARE @ColumnsDiffTotalSelect NVARCHAR(MAX) = '';
    DECLARE @ColumnsMatchSelect NVARCHAR(MAX) = '';
    DECLARE @ColumnsMatchTotalSelect NVARCHAR(MAX) = '';
    DECLARE @ColumnsUnpivotList_Diff NVARCHAR(MAX) = '';
    DECLARE @ColumnsUnpivotList_Match NVARCHAR(MAX) = '';

    -- Set NULL equal to blank based on user input
	IF COALESCE(@ClearNulls, 0) = 1
	BEGIN
		-- Build CASE statements for individual differences (NULL = '')
		SELECT @ColumnsDiffSelect += ',
			SUM(CASE 
				WHEN CAST(COALESCE(CAST(a.[' + ColumnName + '] AS VARCHAR), '''') AS NVARCHAR(MAX)) 
					 <> CAST(COALESCE(CAST(b.[' + ColumnName + '] AS VARCHAR), '''') AS NVARCHAR(MAX))
				THEN 1 
				ELSE 0 
			END) AS [' + ColumnName + '_Diff]'
		FROM @ColumnNamesTable;

		-- Build CASE statements for individual matches (NULL = '')
		SELECT @ColumnsMatchSelect += ',
			SUM(CASE 
				WHEN CAST(COALESCE(CAST(a.[' + ColumnName + '] AS VARCHAR), '''') AS NVARCHAR(MAX)) 
					 = CAST(COALESCE(CAST(b.[' + ColumnName + '] AS VARCHAR), '''') AS NVARCHAR(MAX))
				THEN 1 
				ELSE 0 
			END) AS [' + ColumnName + '_Match]'
		FROM @ColumnNamesTable;
	END
	ELSE IF COALESCE(@ClearNulls, 0) = 0
	BEGIN

		-- Build CASE statements for individual differences (NULL <> '')
		SELECT @ColumnsDiffSelect += ',
			SUM(CASE 
				WHEN CAST(a.[' + ColumnName + '] AS NVARCHAR(MAX)) <> CAST(b.[' + ColumnName + '] AS NVARCHAR(MAX)) 
					 OR (a.[' + ColumnName + '] IS NULL AND b.[' + ColumnName + '] IS NOT NULL)
					 OR (a.[' + ColumnName + '] IS NOT NULL AND b.[' + ColumnName + '] IS NULL)
				THEN 1 
				ELSE 0 
			END) AS [' + ColumnName + '_Diff]'
		FROM @ColumnNamesTable;

		-- Build CASE statements for individual matches (NULL <> '')
		SELECT @ColumnsMatchSelect += ',
			SUM(CASE 
				WHEN CAST(a.[' + ColumnName + '] AS NVARCHAR(MAX)) = CAST(b.[' + ColumnName + '] AS NVARCHAR(MAX)) 
					 OR (a.[' + ColumnName + '] IS NULL AND b.[' + ColumnName + '] IS NULL)
				THEN 1 
				ELSE 0 
			END) AS [' + ColumnName + '_Match]'
		FROM @ColumnNamesTable;
	END

    -- Build SELECT SUM columns for total differences
    SELECT @ColumnsDiffTotalSelect = @ColumnsDiffTotalSelect + ',
    SUM([' + ColumnName + '_Diff]) AS [' + ColumnName + '_Diff_Total]'
    FROM @ColumnNamesTable;

    -- Build SELECT SUM columns for total matches
    SELECT @ColumnsMatchTotalSelect = @ColumnsMatchTotalSelect + ',
    SUM([' + ColumnName + '_Match]) AS [' + ColumnName + '_Match_Total]'
    FROM @ColumnNamesTable;

    -- Remove the leading commas from the total select variables
    IF LEFT(@ColumnsDiffTotalSelect, 1) = ','
        SET @ColumnsDiffTotalSelect = STUFF(@ColumnsDiffTotalSelect, 1, 1, '');
    IF LEFT(@ColumnsMatchTotalSelect, 1) = ','
        SET @ColumnsMatchTotalSelect = STUFF(@ColumnsMatchTotalSelect, 1, 1, '');


    ------------------------------------------------------------------------------------------------------------------------
    -- Build the list of columns for UNPIVOT
    ------------------------------------------------------------------------------------------------------------------------
    SELECT @ColumnsUnpivotList_Diff = @ColumnsUnpivotList_Diff + '[' + ColumnName + '_Diff_Total],'
    FROM @ColumnNamesTable;

    SELECT @ColumnsUnpivotList_Match = @ColumnsUnpivotList_Match + '[' + ColumnName + '_Match_Total],'
    FROM @ColumnNamesTable;

    -- Remove the last comma from @ColumnsUnpivotList_Diff and @ColumnsUnpivotList_Match
    IF LEN(@ColumnsUnpivotList_Diff) >= 1
        SET @ColumnsUnpivotList_Diff = LEFT(@ColumnsUnpivotList_Diff, LEN(@ColumnsUnpivotList_Diff) - 1);

    IF LEN(@ColumnsUnpivotList_Match) >= 1
        SET @ColumnsUnpivotList_Match = LEFT(@ColumnsUnpivotList_Match, LEN(@ColumnsUnpivotList_Match) - 1);

    DECLARE @sql NVARCHAR(MAX);
    DECLARE @ParmDefinition NVARCHAR(500);
    DECLARE @TotalComparedRows INT;

    SET @ParmDefinition = N'@TotalComparedRows INT OUTPUT';


    ------------------------------------------------------------------------------------------------------------------------
    -- Assemble generated script
    ------------------------------------------------------------------------------------------------------------------------
    SET @sql = N'
    SET NOCOUNT ON;

    DROP TABLE IF EXISTS #Diffs;

    SELECT
    ' + @KeyColumnSelectListWithAlias + '
    ' + @ColumnsDiffSelect + '
    ' + @ColumnsMatchSelect + '
    INTO #Diffs
    FROM [' + @SchemaName + '].[' + @TableNameAfter + '] a
    JOIN [' + @SchemaName + '].[' + @TableNameBefore + '] b 
        ON ' + @KeyColumnJoinConditions + '
    GROUP BY ' + @KeyColumnGroupBy + ';


    SELECT @TotalComparedRows = COUNT(*) FROM #Diffs;

    DROP TABLE IF EXISTS #Diff_Sums;

    SELECT
    ' + @ColumnsDiffTotalSelect + '
    , ' + @ColumnsMatchTotalSelect + '
    INTO #Diff_Sums
    FROM #Diffs;

    -- Unpivot differences
    DROP TABLE IF EXISTS #FinalDiff_Diff;

    SELECT REPLACE(ColumnName, ''_Diff_Total'', '''') AS ColumnName, Value AS Differences
    INTO #FinalDiff_Diff
    FROM #Diff_Sums
    UNPIVOT
    (
        Value FOR ColumnName IN (' + @ColumnsUnpivotList_Diff + ')
    ) u;

    -- Unpivot matches
    DROP TABLE IF EXISTS #FinalDiff_Match;

    SELECT REPLACE(ColumnName, ''_Match_Total'', '''') AS ColumnName, Value AS Matches
    INTO #FinalDiff_Match
    FROM #Diff_Sums
    UNPIVOT
    (
        Value FOR ColumnName IN (' + @ColumnsUnpivotList_Match + ')
    ) u;

    -- Combine differences and matches
    DROP TABLE IF EXISTS #FinalDiff;

    SELECT
        d.ColumnName,
        d.Differences,
        m.Matches,
        (d.Differences + m.Matches) AS TotalComparedRows,
        CASE WHEN (d.Differences + m.Matches) > 0 THEN (m.Matches * 100.0)/(d.Differences + m.Matches) ELSE NULL END AS MatchingPercentage
    INTO #FinalDiff
    FROM #FinalDiff_Diff d
    JOIN #FinalDiff_Match m ON d.ColumnName = m.ColumnName;

    SELECT * FROM #FinalDiff ORDER BY MatchingPercentage ASC;

	DECLARE @TotalRecordsBefore INT;
	DECLARE @TotalRecordsAfter INT;

	-- Count total records in the "before" table
	SELECT @TotalRecordsBefore = COUNT(*)
	FROM [' + @SchemaName + '].[' + @TableNameBefore + '];

	-- Count total records in the "after" table
	SELECT @TotalRecordsAfter = COUNT(*)
	FROM [' + @SchemaName + '].[' + @TableNameAfter + '];

    -- Compute overall matching percentage
    SELECT
        SUM(Differences) AS TotalDifferences,
        SUM(Matches) AS TotalMatches,
        (SUM(Differences) + SUM(Matches)) AS TotalComparedCells,
        CASE WHEN (SUM(Differences) + SUM(Matches)) > 0 THEN (SUM(Matches) * 100.0)/(SUM(Differences) + SUM(Matches)) ELSE NULL END AS OverallMatchingPercentage
		,TotalRecordsBefore = @TotalRecordsBefore
		,TotalRecordsAfter = @TotalRecordsAfter
    INTO #OverallStats
    FROM #FinalDiff;

    SELECT * FROM #OverallStats;
    ';

    -- Execute dynamic SQL to get counts of differences and matches
    EXEC sys.sp_executesql @sql, @ParmDefinition, @TotalComparedRows = @TotalComparedRows OUTPUT;


    ------------------------------------------------------------------------------------------------------------------------
    -- Generate the detailed differences with before and after values
    ------------------------------------------------------------------------------------------------------------------------

    -- Build CROSS APPLY VALUES clause for the columns used to show individual differences
    DECLARE @CrossApplyValues NVARCHAR(MAX);

    SELECT @CrossApplyValues = STUFF((
        SELECT ',' + CHAR(13) + CHAR(10) + '(''' + ColumnName + ''', CAST(b.[' + ColumnName + '] AS NVARCHAR(MAX)), CAST(a.[' + ColumnName + '] AS NVARCHAR(MAX)))'
        FROM @ColumnNamesTable
        FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 1, '');

     -- Build final dynamic SQL for detailed differences with conditional WHERE clause
    DECLARE @DetailedDiffSQL NVARCHAR(MAX);

    IF COALESCE(@ClearNulls, 0) = 1
    BEGIN
        SET @DetailedDiffSQL = '
        SELECT DISTINCT ' + @KeyColumnSelectListWithAlias + ',
            v.ColumnName,
            v.BeforeValue,
            v.AfterValue
        FROM
            [' + @SchemaName + '].[' + @TableNameAfter + '] a
            JOIN [' + @SchemaName + '].[' + @TableNameBefore + '] b ON ' + @KeyColumnJoinConditions + '
            CROSS APPLY
            (
                VALUES
                    ' + @CrossApplyValues + ''
            ) v(ColumnName, BeforeValue, AfterValue)
        WHERE
            CAST(COALESCE(v.BeforeValue, '''') AS NVARCHAR(MAX)) <> CAST(COALESCE(v.AfterValue, '''') AS NVARCHAR(MAX))
        ORDER BY v.ColumnName, ' + @KeyColumnOrderBy + ';
        ';
    END
    ELSE IF COALESCE(@ClearNulls, 0) = 0
    BEGIN
        SET @DetailedDiffSQL = '
        SELECT DISTINCT ' + @KeyColumnSelectListWithAlias + ',
            v.ColumnName,
            v.BeforeValue,
            v.AfterValue
        FROM
            [' + @SchemaName + '].[' + @TableNameAfter + '] a
            JOIN [' + @SchemaName + '].[' + @TableNameBefore + '] b ON ' + @KeyColumnJoinConditions + '
            CROSS APPLY
            (
                VALUES
                    ' + @CrossApplyValues + '
            ) v(ColumnName, BeforeValue, AfterValue)
        WHERE
            (
                (v.BeforeValue <> v.AfterValue 
    				OR (v.BeforeValue IS NULL AND v.AfterValue IS NOT NULL) 
    				OR (v.BeforeValue IS NOT NULL AND v.AfterValue IS NULL)
    			)
            )
        ORDER BY v.ColumnName, ' + @KeyColumnOrderBy + ';
        ';
    END

    -- Execute dynamic SQL to get detailed differences
    EXEC sys.sp_executesql @DetailedDiffSQL;
END
