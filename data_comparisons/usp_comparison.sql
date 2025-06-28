USE [NCA_Sandbox]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- =============================================================================================================
-- Procedure: [dbo].[usp_comparison_detailed_differences]
-- Purpose:   Compares two tables row by row based on key columns, identifies differences in comparable columns,
--            and reports detailed column-level differences using a JSON unification strategy.
-- Strategy:  JSON Unification - Uses JSON serialization and hashing to identify changed rows efficiently,
--            then uses JSON serialization and OPENJSON unpivoting to extract detailed differences
--            for only the changed rows.
--            This approach replaces direct column-by-column comparison and sql_variant hashing
--            with a more robust and potentially faster JSON-based method for both row detection and unpivoting.
-- Constraints: Preserves name, parameters, and output signature.
-- Author:    Evolution Agent (Evolver-Apex)
-- Date:      2025-06-28
-- =============================================================================================================
ALTER PROCEDURE [dbo].[usp_comparison_detailed_differences]
    @SchemaName        NVARCHAR (128) = 'dbo',
    @TableNameBefore   NVARCHAR (128),
    @TableNameAfter    NVARCHAR (128),
    @KeyColumns        NVARCHAR (MAX),
    @ExcludeColumns    NVARCHAR (MAX) = 'None',
    @ClearNulls        BIT            = 1,
    @PartitionSize     INT            = 10
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON; -- Abort transaction on error

    -- Declare variables for dynamic SQL components
    DECLARE @TableBeforeFullName NVARCHAR(257);
    DECLARE @TableAfterFullName NVARCHAR(257);
    DECLARE @KeyColsForSelect_Quoted NVARCHAR(MAX);
    DECLARE @KeyColsForSelect_Quoted_dk NVARCHAR(MAX); -- For referencing keys in #DeltaKeys
    DECLARE @KeyColsForSelect_Quoted_fd NVARCHAR(MAX); -- For referencing keys in FormattedDifferences CTE
    DECLARE @KeyJoinCondition NVARCHAR(MAX); -- For joining Before and After tables
    DECLARE @KeyJoinCondition_b_dk NVARCHAR(MAX); -- For joining Before to DeltaKeys
    DECLARE @KeyJoinCondition_a_dk NVARCHAR(MAX); -- For joining After to DeltaKeys
    DECLARE @KeyColsForOrderBy_Quoted NVARCHAR(MAX);
    DECLARE @ComparableColumnJsonList NVARCHAR(MAX); -- List of comparable columns for FOR JSON
    DECLARE @CombinedKeyExpression NVARCHAR(MAX);
    DECLARE @ExecutionSQL NVARCHAR(MAX);

    -- Temporary tables must be dropped in case of error within the CATCH block
    -- DROP TABLE IF EXISTS outside TRY/CATCH for final cleanup is also good practice
    DROP TABLE IF EXISTS #KeyColumnsTable;
    DROP TABLE IF EXISTS #ExcludeColumnsTable;
    DROP TABLE IF EXISTS #AllComparableColumns;
    -- #DeltaKeys is dropped within the dynamic SQL

    BEGIN TRY
        -- Phase 1: Pre-flight Validation and Setup

        -- Validate source tables exist
        SET @TableBeforeFullName = QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableNameBefore);
        SET @TableAfterFullName = QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableNameAfter);

        IF OBJECT_ID(@TableBeforeFullName, 'U') IS NULL
        BEGIN
            ;THROW 50000, N'Source table specified in @TableNameBefore does not exist: ' + @TableBeforeFullName, 1;
        END;

        IF OBJECT_ID(@TableAfterFullName, 'U') IS NULL
        BEGIN
            ;THROW 50000, N'Source table specified in @TableNameAfter does not exist: ' + @TableAfterFullName, 1;
        END;

        -- Setup temporary tables for keys and exclusions
        CREATE TABLE #KeyColumnsTable (ColumnName sysname PRIMARY KEY);
        INSERT INTO #KeyColumnsTable (ColumnName)
        SELECT LTRIM(RTRIM(value)) FROM STRING_SPLIT(@KeyColumns, ',') WHERE LTRIM(RTRIM(value)) <> '';

        -- Validate @KeyColumns are provided
        IF NOT EXISTS (SELECT 1 FROM #KeyColumnsTable)
        BEGIN
            -- Return empty result set with correct structure if no key columns
            SELECT
                KeyColumnsCombined = CAST(NULL AS NVARCHAR(MAX)),
                ColumnName = CAST(NULL AS sysname),
                BeforeValue = CAST(NULL AS NVARCHAR(MAX)),
                AfterValue = CAST(NULL AS NVARCHAR(MAX))
            WHERE 1 = 0;
            RETURN 0;
        END;

        CREATE TABLE #ExcludeColumnsTable (ColumnName sysname PRIMARY KEY);
        IF @ExcludeColumns IS NOT NULL AND @ExcludeColumns <> 'None'
        BEGIN
            INSERT INTO #ExcludeColumnsTable (ColumnName)
            SELECT LTRIM(RTRIM(value)) FROM STRING_SPLIT(@ExcludeColumns, ',') WHERE LTRIM(RTRIM(value)) <> '';
        END;

        -- Identify common columns existing in both tables with matching types, excluding keys and excludes
        -- Columns are ordered by column_id for consistent JSON generation regardless of table schema definition order
        SELECT
            c1.name AS ColumnName,
            c1.column_id -- Needed for consistent ordering in JSON and column lists
        INTO #AllComparableColumns
        FROM sys.columns c1
        INNER JOIN sys.tables t1 ON c1.object_id = t1.object_id
        INNER JOIN sys.schemas s1 ON t1.schema_id = s1.schema_id
        INNER JOIN sys.columns c2 ON c1.name = c2.name AND c1.system_type_id = c2.system_type_id AND c1.max_length = c2.max_length AND c1.precision = c2.precision AND c1.scale = c2.scale AND ISNULL(c1.collation_name, '') = ISNULL(c2.collation_name, '')
        INNER JOIN sys.tables t2 ON c2.object_id = t2.object_id
        INNER JOIN sys.schemas s2 ON t2.schema_id = s2.schema_id
        WHERE s1.name = @SchemaName AND t1.name = @TableNameBefore
          AND s2.name = @SchemaName AND t2.name = @TableNameAfter;

        -- Remove key columns from the comparable list
        DELETE FROM #AllComparableColumns WHERE ColumnName IN (SELECT ColumnName FROM #KeyColumnsTable);

        -- Remove excluded columns from the comparable list
        DELETE FROM #AllComparableColumns WHERE ColumnName IN (SELECT ColumnName FROM #ExcludeColumnsTable);

        -- Validate existence of Key Columns in both tables using sys.columns
        IF EXISTS (
            SELECT kc.ColumnName
            FROM #KeyColumnsTable kc
            LEFT JOIN sys.columns c1 ON c1.name = kc.ColumnName AND c1.object_id = OBJECT_ID(@TableBeforeFullName)
            LEFT JOIN sys.columns c2 ON c2.name = kc.ColumnName AND c2.object_id = OBJECT_ID(@TableAfterFullName)
            WHERE c1.object_id IS NULL OR c2.object_id IS NULL -- Column does not exist in both tables
        )
        BEGIN
            DECLARE @MissingKeyCols NVARCHAR(MAX) = (
                SELECT STRING_AGG(QUOTENAME(kc.ColumnName), ', ')
                FROM #KeyColumnsTable kc
                LEFT JOIN sys.columns c1 ON c1.name = kc.ColumnName AND c1.object_id = OBJECT_ID(@TableBeforeFullName)
                LEFT JOIN sys.columns c2 ON c2.name = kc.ColumnName AND c2.object_id = OBJECT_ID(@TableAfterFullName)
                WHERE c1.object_id IS NULL OR c2.object_id IS NULL
            );
            ;THROW 50000, N'One or more specified @KeyColumns do not exist in both source tables: ' + @MissingKeyCols, 1;
        END;

        -- Validate existence of Excluded Columns in both tables using sys.columns
         IF EXISTS (
            SELECT ec.ColumnName
            FROM #ExcludeColumnsTable ec
            LEFT JOIN sys.columns c1 ON c1.name = ec.ColumnName AND c1.object_id = OBJECT_ID(@TableBeforeFullName)
            LEFT JOIN sys.columns c2 ON c2.name = ec.ColumnName AND c2.object_id = OBJECT_ID(@TableAfterFullName)
            WHERE c1.object_id IS NULL OR c2.object_id IS NULL -- Column does not exist in both tables
        )
        BEGIN
            DECLARE @MissingExcludeCols NVARCHAR(MAX) = (
                SELECT STRING_AGG(QUOTENAME(ec.ColumnName), ', ')
                FROM #ExcludeColumnsTable ec
                LEFT JOIN sys.columns c1 ON c1.name = ec.ColumnName AND c1.object_id = OBJECT_ID(@TableBeforeFullName)
                LEFT JOIN sys.columns c2 ON c2.name = ec.ColumnName AND c2.object_id = OBJECT_ID(@TableAfterFullName)
                WHERE c1.object_id IS NULL OR c2.object_id IS NULL
            );
            ;THROW 50000, N'One or more specified @ExcludeColumns do not exist in both source tables: ' + @MissingExcludeCols, 1;
        END;

        -- Build dynamic SQL components
        SELECT @KeyColsForSelect_Quoted = STRING_AGG(QUOTENAME(ColumnName), ', ') WITHIN GROUP (ORDER BY ColumnName) FROM #KeyColumnsTable;
        SELECT @KeyColsForSelect_Quoted_dk = STRING_AGG(N'dk.' + QUOTENAME(ColumnName), ', ') WITHIN GROUP (ORDER BY ColumnName) FROM #KeyColumnsTable;
        SELECT @KeyColsForSelect_Quoted_fd = STRING_AGG(N'fd.' + QUOTENAME(ColumnName), ', ') WITHIN GROUP (ORDER BY ColumnName) FROM #KeyColumnsTable;

        -- Build join conditions referencing appropriate aliases (b, a, dk)
        SELECT @KeyJoinCondition = STRING_AGG(N'b.' + QUOTENAME(k.ColumnName) + N' = a.' + QUOTENAME(k.ColumnName), N' AND ') WITHIN GROUP (ORDER BY k.ColumnName) FROM #KeyColumnsTable k;
        SELECT @KeyJoinCondition_b_dk = STRING_AGG(N'b.' + QUOTENAME(k.ColumnName) + N' = dk.' + QUOTENAME(k.ColumnName), N' AND ') WITHIN GROUP (ORDER BY k.ColumnName) FROM #KeyColumnsTable k;
        SELECT @KeyJoinCondition_a_dk = STRING_AGG(N'a.' + QUOTENAME(k.ColumnName) + N' = dk.' + QUOTENAME(k.ColumnName), N' AND ') WITHIN GROUP (ORDER BY k.ColumnName) FROM #KeyColumnsTable k;

        SELECT @KeyColsForOrderBy_Quoted = STRING_AGG(QUOTENAME(ColumnName), ', ') WITHIN GROUP (ORDER BY ColumnName) FROM #KeyColumnsTable;

        -- List of comparable columns formatted as quoted names for use in SELECT statements within FOR JSON subqueries
        SELECT @ComparableColumnJsonList = STRING_AGG(QUOTENAME(ColumnName), ', ') WITHIN GROUP (ORDER BY column_id) FROM #AllComparableColumns;

        -- Determine the content of the SELECT statement for FOR JSON based on whether comparable columns exist
        DECLARE @JsonSelectContentForHash NVARCHAR(MAX); -- For hashing CTEs (uses src alias)
        DECLARE @JsonSelectContentForDelta_b NVARCHAR(MAX); -- For DeltaDataWithJson CTE (uses b alias)
        DECLARE @JsonSelectContentForDelta_a NVARCHAR(MAX); -- For DeltaDataWithJson CTE (uses a alias)

        IF @ComparableColumnJsonList IS NULL OR @ComparableColumnJsonList = ''
        BEGIN
             -- If no comparable columns, select a value that produces NULL JSON (which HASHBYTES handles with ISNULL)
             SET @JsonSelectContentForHash = N'(SELECT 1 WHERE 1=0 FOR JSON PATH)'; -- Produces NULL JSON
             -- For delta, also produce NULL JSON
             SET @JsonSelectContentForDelta_b = N'(SELECT 1 WHERE 1=0 FOR JSON PATH)'; -- Produces NULL JSON
             SET @JsonSelectContentForDelta_a = N'(SELECT 1 WHERE 1=0 FOR JSON PATH)'; -- Produces NULL JSON
        END
        ELSE
        BEGIN
             -- Otherwise, select the actual comparable columns from the source table alias (src) for hashing
             SET @JsonSelectContentForHash = N'(SELECT src.' + REPLACE(@ComparableColumnJsonList, ', ', ', src.') + N' FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)';
             -- For delta, select from the outer alias (b or a) within a derived table for FOR JSON
             SET @JsonSelectContentForDelta_b = N'(SELECT b.' + REPLACE(@ComparableColumnJsonList, ', ', ', b.') + N' FROM (SELECT b.' + REPLACE(@ComparableColumnJsonList, ', ', ', b.') + N') AS d FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)';
             SET @JsonSelectContentForDelta_a = N'(SELECT a.' + REPLACE(@ComparableColumnJsonList, ', ', ', a.') + N' FROM (SELECT a.' + REPLACE(@ComparableColumnJsonList, ', ', ', a.') + N') AS d FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)';
        END

        -- Build the expression for the combined key string, referencing columns in the FormattedDifferences CTE alias (fd)
        IF (SELECT COUNT(*) FROM #KeyColumnsTable) > 1
        BEGIN
            SELECT @CombinedKeyExpression = N'CONCAT_WS('','', ' + STRING_AGG(N'CAST(fd.' + QUOTENAME(ColumnName) + N' AS NVARCHAR(MAX))', N', ') WITHIN GROUP (ORDER BY ColumnName) + N')' FROM #KeyColumnsTable;
        END
        ELSE
        BEGIN
            -- If only one key, no concatenation needed, just cast the single column
            SELECT @CombinedKeyExpression = N'CAST(fd.' + QUOTENAME(ColumnName) + N' AS NVARCHAR(MAX))' FROM #KeyColumnsTable;
        END;

        -- Phase 2 & 3: Dynamic SQL Execution Core (JSON Unification Model)

        SET @ExecutionSQL = N'
        -- Temp table for delta keys - dropped before creation
        DROP TABLE IF EXISTS #DeltaKeys;

        -- CTEs to generate JSON hashes for Before and After tables
        -- Uses comparable columns to create a canonical row representation for hashing
        -- Note: HASHBYTES(''SHA2_256'', NULL) returns NULL, hence ISNULL(..., N'''')
        WITH BeforeJsonHash AS (
            SELECT
                -- Select Key Columns from the source table (alias src)
                {KeyColsForSelect_Quoted},
                -- Generate JSON for comparable columns FOR THE CURRENT ROW (src) and hash it
                JsonHash = HASHBYTES(''SHA2_256'', ISNULL({JsonSelectContentForHash}, N''''))
            FROM {TableBeforeFullName} AS src
        ),
        AfterJsonHash AS (
            SELECT
                -- Select Key Columns from the source table (alias src)
                {KeyColsForSelect_Quoted},
                 -- Generate JSON for comparable columns FOR THE CURRENT ROW (src) and hash it
                JsonHash = HASHBYTES(''SHA2_256'', ISNULL({JsonSelectContentForHash}, N''''))
            FROM {TableAfterFullName} AS src
        ),
        -- Identify keys of rows that have changed or are only in one table (inserts/deletes)
        DeltaKeyRows AS (
            SELECT {KeyColsForSelect_Quoted}
            FROM BeforeJsonHash AS b
            FULL OUTER JOIN AfterJsonHash AS a ON {KeyJoinCondition}
            -- Rows differ if hashes don''t match or one side is missing (insert/delete)
            WHERE b.JsonHash <> a.JsonHash -- Compares hashes of JSON representations
               OR b.JsonHash IS NULL -- Row exists in After but not Before
               OR a.JsonHash IS NULL -- Row exists in Before but not After
        )
        -- Stage the delta keys in a temp table for efficient subsequent joins
        SELECT {KeyColsForSelect_Quoted}
        INTO #DeltaKeys
        FROM DeltaKeyRows;

        -- Create clustered index on delta keys for efficient joining back to source tables
        CREATE CLUSTERED INDEX IX_DeltaKeys ON #DeltaKeys ({KeyColsForOrderBy_Quoted});

        -- CTE to get the full data for delta rows and generate Before/After JSON for unpivoting
        -- LEFT JOINs from #DeltaKeys handle inserts/deletes (one side will be NULL)
        WITH DeltaDataWithJson AS (
            SELECT
                {KeyColsForSelect_Quoted_dk}, -- Select key columns from #DeltaKeys
                -- Generate JSON for comparable columns for the BEFORE side of this delta row
                -- ISNULL handles cases where the LEFT JOIN to ''b'' found no row (deleted row)
                BeforeJson = ISNULL({JsonSelectContentForDelta_b}, N''''),
                -- Generate JSON for comparable columns for the AFTER side of this delta row
                -- ISNULL handles cases where the LEFT JOIN to ''a'' found no row (inserted row)
                AfterJson = ISNULL({JsonSelectContentForDelta_a}, N'''')
            FROM #DeltaKeys AS dk
            LEFT JOIN {TableBeforeFullName} AS b ON {KeyJoinCondition_b_dk}
            LEFT JOIN {TableAfterFullName} AS a ON {KeyJoinCondition_a_dk}
        ),
        -- Unpivot the Before and After JSON using OPENJSON and join the results by column name ([key])
        -- FULL OUTER JOIN ensures columns present in only one JSON are included (e.g. new columns in After with data)
        -- Note: Columns that are NULL on BOTH sides in the source tables will not appear in the JSON
        -- generated by FOR JSON (default behavior) and thus not be unpivoted by OPENJSON.
        -- This is desired behavior for detailed *differences*.
        UnpivotedDifferences AS (
            SELECT
                dd.{KeyColsForSelect_Quoted_dk}, -- Pass key columns through
                COALESCE(bj.[key], aj.[key]) AS ColumnName, -- Get the column name from either side
                CAST(bj.[value] AS NVARCHAR(MAX)) AS BeforeValue_Raw, -- Get the value from Before JSON (as NVARCHAR(MAX))
                CAST(aj.[value] AS NVARCHAR(MAX)) AS AfterValue_Raw   -- Get the value from After JSON (as NVARCHAR(MAX))
            FROM DeltaDataWithJson AS dd
            OUTER APPLY OPENJSON(dd.BeforeJson) AS bj -- Use OUTER APPLY to handle empty/NULL JSON
            FULL OUTER JOIN OPENJSON(dd.AfterJson) AS aj ON bj.[key] = aj.[key] -- Join on column name ([key])
            WHERE bj.[key] IS NOT NULL OR aj.[key] IS NOT NULL -- Exclude rows if both JSONs were empty/NULL, resulting in OPENJSON producing empty sets
        ),
        -- Filter for actual differences (handling NULL vs non-NULL and different non-NULLs)
        FilteredDifferences AS (
            SELECT
                {KeyColsForSelect_Quoted_dk}, -- Pass key columns through
                ud.ColumnName,
                ud.BeforeValue_Raw,
                ud.AfterValue_Raw
            FROM UnpivotedDifferences AS ud
            -- A difference exists if:
            -- 1. Before is NULL and After is NOT NULL
            -- 2. Before is NOT NULL and After is NULL
            -- 3. Before and After are NOT NULL and have different values (standard comparison)
            WHERE (ud.BeforeValue_Raw IS NULL AND ud.AfterValue_Raw IS NOT NULL)
               OR (ud.BeforeValue_Raw IS NOT NULL AND ud.AfterValue_Raw IS NULL)
               OR (ud.BeforeValue_Raw <> ud.AfterValue_Raw)
        ),
        -- Format output, apply ClearNulls, and build combined key string
        FormattedDifferences AS (
            SELECT
                -- Select key columns explicitly using the CTE alias (fd)
                {KeyColsForSelect_Quoted_fd},
                ColumnName,
                -- Apply @ClearNulls logic: replace NULL with empty string if @ClearNulls = 1
                BeforeValue = CASE WHEN @ClearNulls = 1 THEN COALESCE(BeforeValue_Raw, N'''') ELSE BeforeValue_Raw END,
                AfterValue = CASE WHEN @ClearNulls = 1 THEN COALESCE(AfterValue_Raw, N'''') ELSE AfterValue_Raw END,
                -- Build the combined key string using columns from the current CTE (fd)
                KeyColumnsCombined = {CombinedKeyExpression}
            FROM FilteredDifferences AS fd -- Alias for clarity
        ),
        -- Apply PartitionSize using ROW_NUMBER() to limit results per column
        PartitionedResults AS (
            SELECT
                KeyColumnsCombined,
                ColumnName,
                BeforeValue,
                AfterValue,
                -- Partition by Column Name and order by Key Columns to limit differences per column
                rn = ROW_NUMBER() OVER (PARTITION BY ColumnName ORDER BY {KeyColsForOrderBy_Quoted})
            FROM FormattedDifferences
        )
        -- Final selection of the required columns, ordered as specified by output signature
        SELECT
            KeyColumnsCombined,
            ColumnName,
            BeforeValue,
            AfterValue
        FROM PartitionedResults
        WHERE rn <= @PartitionSize
        ORDER BY ColumnName, KeyColumnsCombined -- Final output order: by column, then by key
        OPTION (RECOMPILE); -- Use recompile as table/column names are dynamic

        -- Cleanup staged delta keys
        DROP TABLE IF EXISTS #DeltaKeys;
        ';

        -- Replace placeholders with generated dynamic SQL parts
        SET @ExecutionSQL = REPLACE(@ExecutionSQL, '{KeyColsForSelect_Quoted}', @KeyColsForSelect_Quoted);
        SET @ExecutionSQL = REPLACE(@ExecutionSQL, '{KeyColsForSelect_Quoted_dk}', @KeyColsForSelect_Quoted_dk);
        SET @ExecutionSQL = REPLACE(@ExecutionSQL, '{KeyColsForSelect_Quoted_fd}', @KeyColsForSelect_Quoted_fd);
        SET @ExecutionSQL = REPLACE(@ExecutionSQL, '{KeyJoinCondition}', @KeyJoinCondition);
        SET @ExecutionSQL = REPLACE(@ExecutionSQL, '{KeyJoinCondition_b_dk}', @KeyJoinCondition_b_dk);
        SET @ExecutionSQL = REPLACE(@ExecutionSQL, '{KeyJoinCondition_a_dk}', @KeyJoinCondition_a_dk);
        SET @ExecutionSQL = REPLACE(@ExecutionSQL, '{KeyColsForOrderBy_Quoted}', @KeyColsForOrderBy_Quoted);
        SET @ExecutionSQL = REPLACE(@ExecutionSQL, '{CombinedKeyExpression}', @CombinedKeyExpression);
        SET @ExecutionSQL = REPLACE(@ExecutionSQL, '{TableBeforeFullName}', @TableBeforeFullName);
        SET @ExecutionSQL = REPLACE(@ExecutionSQL, '{TableAfterFullName}', @TableAfterFullName);

        -- Replace the JSON select content placeholders
        SET @ExecutionSQL = REPLACE(@ExecutionSQL, '{JsonSelectContentForHash}', @JsonSelectContentForHash);
        SET @ExecutionSQL = REPLACE(@ExecutionSQL, '{JsonSelectContentForDelta_b}', @JsonSelectContentForDelta_b);
        SET @ExecutionSQL = REPLACE(@ExecutionSQL, '{JsonSelectContentForDelta_a}', @JsonSelectContentForDelta_a);

        -- Execute the dynamic SQL using sp_executesql for security (parameterization)
        EXEC sys.sp_executesql
            @ExecutionSQL,
            N'@ClearNulls BIT, @PartitionSize INT', -- Parameters passed into dynamic SQL
            @ClearNulls = @ClearNulls,
            @PartitionSize = @PartitionSize;

    END TRY
    BEGIN CATCH
        -- Clean up temporary tables on error using OBJECT_ID check
        IF OBJECT_ID('tempdb..#KeyColumnsTable') IS NOT NULL DROP TABLE #KeyColumnsTable;
        IF OBJECT_ID('tempdb..#ExcludeColumnsTable') IS NOT NULL DROP TABLE #ExcludeColumnsTable;
        IF OBJECT_ID('tempdb..#AllComparableColumns') IS NOT NULL DROP TABLE #AllComparableColumns;
        IF OBJECT_ID('tempdb..#DeltaKeys') IS NOT NULL DROP TABLE #DeltaKeys; -- Dropped explicitly in dynamic SQL, but included for safety

        -- Re-throw the error to provide necessary context to the caller
        THROW;
    END CATCH

    -- Clean up temporary tables on success using OBJECT_ID check
    IF OBJECT_ID('tempdb..#KeyColumnsTable') IS NOT NULL DROP TABLE #KeyColumnsTable;
    IF OBJECT_ID('tempdb..#ExcludeColumnsTable') IS NOT NULL DROP TABLE #ExcludeColumnsTable;
    IF OBJECT_ID('tempdb..#AllComparableColumns') IS NOT NULL DROP TABLE #AllComparableColumns;
    -- #DeltaKeys is dropped within the dynamic SQL

END;
GO
