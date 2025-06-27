USE [NCA_Sandbox];
GO

-- Step 1: Drop the old procedure completely to ensure a clean slate.
IF OBJECT_ID('dbo.spn_nh_rpt_comparison_detailed_differences', 'P') IS NOT NULL
BEGIN
    DROP PROCEDURE [dbo].[spn_nh_rpt_comparison_detailed_differences];
    PRINT 'Old procedure dropped successfully.';
END
GO

-- Step 2: Create the procedure from scratch with all fixes applied.
CREATE PROCEDURE [dbo].[spn_nh_rpt_comparison_detailed_differences]
	@SchemaName		 NVARCHAR (128) = 'dbo'
   ,@TableNameBefore NVARCHAR (128)
   ,@TableNameAfter	 NVARCHAR (128)
   ,@KeyColumns		 NVARCHAR (MAX)
   ,@ExcludeColumns	 NVARCHAR (MAX) = 'None'
   ,@ClearNulls		 BIT			= 1
   ,@PartitionSize	 INT			= 10
AS
BEGIN
	SET NOCOUNT ON;
	SET XACT_ABORT ON;

	DROP TABLE IF EXISTS #KeyColumnsTable;
	DROP TABLE IF EXISTS #ExcludeColumnsTable;
	DROP TABLE IF EXISTS #ColumnNamesTable;

	BEGIN TRY
		-- 1. Validate source tables exist
		DECLARE @TableBeforeFullName NVARCHAR(257) = QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableNameBefore);
		DECLARE @TableAfterFullName NVARCHAR(257) = QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableNameAfter);

		IF OBJECT_ID(@TableBeforeFullName, 'U') IS NULL
		BEGIN
			;THROW 50000, 'Source table specified in @TableNameBefore does not exist.', 1;
		END;

		IF OBJECT_ID(@TableAfterFullName, 'U') IS NULL
		BEGIN
			;THROW 50000, 'Source table specified in @TableNameAfter does not exist.', 1;
		END;

		-- 2. Setup temporary tables for keys and exclusions
		CREATE TABLE #KeyColumnsTable (ColumnName sysname PRIMARY KEY);
		INSERT INTO #KeyColumnsTable (ColumnName)
		SELECT LTRIM(RTRIM(v.value)) FROM STRING_SPLIT(@KeyColumns, ',') v WHERE LTRIM(RTRIM(v.value)) <> '';

		IF NOT EXISTS (SELECT 1 FROM #KeyColumnsTable)
		BEGIN
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
			SELECT LTRIM(RTRIM(v.value)) FROM STRING_SPLIT(@ExcludeColumns, ',') v WHERE LTRIM(RTRIM(v.value)) <> '';
		END;

		-- 3. Identify common columns for comparison
		CREATE TABLE #ColumnNamesTable (ColumnName sysname PRIMARY KEY);
		INSERT INTO #ColumnNamesTable (ColumnName)
		SELECT c1.name
		FROM sys.columns c1
		INNER JOIN sys.tables t1 ON c1.object_id = t1.object_id
		INNER JOIN sys.schemas s1 ON t1.schema_id = s1.schema_id
		INNER JOIN sys.columns c2 ON c1.name = c2.name AND c1.system_type_id = c2.system_type_id AND c1.max_length = c2.max_length AND c1.precision = c2.precision AND c1.scale = c2.scale AND ISNULL(c1.collation_name, '') = ISNULL(c2.collation_name, '')
		INNER JOIN sys.tables t2 ON c2.object_id = t2.object_id
		INNER JOIN sys.schemas s2 ON t2.schema_id = s2.schema_id
		WHERE s1.name = @SchemaName AND t1.name = @TableNameBefore
		  AND s2.name = @SchemaName AND t2.name = @TableNameAfter
		  AND NOT EXISTS (SELECT 1 FROM #KeyColumnsTable k WHERE k.ColumnName = c1.name)
		  AND NOT EXISTS (SELECT 1 FROM #ExcludeColumnsTable e WHERE e.ColumnName = c1.name);

		-- 4. Get column count for strategy selection
		DECLARE @ComparableColumnCount INT = (SELECT COUNT(*) FROM #ColumnNamesTable);
		DECLARE @ComparisonThreshold INT = 50;

		-- 5. Build dynamic SQL components
		DECLARE @KeyColsForSelect_Quoted NVARCHAR(MAX);
		SELECT @KeyColsForSelect_Quoted = STRING_AGG(QUOTENAME(ColumnName), ', ') WITHIN GROUP (ORDER BY ColumnName) FROM #KeyColumnsTable;

		DECLARE @KeyJoinCondition NVARCHAR(MAX);
		SELECT @KeyJoinCondition = STRING_AGG(N'b.' + QUOTENAME(k.ColumnName) + N' = a.' + QUOTENAME(k.ColumnName), N' AND ') WITHIN GROUP (ORDER BY k.ColumnName) FROM #KeyColumnsTable k;

		DECLARE @KeyColsForCombinedString NVARCHAR(MAX);
		SELECT @KeyColsForCombinedString = STRING_AGG(N'CAST(' + QUOTENAME(ColumnName) + N' AS NVARCHAR(MAX))', ', ') WITHIN GROUP (ORDER BY ColumnName) FROM #KeyColumnsTable;

		DECLARE @KeyColsForOrderBy_Quoted NVARCHAR(MAX) = @KeyColsForSelect_Quoted;

		DECLARE @HashColumnList NVARCHAR(MAX);
		SELECT @HashColumnList = STRING_AGG(N'CAST(CAST(src.' + QUOTENAME(ColumnName) + N' AS sql_variant) AS VARBINARY(MAX))', N', ') WITHIN GROUP (ORDER BY ColumnName) FROM #ColumnNamesTable;

		DECLARE @RowHashExpression NVARCHAR(MAX);
		SET @RowHashExpression = N'HASHBYTES(''SHA2_256'', CONCAT_WS(0x1F, ' + ISNULL(@HashColumnList, N'CAST(N'''' AS VARBINARY(MAX))') + N'))';

		DECLARE @CombinedKeyExpression NVARCHAR(MAX);
		IF (SELECT COUNT(*) FROM #KeyColumnsTable) > 1
		BEGIN
			SET @CombinedKeyExpression = N'CONCAT_WS('','', ' + @KeyColsForCombinedString + N')';
		END
		ELSE
		BEGIN
			SET @CombinedKeyExpression = @KeyColsForCombinedString; -- If only one key, no concatenation needed
		END;

		-- 6. Strategy Selection and Dynamic SQL Construction
		DECLARE @ExecutionSQL NVARCHAR(MAX);

		IF @ComparableColumnCount < @ComparisonThreshold
		BEGIN
			-- PATH A: High-Speed Stream
			DECLARE @AllColsForSelect_Quoted NVARCHAR(MAX);
			SELECT @AllColsForSelect_Quoted = STRING_AGG(QUOTENAME(ColumnName), ', ') WITHIN GROUP (ORDER BY Ord, ColumnName) FROM ( SELECT ColumnName, 1 as Ord FROM #KeyColumnsTable UNION ALL SELECT ColumnName, 2 as Ord FROM #ColumnNamesTable ) AS cols;

			DECLARE @DeltaRowSelectCols_Aliased NVARCHAR(MAX);
			DECLARE @KeyPart NVARCHAR(MAX);
			DECLARE @ComparablePart NVARCHAR(MAX);
			
			SELECT @KeyPart = STRING_AGG(N'COALESCE(b.' + QUOTENAME(ColumnName) + N', a.' + QUOTENAME(ColumnName) + N') AS ' + QUOTENAME(ColumnName), ', ') WITHIN GROUP (ORDER BY ColumnName) FROM #KeyColumnsTable;
			SELECT @ComparablePart = STRING_AGG( N'b.' + QUOTENAME(ColumnName) + N' AS ' + QUOTENAME('Before_' + ColumnName) + N', a.' + QUOTENAME(ColumnName) + N' AS ' + QUOTENAME('After_' + ColumnName), ', ') WITHIN GROUP (ORDER BY ColumnName) FROM #ColumnNamesTable;
			SET @DeltaRowSelectCols_Aliased = @KeyPart + ISNULL(N', ' + @ComparablePart, N'');

			DECLARE @InFlightUnpivotValuesList NVARCHAR(MAX);
			SELECT @InFlightUnpivotValuesList = STRING_AGG( N'(N''' + REPLACE(ColumnName, '''', '''''') + N''', CAST(dr.' + QUOTENAME('Before_' + ColumnName) + N' AS sql_variant), CAST(dr.' + QUOTENAME('After_' + ColumnName) + N' AS sql_variant))', N', ') WITHIN GROUP (ORDER BY ColumnName) FROM #ColumnNamesTable;
			IF @InFlightUnpivotValuesList IS NULL SET @InFlightUnpivotValuesList = N'(NULL, CAST(NULL AS sql_variant), CAST(NULL AS sql_variant))';

			SET @ExecutionSQL = N'
WITH BeforeData AS (SELECT ' + @AllColsForSelect_Quoted + N', RowHash = ' + @RowHashExpression + N' FROM ' + @TableBeforeFullName + N' AS src),
AfterData AS (SELECT ' + @AllColsForSelect_Quoted + N', RowHash = ' + @RowHashExpression + N' FROM ' + @TableAfterFullName + N' AS src),
DeltaRows AS (SELECT ' + @DeltaRowSelectCols_Aliased + N' FROM BeforeData AS b FULL OUTER JOIN AfterData AS a ON ' + @KeyJoinCondition + N' WHERE b.RowHash <> a.RowHash OR b.RowHash IS NULL OR a.RowHash IS NULL),
UnpivotedDeltaDataStream AS (SELECT ' + @KeyColsForSelect_Quoted + N', u.ColumnName, u.BeforeValue_SV, u.AfterValue_SV FROM DeltaRows AS dr CROSS APPLY (VALUES ' + @InFlightUnpivotValuesList + N') AS u(ColumnName, BeforeValue_SV, AfterValue_SV) WHERE u.ColumnName IS NOT NULL),
AggregatedDifferences AS (SELECT ' + @KeyColsForSelect_Quoted + N', ColumnName, BeforeValue_SV, AfterValue_SV FROM UnpivotedDeltaDataStream WHERE CAST(BeforeValue_SV AS VARBINARY(MAX)) <> CAST(AfterValue_SV AS VARBINARY(MAX))),
FormattedDifferences AS (SELECT ' + @KeyColsForSelect_Quoted + N', ColumnName, CASE WHEN @ClearNulls = 1 THEN COALESCE(CAST(BeforeValue_SV AS NVARCHAR(MAX)), N'''') ELSE CAST(BeforeValue_SV AS NVARCHAR(MAX)) END AS BeforeValue, CASE WHEN @ClearNulls = 1 THEN COALESCE(CAST(AfterValue_SV AS NVARCHAR(MAX)), N'''') ELSE CAST(AfterValue_SV AS NVARCHAR(MAX)) END AS AfterValue, ' + @CombinedKeyExpression + N' AS KeyColumnsCombined_Raw FROM AggregatedDifferences),
PartitionedResults AS (SELECT KeyColumnsCombined_Raw AS KeyColumnsCombined, ColumnName, BeforeValue, AfterValue, rn = ROW_NUMBER() OVER (PARTITION BY ColumnName ORDER BY ' + @KeyColsForOrderBy_Quoted + N') FROM FormattedDifferences)
SELECT KeyColumnsCombined, ColumnName, BeforeValue, AfterValue FROM PartitionedResults WHERE rn <= @PartitionSize ORDER BY ColumnName, KeyColumnsCombined OPTION (RECOMPILE);';
		END
		ELSE
		BEGIN
			-- PATH B: Memory-Safe Staging
			DECLARE @DeltaKeySelectCols_AliasedB NVARCHAR(MAX);
			SELECT @DeltaKeySelectCols_AliasedB = STRING_AGG(N'COALESCE(b.' + QUOTENAME(ColumnName) + N', a.' + QUOTENAME(ColumnName) + N') AS ' + QUOTENAME(ColumnName), ', ') WITHIN GROUP (ORDER BY ColumnName) FROM #KeyColumnsTable;
			
			DECLARE @SelectiveUnpivotValuesList NVARCHAR(MAX);
			SELECT @SelectiveUnpivotValuesList = STRING_AGG(N'(N''' + REPLACE(ColumnName, '''', '''''') + N''', CAST(b.' + QUOTENAME(ColumnName) + N' AS sql_variant), CAST(a.' + QUOTENAME(ColumnName) + N' AS sql_variant))', N', ') WITHIN GROUP (ORDER BY ColumnName) FROM #ColumnNamesTable;
			IF @SelectiveUnpivotValuesList IS NULL SET @SelectiveUnpivotValuesList = N'(NULL, CAST(NULL AS sql_variant), CAST(NULL AS sql_variant))';
			
			SET @ExecutionSQL = N'
DROP TABLE IF EXISTS #DeltaKeys;
WITH BeforeHashes AS (SELECT ' + @KeyColsForSelect_Quoted + N', RowHash = ' + @RowHashExpression + N' FROM ' + @TableBeforeFullName + N' AS src),
AfterHashes AS (SELECT ' + @KeyColsForSelect_Quoted + N', RowHash = ' + @RowHashExpression + N' FROM ' + @TableAfterFullName + N' AS src),
DeltaKeyRows AS (SELECT ' + @DeltaKeySelectCols_AliasedB + N' FROM BeforeHashes AS b FULL OUTER JOIN AfterHashes AS a ON ' + @KeyJoinCondition + N' WHERE b.RowHash <> a.RowHash OR b.RowHash IS NULL OR a.RowHash IS NULL)
SELECT ' + @KeyColsForSelect_Quoted + N' INTO #DeltaKeys FROM DeltaKeyRows;
CREATE CLUSTERED INDEX IX_DeltaKeys_KeyCols ON #DeltaKeys (' + @KeyColsForOrderBy_Quoted + N');
WITH UnpivotedDeltaData AS (SELECT dk.' + REPLACE(@KeyColsForSelect_Quoted, ',', ', dk.') + N', u.ColumnName, u.BeforeValue_SV, u.AfterValue_SV FROM #DeltaKeys AS dk LEFT JOIN ' + @TableBeforeFullName + N' AS b ON ' + REPLACE(REPLACE(@KeyJoinCondition, 'b.', 'dk.'), 'a.', 'b.') + N' LEFT JOIN ' + @TableAfterFullName + N' AS a ON ' + REPLACE(REPLACE(@KeyJoinCondition, 'b.', 'dk.'), 'a.', 'a.') + N' CROSS APPLY (VALUES ' + @SelectiveUnpivotValuesList + N') AS u(ColumnName, BeforeValue_SV, AfterValue_SV) WHERE u.ColumnName IS NOT NULL),
AggregatedDifferences AS (SELECT ' + @KeyColsForSelect_Quoted + N', ColumnName, BeforeValue_SV, AfterValue_SV FROM UnpivotedDeltaData WHERE CAST(BeforeValue_SV AS VARBINARY(MAX)) <> CAST(AfterValue_SV AS VARBINARY(MAX))),
FormattedDifferences AS (SELECT ' + @KeyColsForSelect_Quoted + N', ColumnName, CASE WHEN @ClearNulls = 1 THEN COALESCE(CAST(BeforeValue_SV AS NVARCHAR(MAX)), N'''') ELSE CAST(BeforeValue_SV AS NVARCHAR(MAX)) END AS BeforeValue, CASE WHEN @ClearNulls = 1 THEN COALESCE(CAST(AfterValue_SV AS NVARCHAR(MAX)), N'''') ELSE CAST(AfterValue_SV AS NVARCHAR(MAX)) END AS AfterValue, ' + @CombinedKeyExpression + N' AS KeyColumnsCombined_Raw FROM AggregatedDifferences),
PartitionedResults AS (SELECT KeyColumnsCombined_Raw AS KeyColumnsCombined, ColumnName, BeforeValue, AfterValue, rn = ROW_NUMBER() OVER (PARTITION BY ColumnName ORDER BY ' + @KeyColsForOrderBy_Quoted + N') FROM FormattedDifferences)
SELECT KeyColumnsCombined, ColumnName, BeforeValue, AfterValue FROM PartitionedResults WHERE rn <= @PartitionSize ORDER BY ColumnName, KeyColumnsCombined OPTION (RECOMPILE);
DROP TABLE IF EXISTS #DeltaKeys;';
		END;

		EXEC sys.sp_executesql
			@ExecutionSQL,
			N'@ClearNulls BIT, @PartitionSize INT',
			@ClearNulls = @ClearNulls,
			@PartitionSize = @PartitionSize;

	END TRY
	BEGIN CATCH
		DROP TABLE IF EXISTS #KeyColumnsTable;
		DROP TABLE IF EXISTS #ExcludeColumnsTable;
		DROP TABLE IF EXISTS #ColumnNamesTable;
		DROP TABLE IF EXISTS #DeltaKeys;

		THROW;
	END CATCH

	DROP TABLE IF EXISTS #KeyColumnsTable;
	DROP TABLE IF EXISTS #ExcludeColumnsTable;
	DROP TABLE IF EXISTS #ColumnNamesTable;
	DROP TABLE IF EXISTS #DeltaKeys;

END;
GO

-- Step 3: Execute your test case against the newly created procedure.
DECLARE	@return_value int;

EXEC	@return_value = [dbo].[spn_nh_rpt_comparison_detailed_differences]
		@SchemaName = N'tmp',
		@TableNameBefore = N'WorkOrder_before',
		@TableNameAfter = N'WorkOrder_after',
		@KeyColumns = N'WorkOrderID',
		@ExcludeColumns = N'ProductID',
		@ClearNulls = 1,
		@PartitionSize = 10;

SELECT	'Return Value' = @return_value;
GO