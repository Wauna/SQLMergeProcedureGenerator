CREATE PROCEDURE [dbo].proc_GenerateMERGEProc(
@MergeProcSchema NVARCHAR(128),
@TableSchema NVARCHAR(128),
@TableName NVARCHAR(128)

)
AS 

SET NOCOUNT ON;

DECLARE @ErrorNumber INTEGER
DECLARE @ErrorMessage NVARCHAR(2500)
DECLARE @ProcName nvarchar(128)
DECLARE @ShortDate nvarchar(128)
DECLARE @PropertyList varchar(max)

DECLARE @RowsToProcess  int
DECLARE @CurrentRow     int

SET @ProcName = @MergeProcSchema + '.Merge' + @TableName
SET @ShortDate = CONVERT(Varchar(20), GETDATE(),101)


DECLARE @tableStructure 
TABLE(
  rowID int not null primary key identity(1,1)
  ,schemaName nvarchar(128)
  ,tableName nvarchar(128)
  ,columnName varchar(128) NOT NULL
  ,variableName varchar(128) NOT NULL
  ,dataType varchar(128) NOT NULL
  ,nullable varchar(128)
  ,isPK bit
	);

INSERT INTO @tableStructure (schemaName,tableName,columnName,variableName,dataType,nullable,isPK)

SELECT 
	TABLE_SCHEMA,
	TABLE_NAME,
	COLUMN_NAME,
	'@'+ COLUMN_NAME as ParamName,
	CASE
	WHEN CHARACTER_MAXIMUM_LENGTH IS NULL THEN
		 CAST(DATA_TYPE AS NVARCHAR(128))
	ELSE
		 DATA_TYPE + '(' + CAST(CHARACTER_MAXIMUM_LENGTH as nvarchar(128)) +')' 
	END as DATATYPE,
	IS_NULLABLE,
	0 as isPK
FROM [INFORMATION_SCHEMA].[COLUMNS]
WHERE TABLE_NAME= @TableName
AND TABLE_SCHEMA =@TableSchema

SET @RowsToProcess=@@ROWCOUNT


UPDATE @tableStructure SET isPK=1
WHERE columnName IN (
SELECT
	C.COLUMN_NAME

FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS TC
INNER JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE C ON C.CONSTRAINT_NAME= TC.CONSTRAINT_NAME
INNER JOIN [INFORMATION_SCHEMA].[COLUMNS] COL on COL.COLUMN_NAME=C.COLUMN_NAME AND COL.TABLE_NAME=TC.TABLE_NAME and COL.TABLE_SCHEMA = TC.TABLE_SCHEMA
WHERE TC.CONSTRAINT_TYPE='PRIMARY KEY'
AND C.TABLE_NAME=@TableName
)


--SELECT * from @tableStructure



PRINT '/*******************************************************************************'
PRINT '   PROCEDURE:    ' + @ProcName
PRINT '   PURPOSE :     Merges records in ' + @TableSchema + '.'+ @TableName + ' (UPSERT)'
PRINT '  '
PRINT '  '
PRINT '   REVISIONS:    '
PRINT '   DATE		    AUTHOR				DESCRIPTION'
PRINT '   ----------	-----------------	---------------------------------------'
PRINT '   ' + @ShortDate + '   ' + SUSER_NAME() +  '          INITIAL CREATION '
PRINT '********************************************************************************/'
PRINT ' ' 
PRINT ' ' 
PRINT ' ' 
PRINT 'CREATE PROCEDURE ' + @ProcName 
PRINT ' ('
DECLARE @SelectCol1     varchar(128)
DECLARE @SelectCol2     varchar(128)
DECLARE @SelectCol3		varchar(128)

SET @CurrentRow=0
WHILE @CurrentRow < @RowsToProcess
BEGIN
    SET @CurrentRow=@CurrentRow+1
    SELECT 
        @SelectCol1 = variableName,
		@SelectCol2 = dataType,
		@SelectCol3=nullable
        FROM @tableStructure
        WHERE RowID=@CurrentRow

    --POOP OUT PROPERTY NAMES--
    IF @CurrentRow <> @RowsToProcess 	 	
		IF @SelectCol3='YES' 
			print '        ' + @SelectCol1 + ' ' + @SelectCol2  + ' NULL,'
		ELSE
		    print '        ' + @SelectCol1 + ' ' + @SelectCol2  + ','
	ELSE
		IF @SelectCol3='YES' 
			print '        ' + @SelectCol1 + ' ' + @SelectCol2 + ' NULL' 
		ELSE
			print '        ' + @SelectCol1 + ' ' + @SelectCol2 
		
END


PRINT ' )'
PRINT ' AS'
PRINT ' DECLARE @ErrorNumber INTEGER'
PRINT ' DECLARE @ErrorMessage NVARCHAR(2500)' 
--PRINT ' SET @synchronized = 0 ' 
--PRINT ' SET @lastUpdate = GETDATE()' 
--PRINT ' SET @lastUpdateBy = SUSER_NAME()' 
PRINT ' BEGIN TRY ' 
-- ************** BUILDING THE MERGE ******************

PRINT ' ' 
PRINT ' MERGE ' + @TableSchema + '.'+ @TableName + ' AS TARGET'
PRINT  ' '
PRINT  '     USING ( ' 
PRINT  '             SELECT ' 


--  '************ CREATE THE SELECT PROPERTY LIST **************'
		SET @CurrentRow=0
		WHILE @CurrentRow < @RowsToProcess
		BEGIN
			SET @CurrentRow=@CurrentRow+1
			SELECT 
				@SelectCol1 = variableName
				FROM @tableStructure
				WHERE RowID=@CurrentRow
		 IF @CurrentRow <> @RowsToProcess 	 	
			PRINT '                   ' + @SelectCol1 + ','
ELSE
			PRINT '                   ' + @SelectCol1 
END
PRINT '            ) AS SOURCE '
PRINT '            ('



--  '************ CREATE THE SELECT PROPERTY LIST **************'
SET @CurrentRow=0
WHILE @CurrentRow < @RowsToProcess
BEGIN
    SET @CurrentRow=@CurrentRow+1
    SELECT 
        @SelectCol1 = columnName
        FROM @tableStructure
        WHERE RowID=@CurrentRow
		 IF @CurrentRow <> @RowsToProcess 	 	
			PRINT '                   [' + @SelectCol1 + '],'
ELSE
			PRINT '                   [' + @SelectCol1 =']'
END

PRINT '             )'


PRINT ' ON (' 
				DECLARE @pkCursor AS CURSOR
				DECLARE @delim as varchar(20)
				SET @delim=''

				SET @pkCursor = CURSOR FOR SELECT columnName FROM @tableStructure WHERE isPK=1
				OPEN @pkCursor

				FETCH NEXT FROM @pkCursor INTO @SelectCol1

				WHILE @@FETCH_STATUS = 0
				BEGIN
				 PRINT '               '+ @delim +'target.' + @SelectCol1  + '=source.' + @SelectCol1
				 SET @delim='AND '
				 FETCH NEXT FROM @pkCursor INTO @SelectCol1
				END

				CLOSE @pkCursor;
				DEALLOCATE @pkCursor;

PRINT '            )'

PRINT ' WHEN MATCHED THEN ' 
PRINT '     UPDATE SET ' 

			SET @CurrentRow=0
			SET @delim=''
			WHILE @CurrentRow < @RowsToProcess
			BEGIN
				SET @CurrentRow=@CurrentRow+1
				SELECT 
					@SelectCol1 = columnName
					FROM @tableStructure
					WHERE RowID=@CurrentRow
						PRINT '                   ' + @delim +'[' + @SelectCol1 + ']=[source].['+ @SelectCol1+']'
					SET @delim=','
			END

PRINT ' ' 
PRINT ' WHEN NOT MATCHED THEN' 
PRINT '      INSERT('
			SET @CurrentRow=0
			SET @delim=''
			WHILE @CurrentRow < @RowsToProcess
			BEGIN
				SET @CurrentRow=@CurrentRow+1
				SELECT 
					@SelectCol1 = columnName
					FROM @tableStructure
					WHERE RowID=@CurrentRow
						PRINT '                   ' + @delim +'[' + @SelectCol1 + ']'
					SET @delim=','
			END

PRINT '            )'
PRINT '       VALUES ( '
			SET @CurrentRow=0
			SET @delim=''
			WHILE @CurrentRow < @RowsToProcess
			BEGIN
				SET @CurrentRow=@CurrentRow+1
				SELECT 
					@SelectCol1 = columnName
					FROM @tableStructure
					WHERE RowID=@CurrentRow
						PRINT '                   ' + @delim +'[source].['+ @SelectCol1+']'
					SET @delim=','
			END

PRINT '                 );'
PRINT 'END TRY'
PRINT ' BEGIN CATCH'
PRINT ' ' 
PRINT ' 	SELECT @ErrorNumber =  ERROR_NUMBER(),'
PRINT ' 		   @ErrorMessage = ERROR_MESSAGE()'
PRINT ' ' 		   
PRINT ' 	RAISERROR(''ERROR: FAILED TO EXECUTE ' + @ProcName + ' :%d:%s'',16,1,@ErrorNumber, @ErrorMessage)'
PRINT ' ' 
PRINT ' END CATCH'



