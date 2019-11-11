{-# LANGUAGE TemplateHaskell, FlexibleContexts, DuplicateRecordFields, NamedFieldPuns #-}
module Kyuu.Parse.Analyzer
        ( Query(..)
        , ParserNode(..)
        , RangeTableEntry(..)
        , RangeTable
        , RangeTableRef(..)
        , parseSQLStatement
        , analyzeParseTree
        )
where

import           Kyuu.Prelude
import           Kyuu.Core
import           Kyuu.Expression
import           Kyuu.Value
import           Kyuu.Catalog.Schema
import           Kyuu.Catalog.Catalog
import           Kyuu.Error

import           Control.Lens
import           Control.Monad.State.Lazy

import           Data.List                      ( find )

import qualified Language.SQL.SimpleSQL.Syntax as S
import           Language.SQL.SimpleSQL.Dialect
import           Language.SQL.SimpleSQL.Parse

data RangeTableEntry = RteTable { tableId :: OID
                                , tableName :: String }
                     | RteJoin { left :: RangeTableRef
                               , right :: RangeTableRef }
                     deriving (Show)

type RangeTable = [RangeTableEntry]

-- |Reference to range table entry in the analyzer state
newtype RangeTableRef = RangeTableRef Int
                      deriving (Show)

data ColumnTableEntry = ColumnTableEntry OID OID String
                      deriving (Show)

data ColumnConstraint = ColumnConstraint
                      deriving (Eq, Show)

data ParserNode = SelectStmt { isDistinct :: Bool
                             , selectItems :: [SqlExpr Value]
                             , fromItem :: RangeTableRef
                             , whereExpr :: Maybe (SqlExpr Value) }
                | CreateTableStmt { tableName :: String
                                  , columns :: [ColumnSchema]
                                  , constraints :: [ColumnConstraint] }
                deriving (Show)

data AnalyzerState = AnalyzerState { _namePool :: [String]
                                   , _rangeTable :: RangeTable
                                   , _columnTable :: [ColumnTableEntry] }
                     deriving (Show)

type Analyzer m a = StateT AnalyzerState (Kyuu m) a

data Query = Query { _parseTree :: ParserNode
                   , _rangeTable :: RangeTable }
           deriving (Show)

makeLensesWith (lensRules & lensField .~ lensGen) ''AnalyzerState

parseSQLStatement :: String -> Either ParseError S.Statement
parseSQLStatement = parseStatement postgres "<repl>" Nothing

isDistinctSelect :: S.SetQuantifier -> Bool
isDistinctSelect S.Distinct = True
isDistinctSelect _          = False

initAnalyzerState = AnalyzerState [] [] []

appendRangeTable
        :: (MonadState AnalyzerState m) => RangeTableEntry -> m RangeTableRef
appendRangeTable entry = do
        modify $ over rangeTable_ $ \tbl -> tbl ++ [entry]
        state <- get
        return $ RangeTableRef (length (state ^. rangeTable_) - 1)

getRangeTable :: (MonadState AnalyzerState m) => m RangeTable
getRangeTable = get >>= \state -> return $ state ^. rangeTable_

appendColumnTable
        :: (MonadState AnalyzerState m)
        => OID
        -> OID
        -> String
        -> m ColumnTableEntry
appendColumnTable tableId colId colName = do
        let entry = ColumnTableEntry tableId colId colName
        appendColumnTableEntry entry
        return entry

appendColumnTableEntry
        :: (MonadState AnalyzerState m) => ColumnTableEntry -> m ()
appendColumnTableEntry entry =
        modify . over columnTable_ $ \tbl -> tbl ++ [entry]

getTableColumn
        :: (StorageBackend m)
        => RangeTableEntry
        -> String
        -> Analyzer m (Maybe ColumnSchema)
getTableColumn (RteTable tId _) name = lift $ lookupTableColumnByName tId name

addColumnByName
        :: (StorageBackend m)
        => Maybe String
        -> String
        -> Maybe String
        -> Analyzer m ColumnTableEntry
addColumnByName Nothing colName Nothing = do
        state   <- get
        schemas <- sequence
                <$> forM (state ^. rangeTable_) (`getTableColumn` colName)

        case schemas of
                Nothing           -> lift $ lerror (ColumnNotFound colName)
                Just (x : y : xs) -> lift $ lerror (DuplicateColumn colName)
                Just [ColumnSchema tId cId cName _] ->
                        appendColumnTable tId cId cName

addColumnByName (Just tableName) colName Nothing = do
        tId <- lift $ lookupTableIdByName tableName
        case tId of
                Nothing  -> lift $ lerror (TableWithNameNotFound tableName)
                Just tId -> do
                        cId <- lift $ lookupTableColumnByName tId colName
                        case cId of
                                Nothing ->
                                        lift $ lerror (ColumnNotFound colName)
                                Just (ColumnSchema tId cId cName _) ->
                                        appendColumnTable tId cId cName

lookupColumnById
        :: (MonadState AnalyzerState m)
        => OID
        -> OID
        -> m (Maybe ColumnTableEntry)
lookupColumnById tId cId = get >>= \state -> return $ find
        (\(ColumnTableEntry colTable colId _) -> colTable == tId && colId == cId
        )
        (state ^. columnTable_)

lookupColumnByName
        :: (MonadState AnalyzerState m) => String -> m (Maybe ColumnTableEntry)
lookupColumnByName name = get >>= \state -> return $ find
        (\(ColumnTableEntry _ _ colName) -> colName == name)
        (state ^. columnTable_)

runAnalyzer :: Analyzer m a -> AnalyzerState -> Kyuu m (a, AnalyzerState)
runAnalyzer = runStateT

analyzeParseTree :: (StorageBackend m) => S.Statement -> Kyuu m Query
analyzeParseTree stmt = do
        (n, s) <- runAnalyzer (transformTopLevelStmt stmt) initAnalyzerState
        return $ Query n (s ^. rangeTable_)

transformTopLevelStmt
        :: (StorageBackend m) => S.Statement -> Analyzer m ParserNode
transformTopLevelStmt = transformStmt

transformStmt :: (StorageBackend m) => S.Statement -> Analyzer m ParserNode
transformStmt stmt@S.SelectStatement{} = transformSelectStmt stmt
transformStmt stmt@S.CreateTable{}     = transformDDL stmt

transformScalarExpr
        :: (StorageBackend m) => S.ScalarExpr -> Analyzer m (SqlExpr Value)
transformScalarExpr (S.Iden [S.Name _ name]) = do
        lookupResult <- lookupColumnByName name
        case lookupResult of
                Just (ColumnTableEntry colTable colId _) ->
                        return $ ColumnRefExpr colTable colId
                _ -> do
                        (ColumnTableEntry tableId colId colName) <-
                                addColumnByName Nothing name Nothing

                        return $ ColumnRefExpr tableId colId

transformScalarExpr (S.Iden [S.Name _ tableName, S.Name _ colName]) = do
        (ColumnTableEntry tableId colId colName) <- addColumnByName
                (Just tableName)
                colName
                Nothing
        return $ ColumnRefExpr tableId colId

transformScalarExpr (S.BinOp lhs [S.Name _ opName] rhs) = do
        left  <- transformScalarExpr lhs
        right <- transformScalarExpr rhs
        let op = getBinOp opName
        return $ BinOpExpr op left right

transformScalarExpr (S.NumLit lit) = return $ ValueExpr (readNumLit lit)
    where
        readNumLit :: String -> Value
        readNumLit lit =
                if '.' `elem` lit then VDouble (read lit) else VInt (read lit)

transformSelectStmt
        :: (StorageBackend m) => S.Statement -> Analyzer m ParserNode
transformSelectStmt (S.SelectStatement S.Select { S.qeSetQuantifier = setQuantifier, S.qeSelectList = selectClause, S.qeFrom = fromClause, S.qeWhere = whereClause })
        = do
                let distinct = isDistinctSelect setQuantifier
                fromItem    <- transformFromClause fromClause
                selectItems <- transformSelectClause selectClause
                whereExpr   <- forM whereClause
                        $ \cond -> transformScalarExpr cond
                return $ SelectStmt distinct selectItems fromItem whereExpr

transformSelectClause
        :: (StorageBackend m)
        => [(S.ScalarExpr, Maybe S.Name)]
        -> Analyzer m [SqlExpr Value]
transformSelectClause items = concat <$> mapM transformSelectItem items

transformSelectItem
        :: (StorageBackend m)
        => (S.ScalarExpr, Maybe S.Name)
        -> Analyzer m [SqlExpr Value]
transformSelectItem (S.Star, Nothing) = do
        rangeTable <- getRangeTable
        columns    <- mapM (`transformTableColumns` True) rangeTable

        return $ concat columns

transformSelectItem (expr, Nothing) = (: []) <$> transformScalarExpr expr

-- |Expand STAR in select clauses to columns
transformTableColumns
        :: (StorageBackend m)
        => RangeTableEntry
        -> Bool
        -> Analyzer m [SqlExpr Value]
transformTableColumns (RteTable tableId _) createExpr = do
        columns <- lift $ getTableColumns tableId

        exprs   <- forM columns $ \ColumnSchema { colId, colName } -> do
                colEntry <- lookupColumnById tableId colId

                case colEntry of
                        Just column -> return ()
                        Nothing ->
                                void $ appendColumnTable tableId colId colName

                if createExpr
                        then do
                                let expr = ColumnRefExpr tableId colId
                                return $ Just expr
                        else return Nothing

        case sequence exprs of
                Just l  -> return l
                Nothing -> return []

transformTableColumns _ _ = return []

transformFromClause
        :: (StorageBackend m) => [S.TableRef] -> Analyzer m RangeTableRef
transformFromClause [ref       ] = transformFromItem ref
transformFromClause (ref : refs) = do
        left  <- transformFromClause [ref]
        right <- transformFromClause refs
        appendRangeTable (RteJoin left right)

transformFromItem
        :: (StorageBackend m) => S.TableRef -> Analyzer m RangeTableRef
transformFromItem (S.TRAlias table alias) =
        transformFromItemAlias table (Just alias)
transformFromItem ref = transformFromItemAlias ref Nothing

transformFromItemAlias
        :: (StorageBackend m)
        => S.TableRef
        -> Maybe S.Alias
        -> Analyzer m RangeTableRef
transformFromItemAlias (S.TRSimple [S.Name _ tableName]) alias = do
        tableId <- lift $ lookupTableIdByName tableName
        case tableId of
                (Just id) -> appendRangeTable (RteTable id tableName)
                Nothing   -> lift $ lerror $ TableWithNameNotFound tableName

transformDDL :: (StorageBackend m) => S.Statement -> Analyzer m ParserNode
transformDDL (S.CreateTable [S.Name _ tableName] elts) = do
        (cols, constraints) <- transformCreateTableElements elts 1
        return $ CreateTableStmt tableName cols constraints

transformCreateTableElements
        :: (StorageBackend m)
        => [S.TableElement]
        -> OID
        -> Analyzer m ([ColumnSchema], [ColumnConstraint])
transformCreateTableElements [] _     = return ([], [])
transformCreateTableElements (S.TableColumnDef colDesc : elts) colId = do
        (cols, constraints) <- transformCreateTableElements elts (colId + 1)
        col                 <- transformColumnSchema colDesc colId
        return (col : cols, constraints)

transformColumnSchema
        :: (StorageBackend m) => S.ColumnDef -> OID -> Analyzer m ColumnSchema
transformColumnSchema (S.ColumnDef (S.Name _ colName) typeName _ _) colId = do
        colType <- transformColumnType typeName
        return $ ColumnSchema 0 colId colName colType

transformColumnType :: (StorageBackend m) => S.TypeName -> Analyzer m SchemaType
transformColumnType (S.TypeName [S.Name _ "int"    ]) = return SInt
transformColumnType (S.TypeName [S.Name _ "varchar"]) = return SString
transformColumnType (S.TypeName [S.Name _ "double" ]) = return SDouble
transformColumnType (S.TypeName [S.Name _ typeName]) =
        lift $ lerror (UnknownDataType typeName)