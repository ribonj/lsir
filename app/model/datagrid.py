
from app import config
from app.controllers.spark import sqlContext

from pyspark.sql.functions import coalesce, monotonically_increasing_id, lit # literal
from pyspark.sql.types import NullType


class DataGrid():
    """
    DataGrid is based on the notion of DataFrame.
    
    A DataFrame is a distributed collection of data grouped into named columns.
    A :class:`DataFrame` is equivalent to a relational table in Spark SQL,
    and can be created using various functions in :class:`SQLContext`.
    Once created, a DataFrame can be manipulated using the various
    domain-specific-language (DSL) functions 
    defined in: :class:`DataFrame`, and :class:`Column`.
    
    A :class:`DataGrid` is an extension of this concept in the sense that
    a DataGrid represents a set of potentially related DataFrames.
    In short, it can represent a whole relational database, i.e. a group of
    tables connected by foreign key relationships or any data files sharing 
    a group of columns.
    """
    
    dataframe = None # current DataFrame
    history = [] # history of user operations on DataGrid
    histroyIndex = 0 # position of current DataFrame in history list
    
    def __init__(self, df):
        """
        Creates the Data Grid based on a given DataFrame.
        """
        self.dataframe = df
        self.dataframe.persist()
        
        self.historyIndex = 0
        self.history = [df]
        
    
    def reset(self, df=None):
        """
        Reset the DataGrid to its original state (i.e. re-initialization).
        If a DataFrame is given, it is used to reset the DataGrid.
        """
        if df is None:
            # Use original DataFrame to reset the DataGrid
            df = self.history[0]
        
        self.dataframe.unpersist()
        self.dataframe = df
        self.dataframe.persist()
        
        self.historyIndex = 0
        self.history = [df]
        
    
    def setDataFrame(self, df):
        """
        Set the given DataFrame as DataGrid.
        """
        self.dataframe.unpersist()
        self.dataframe = df
        self.dataframe.persist()
        
        # remove tail of history and append new DataFrame
        self.historyIndex += 1
        self.history = self.history[:self.historyIndex]
        self.history.append(df)
    
    
    def addColumn(self, name, value):
        """
        Add a new column.
        """
        df = self.dataframe.withColumn(name, lit(value))
        self.setDataFrame(df)
    
    
    def undo(self):
        """
        Cancels previous transformation.
        This does not modify the history.
        """
        if self.historyIndex > 0:
            self.historyIndex -= 1
            #self.setDataFrame(self.history[self.historyIndex])
            self.dataframe.unpersist()
            self.dataframe = self.history[self.historyIndex]
            self.dataframe.persist()
    
    
    def redo(self):
        """
        Re-applies cancelled transformation.
        This does not modify the history.
        """
        if self.historyIndex < len(self.history) - 1:
            self.historyIndex += 1
            self.dataframe.unpersist()
            self.dataframe = self.history[self.historyIndex]
            self.dataframe.persist()
    
    
    def sort(self, column, ascending):
        """
        Sort the datagrid according to a given column and order.
        """
        df = self.dataframe.sort(column, ascending=ascending)
        sp = self.sample.sort(column, ascending=ascending)
        self.setDataFrame(df)
        self.setDataFrame(sp)
    
    
    def index(self):
        """
        Add a unique (monotonically increasing) row ID to the DataGrid
        """
        self.dataframe = self.dataframe.withColumn(
            'GridID', 
            monotonically_increasing_id())
    
    def merge(self, df):
        """
        Combines the Data Grid with a given DataFrame.
        The result is similar to a full outer join, 
        except that when there is no match for a given row, 
        the row is created anyway with all other columns set to NULL.
        
        Assuming these schemas:
        
            DataGrid:  dg[a,b,c]
            DataFrame: df[b,c,d,e]
            
        This is equivalent of executing the following query after adding 
        new columns to the original DataGrid:
        
            SELECT a, b, e, COALESCE(dg.b,df.b) as b, COALESCE(dg.c,df.c) as c
            FROM df
            FULL OUTER JOIN df ON 1=1
                AND dg.b = df.b
                AND dg.c = df.c
        """
        dg = self.dataframe
        
        # Get information about columns and computes common 
        # and different column sets between the DataFrame and DataGrid.
        dg_columns = dg.columns
        df_columns = df.columns
        common_columns = list(set(dg_columns) & set(df_columns)) # intersect
        
        # Merge the new DataFrame with the current DataGrid
        if not dg_columns:
            # Use the given DataFrame as default DataGrid
            #self.dataframe = df
            #self.setDataFrame(df)
            dg = df
            #self.index()
        else:
            all_columns = list(set(dg_columns + df_columns)) # union
            new_columns = list(set(df_columns) - set(dg_columns)) # diff
            diff_columns = list(set(all_columns) - set(common_columns)) # diff
            
            # Merge Columns
            if not common_columns:
                # Add all columns from the given DataFrame that do not exist yet 
                # in the DataGrid and initialize them with NULL values.
                common_columns = df_columns
                diff_columns = dg_columns
                for c in new_columns:
                    dg = dg.withColumn(c, lit(None).cast(NullType()))
            
            # Rename DataFrame's columns that are shared with the DataGrid
            condition = []
            for c in common_columns:
                df = df.withColumnRenamed(c, 'df_'+c)
                condition.append(dg[c] == df['df_'+c])
            
            # Join DataFrames
            dg = dg.join(df, condition, 'outer')
            
            for c in common_columns:
                dg = dg.withColumn(c, coalesce(c, "df_"+c))
                dg = dg.drop("df_"+c)
                
            #self.dataframe = dg
            #self.setDataFrame(dg)
            #self.index()
            
            if config.DEBUG:
                # Debugging: Print out the equivalent SQL Query
                select_stmt = ' SELECT '
                from_stmt = ' FROM dg'
                join_stmt = ' FULL OUTER JOIN df ON 1=1'
                
                # Append non-common columns to select statement
                for i in range(0,len(diff_columns)):
                    if i!=0: select_stmt += ', '
                    select_stmt += diff_columns[i]
                
                # Append common columns with COALESCE function to select statement
                # and add conditions to the join predicate 
                for i in range(0,len(common_columns)):
                    if i!=0 or len(diff_columns)>0: select_stmt += ', '
                    c = common_columns[i]
                    select_stmt += 'COALESCE(dg.'+c+', df.'+c+') AS '+c
                    join_stmt += ' AND dg.'+c+'=df.'+c
                 
                query = select_stmt + from_stmt + join_stmt
                print query
                
                # Column information
                print 'dg_columns: ' + dg_columns.__repr__()
                print 'df_columns: ' + df_columns.__repr__()
                print 'all_columns: ' + all_columns.__repr__()
                print 'new_columns: ' + new_columns.__repr__()
                print 'common_columns: ' + common_columns.__repr__()
                print 'diff_columns: ' + diff_columns.__repr__()
        
        #self.dataframe.cache()
        return dg
    
    
    def sample(self, dataframe):
        """
        Obtain a data sample representing the whole data grid.  
        Choose fraction depending on size of dataset:
            sample_size = dataset_size * fraction
            fraction = sample_size / data_size
        Example: dataset_size = 4e10, fraction = 1e-7, sample_size = 4e3
        """
        fraction = 1000.0/dataframe.count()
        if fraction > 1.0:
            fraction = 1.0
        
        sample = dataframe.sample(withReplacement=False, 
                                  fraction=fraction, 
                                  seed=42)
        
        sample = sqlContext.createDataFrame(sample.collect())
        return sample
    
    def resample(self):
        """
        Obtain a data sample representing the whole data grid.  
        """
        df = self.sample(self.dataframe)
        self.setDataFrame(df)
        
    
    def apply(self, column, function, type):
        """
        Apply a given function to one specific column.
        Duality between standard and large transformations (for 1 column).
        The idea is to apply the same function to standard data structure 
        (e.g. Python dictionary) and large collection.
        """
        df = self.dataframe
        schema = dict(df.dtypes)
        
        if (column in schema): # and (schema[column] == 'vector'):
            # Lazily applies function to the data grid
            f = udf(function, type)
            self.dataframe = df.columnWith(column, f(df[column]))
            
            # Eagerly applies function to the data sample
            self.sample = self.sample.columnWith(column, f(df[column]))
            #for s in self.sample:
            #    s[column] = function(s[column])
            
            # Re-samples the data sample if count is smaller than a given threshold
            if len(self.sample) < 100:
                self.resample()
        
        return self.sample
    
    
