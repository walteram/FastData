using System;
using System.CodeDom.Compiler;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using FastData.Configuration;
using FastData.Utils;
using Microsoft.CSharp;

namespace FastData
{
    public abstract class TableMap<T> : IDisposable where T : class, new()
    {
        private Dictionary<string, CachedCustomQuery> _cachedCustomQueries;

        public delegate void ObjectInsertEventHandler(T obj, bool successfull);

        public event ObjectInsertEventHandler OnInsert;

        public delegate void ObjectBeforeInsertEventHandler(T obj);

        public event ObjectBeforeInsertEventHandler OnBeforeInsert;

        public bool OnInsertIsNull { get { return OnInsert == null; } }

        public bool OnBeforeInsertIsNull { get { return OnBeforeInsert == null; } }

        private readonly List<AppDomain> _currentAppDomains = new List<AppDomain>();

        /// <summary>
        /// Informa um caminho para buscar as referências
        /// </summary>
        public void AppendPrivatePath(string path)
        {
            var appDomainSetup = new AppDomainSetup
            {
                ApplicationBase = AppDomain.CurrentDomain.BaseDirectory,
                PrivateBinPath = path
            };
            _currentAppDomains.Add(AppDomain.CreateDomain("BaseDomain", null, appDomainSetup));
        }

        public void UnloadDomains()
        {
            foreach (var appDomain in _currentAppDomains)
            {
                AppDomain.Unload(appDomain);
            }
            _currentAppDomains.Clear();
        }

        public void Dispose()
        {
            UnloadDomains();
        }

        public static TableMap<T> Instance;

        public abstract T CreateObject(DbDataReader dataReader);

        protected abstract void CreateUpdateCommand(T obj, ref DbCommand command);

        protected abstract void CreateUpdateCommand(T obj, ref DbCommand command, string[] changedProperties, string[] incrementProperties);

        protected abstract void CreateInsertCommand(T obj, ref DbCommand command);

        public abstract void CreateTryInsertCommand(T obj, ref DbCommand command);

        public abstract void UpdateTryInsertCommand(T obj, ref DbCommand command);

        public abstract object[] GetUniqueKeyValues(T obj);

        protected abstract string SelectCommandText { get; }

        protected abstract string SelectTopNCommandText { get; }

        protected abstract string SelectFieldCommandText { get; }

        protected abstract string SelectCreationDateCommandText { get; }

        protected abstract string SelectPrimaryKeyCommandText { get; }

        protected abstract string UpdateCommandText { get; }

        protected abstract string InsertCommandText { get; }        
        
        protected abstract string TryInsertCommandText { get; }

        protected abstract string GetLastIdCommandText { get; }        

        protected abstract string PrimaryKeyName { get; }
        
        protected abstract string DetectChangesCommandText { get; }

        protected abstract string CountCommandText { get; }

        protected abstract string DeleteCommandText { get; }

        protected abstract string TruncateCommandText { get; }

        protected abstract string TableName { get; }

        protected abstract string[] UniqueKeyNames { get; }

        public int Count()
        {
            return Convert.ToInt32(GetCount());
        }

        public long LongCount()
        {
            return Convert.ToInt64(GetCount());
        }

        public int Count(DbConnection connection)
        {
            return Convert.ToInt32(GetCount(connection));
        }

        public long LongCount(DbConnection connection)
        {
            return Convert.ToInt64(GetCount(connection));
        }

        private object GetCount()
        {
            using (var connection = CreateConnection())
            {
                return GetCount(connection);
            }
        }

        private object GetCount(DbConnection connection)
        {
            var command = connection.CreateCommand();
            try { command.CommandTimeout = FastDataOptions.Instance.CommandTimeout; }
            catch (Exception) { Debug.WriteLine("Timeout nao aceito."); }
            command.CommandText = CountCommandText;
            var result = command.ExecuteScalar();
            return result;
        }

        public T GetByUniqueKeys(T obj)
        {
            return GetByUniqueKeys(GetUniqueKeyValues(obj));
        }

        public T GetByUniqueKeysFromEntity(DbConnection connection, T obj)
        {
            return GetByUniqueKeys(connection, GetUniqueKeyValues(obj));
        }
        
        public T GetByUniqueKeys(params object[] uniqueKeyValues)
        {
            using (var connection = CreateConnection())
            {
                return RunCachedQuerySingle(UniqueKeysQuery, uniqueKeyValues, connection);                
            }
        }

        public T GetByUniqueKeys(DbConnection connection, object[] uniqueKeyValues)
        {
            return RunCachedQuerySingle(UniqueKeysQuery, uniqueKeyValues, connection);            
        }

        internal CachedCustomQuery UniqueKeysQuery;

        protected TableMap()
        {
            if(UniqueKeyNames != null)
            {
                UniqueKeysQuery = CreateCachedCustomQuery("UniqueKeysQuery", UniqueKeyNames);
            }
        } 

        public object GetFieldByUniqueKeys(string fieldName, T obj)
        {
            using (var connection = CreateConnection())
            {
                return GetFieldByUniqueKeys(fieldName, connection, GetUniqueKeyValues(obj));                
            }
        }

        public object GetFieldByUniqueKeys(string fieldName, T obj, DbConnection connection)
        {
            return GetFieldByUniqueKeys(fieldName, connection, GetUniqueKeyValues(obj));            
        }

        public long GetIdByUniqueKeys(T obj)
        {
            using (var connection = CreateConnection())
            {
                return GetPrimaryKey(connection, GetUniqueKeyValues(obj));
            }
        }

        public long GetIdByUniqueKeys(T obj, DbConnection connection)
        {
            return GetPrimaryKey(connection, GetUniqueKeyValues(obj));
        }

        public object GetFieldByUniqueKeys(string fieldName, params object[] uniqueKeyValues)
        {
            using (var connection = CreateConnection())
            {
                return GetFieldByUniqueKeys(fieldName, connection, uniqueKeyValues);
            }
        }

        public object GetFieldByUniqueKeys(string fieldName, DbConnection connection, object[] uniqueKeyValues)
        {
            if (fieldName == null)
            {
                fieldName = PrimaryKeyName;
            }
            else
            {
                fieldName = FastDataOptions.Instance.TypeInfoLoader.GetColumnName(FastDataOptions.Instance.TypeInfoLoader.GetProperty(typeof(T), fieldName));
            }
            return GetColumnByUniqueKeys(fieldName, connection, uniqueKeyValues);
        }

        public object GetColumnByUniqueKeys(string fieldName, DbConnection connection, object[] uniqueKeyValues)
        {
            var command = connection.CreateCommand();
            try{command.CommandTimeout = FastDataOptions.Instance.CommandTimeout;}catch (Exception) { Debug.WriteLine("Timeout nao aceito."); }
            command.CommandText = FastDataOptions.Instance.SelectQueryPreamble + string.Format(SelectFieldCommandText, fieldName, FastDataOptions.Instance.GetTop(1), FastDataOptions.Instance.GetLimit(1), FastDataOptions.Instance.IsNullExpression);            
            command.Parameters.AddRange(CreateParameter(UniqueKeyNames, uniqueKeyValues, command).ToArray());
            var result = command.ExecuteScalar();
            if (result == DBNull.Value)
            {
                result = null;
            }
            return result;
        }

        public long GetLastId(DbConnection connection)
        {
            var command = connection.CreateCommand();
            command.CommandText = string.Format(GetLastIdCommandText, PrimaryKeyName, FastDataOptions.Instance.GetTop(1), FastDataOptions.Instance.GetLimit(1), FastDataOptions.Instance.IsNullExpression);            
            var result = command.ExecuteScalar();
            if (result == DBNull.Value || result == null)
            {
                result = 0;
            }
            if (FastDataOptions.Instance.UseSystemConvert)
            {
                return Convert.ToInt64(result);
            }
            return (long)result;
        }

        public object GetCreationDate(object[] uniqueKeyValues)
        {
            using (var connection = CreateConnection())
            {
                return GetCreationDate(connection, uniqueKeyValues);
            }
        }

        public object GetCreationDate(DbConnection connection, object[] uniqueKeyValues)
        {
            var command = connection.CreateCommand();
            try{command.CommandTimeout = FastDataOptions.Instance.CommandTimeout;}catch (Exception) { Debug.WriteLine("Timeout nao aceito."); }
            command.CommandText = SelectCreationDateCommandText;
            command.Parameters.AddRange(CreateParameter(UniqueKeyNames, uniqueKeyValues, command).ToArray());
            var result = command.ExecuteScalar();
            if (result == DBNull.Value)
            {
                result = null;
            }
            return result;
        }

        public long GetPrimaryKey(DbConnection connection, object[] uniqueKeyValues)
        {
            var command = connection.CreateCommand();
            try{command.CommandTimeout = FastDataOptions.Instance.CommandTimeout;}catch (Exception) { Debug.WriteLine("Timeout nao aceito."); }
            command.CommandText = SelectPrimaryKeyCommandText;
            command.Parameters.AddRange(CreateParameter(UniqueKeyNames, uniqueKeyValues, command).ToArray());
            var result = command.ExecuteScalar();            
            if (result == null)
            {
                return 0;
            }
            if (FastDataOptions.Instance.UseSystemConvert)
            {
                return Convert.ToInt64(result);
            }
            return (long)result;
        }
        
        public T GetByPrimaryKey(object primaryKeyValue)
        {
            var items = GetByColumns(new[] {PrimaryKeyName}, new[] {primaryKeyValue});
            return items.Length > 0 ? items[0] : null;
        }

        public T[] GetByColumns(string[] columnNames, object[] columnValues)
        {
            using (var connection = CreateConnection())
            {
                return GetByColumns(connection, columnNames, columnValues);
            }
        }

        public T[] GetAll(DbConnection connection, long limit = 0, Action<T> itemGetCallback = null)
        {
            return GetByColumns(connection, null, null, limit, itemGetCallback);
        }

        public T[] GetAll(long limit = 0, Action<T> itemGetCallback = null)
        {
            using (var connection = CreateConnection())
            {
                return GetByColumns(connection, null, null, limit, itemGetCallback);
            }
        }

        public T[] GetByColumns(DbConnection connection, string[] columnNames, object[] columnValues, long limit = 0, Action<T> itemGetCallback = null)
        {
            if (columnNames != null && columnValues != null && columnNames.Length != columnValues.Length)
            {
                throw new ArgumentException("The columnNames parameter length must be equals to columnValues property length.", "columnNames");
            }
            var objs = new List<T>();            
            var command = connection.CreateCommand();
            try{command.CommandTimeout = FastDataOptions.Instance.CommandTimeout;}catch (Exception) { Debug.WriteLine("Timeout nao aceito."); }
            command.CommandText = FastDataOptions.Instance.SelectQueryPreamble + SelectCommandText;
            if (columnNames != null && columnValues != null)
            {
                command.CommandText += " WHERE ";
                for (var i = 0; i < columnValues.Length; i++)
                {
                    if (i > 0)
                    {
                        command.CommandText += " AND ";
                    }
                    command.CommandText += CreateParameter(columnNames[i], columnValues[i], ref command);
                }   
            }
            using (var dataReader = command.ExecuteReader())
            {
                var i = 0;
                while (dataReader.Read() && (limit == 0 || i < limit))
                {
                    if (itemGetCallback != null)
                    {
                        itemGetCallback(CreateObject(dataReader));                        
                    }
                    else
                    {
                        objs.Add(CreateObject(dataReader));   
                    }                    
                    i++;
                }
                dataReader.Close();
            }            
            return objs.ToArray();
        }

        public List<T> GetTopN(int limit)
        {
            using (var connection = CreateConnection())
            {
                return GetTopN(connection, limit);
            }
        }

        public List<T> GetTopN(DbConnection connection, int limit)
        {
            var objs = new List<T>();            
            var command = connection.CreateCommand();
            try{command.CommandTimeout = FastDataOptions.Instance.CommandTimeout;}catch (Exception) { Debug.WriteLine("Timeout nao aceito."); }
            command.CommandText = FastDataOptions.Instance.SelectQueryPreamble + string.Format(SelectTopNCommandText, FastDataOptions.Instance.GetTop(limit), FastDataOptions.Instance.GetLimit(limit));

            using (var dataReader = command.ExecuteReader())
            {
                var i = 0;
                while (dataReader.Read() && i<limit)
                {
                    objs.Add(CreateObject(dataReader));
                    i++;
                }
                dataReader.Close();
            }            
            return objs;
        }

        public T GetFirst()
        {
            using (var connection = CreateConnection())
            {
                return GetFirst(connection);
            }
        }

        public T GetFirst(DbConnection connection)
        {
            T obj = null;
            var command = connection.CreateCommand();
            try { command.CommandTimeout = FastDataOptions.Instance.CommandTimeout; }
            catch (Exception) { Debug.WriteLine("Timeout nao aceito."); }
            command.CommandText = FastDataOptions.Instance.SelectQueryPreamble + string.Format(SelectTopNCommandText, FastDataOptions.Instance.GetTop(1), FastDataOptions.Instance.GetLimit(1));

            using (var dataReader = command.ExecuteReader())
            {                
                if(dataReader.Read())
                {
                    obj = CreateObject(dataReader);
                }
                dataReader.Close();
            }
            return obj;
        }

        public bool HasChanges(object afterValue)
        {
            using (var connection = CreateConnection())
            {
                var command = connection.CreateCommand();
                try{command.CommandTimeout = FastDataOptions.Instance.CommandTimeout;}catch (Exception) { Debug.WriteLine("Timeout nao aceito."); }
                command.CommandText += DetectChangesCommandText;
                CreateParameter("ChangesColumnValue", afterValue, ref command);
                return command.ExecuteScalar().ToString() != "0";
            }
        }

        public bool Update(T obj, object primaryKeyColumnValue)
        {
            using (var connection = CreateConnection())
            {
                return Update(obj, primaryKeyColumnValue, connection);
            }
        }

        public bool Update(T obj, object primaryKeyColumnValue, DbConnection connection)
        {
            var command = connection.CreateCommand();
            try{command.CommandTimeout = FastDataOptions.Instance.CommandTimeout;}catch (Exception) { Debug.WriteLine("Timeout nao aceito."); }
            CreateUpdateCommand(obj, ref command);
            command.CommandText += CreateParameter(PrimaryKeyName, primaryKeyColumnValue, ref command);
            var result = command.ExecuteNonQuery() != 0;
            return result;
        }

        public bool Update(T obj, object primaryKeyColumnValue, string[] changedProperties, string[] incrementProperties)
        {
            using (var connection = CreateConnection())
            {
                return Update(obj, primaryKeyColumnValue, changedProperties, incrementProperties, connection);
            }
        }

        public bool Update(T obj, object primaryKeyColumnValue, string[] changedProperties, string[] incrementProperties, DbConnection connection)
        {
            if ((changedProperties == null || changedProperties.Length == 0) && (incrementProperties == null || incrementProperties.Length == 0))
            {
                return false;
            }
            var command = connection.CreateCommand();
            try{command.CommandTimeout = FastDataOptions.Instance.CommandTimeout;}catch (Exception) { Debug.WriteLine("Timeout nao aceito."); }
            CreateUpdateCommand(obj, ref command, changedProperties, incrementProperties);
            command.CommandText += CreateParameter(PrimaryKeyName, primaryKeyColumnValue, ref command);
            try
            {
                return command.ExecuteNonQuery() != 0;
            }
            catch (Exception exception)
            {
                throw new Exception(command.CommandText, exception);
            }            
        }

        public bool Insert(T obj)
        {
            bool result;
            if (OnBeforeInsert != null)
            {
                OnBeforeInsert(obj);
            }
            using (var connection = CreateConnection())
            {
                var command = connection.CreateCommand();
                try{command.CommandTimeout = FastDataOptions.Instance.CommandTimeout;}catch (Exception) { Debug.WriteLine("Timeout nao aceito."); }
                CreateInsertCommand(obj, ref command);
                result = command.ExecuteNonQuery() != 0;
                if (FastDataOptions.Instance.AutoLoadIdOnInsert)
                {
                    SetId(obj, connection);
                }
            }
            if (OnInsert != null)
            {
                OnInsert(obj, result);
            }
            return result;
        }

        public bool TryInsert(T obj)
        {
            using (var connection = CreateConnection())
            {
                return TryInsert(obj, connection);
            }
        }

        private void SetId(T obj, DbConnection connection)
        {            
            typeof(T).GetProperty("Id").SetValue(obj, GetLastId(connection), null);
        }

        public bool TryInsert(T obj, DbConnection connection)
        {
            if (OnBeforeInsert != null)
            {
                OnBeforeInsert(obj);
            }
            var command = connection.CreateCommand();
            try{command.CommandTimeout = FastDataOptions.Instance.CommandTimeout;}catch (Exception) { Debug.WriteLine("Timeout nao aceito."); }
            CreateTryInsertCommand(obj, ref command);            
            var result = command.ExecuteNonQuery() != 0;
            if (FastDataOptions.Instance.AutoLoadIdOnInsert)
            {
                SetId(obj, connection);
            }
            if (OnInsert != null)
            {
                OnInsert(obj, result);
            }
            return result;
        }

        public bool TryInsert(T obj, DbCommand command, DbConnection connection)
        {
            if (OnBeforeInsert != null)
            {
                OnBeforeInsert(obj);
            }
            var result = command.ExecuteNonQuery() != 0;
            if (FastDataOptions.Instance.AutoLoadIdOnInsert)
            {
                SetId(obj, connection);
            }
            if (OnInsert != null)
            {
                OnInsert(obj, result);
            }
            return result;
        }

        public long DeleteRange(long min, long max)
        {
            using (var connection = CreateConnection())
            {
                var command = connection.CreateCommand();
                try{command.CommandTimeout = FastDataOptions.Instance.CommandTimeout;}catch (Exception) { Debug.WriteLine("Timeout nao aceito."); }
                command.CommandText = DeleteCommandText + CreateParameter(PrimaryKeyName, min, ref command, ">=", "_MIN") + " AND " + CreateParameter(PrimaryKeyName, max, ref command, "<=", "_MAX");
                return command.ExecuteNonQuery();
            }
        }

        public long DeleteWhere(params object[] whereExpression)
        {
            using (var connection = CreateConnection())
            {
                var command = connection.CreateCommand();
                try{command.CommandTimeout = FastDataOptions.Instance.CommandTimeout;}catch (Exception) { Debug.WriteLine("Timeout nao aceito."); }
                command.CommandText = DeleteCommandText;
                CreateWhere(whereExpression, command);   
                return command.ExecuteNonQuery();                
            }
        }
        
        public long TruncateTable()
        {
            using (var connection = CreateConnection())
            {
                var command = connection.CreateCommand();
                try{command.CommandTimeout = FastDataOptions.Instance.CommandTimeout;}catch (Exception) { Debug.WriteLine("Timeout nao aceito."); }
                command.CommandText = TruncateCommandText;
                long result;
                try
                {
                    result = command.ExecuteNonQuery();
                }
                catch (Exception)
                {
                    command.CommandText = "DELETE FROM " + TableName;
                    result = command.ExecuteNonQuery();
                }
                return result;
            }
        }

        public DbConnection CreateConnection()
        {
            var connection = FastDataOptions.Instance.ConnectionBuilder.CreateConnection();
            connection.Open();
            FastDataOptions.Instance.ConnectionBuilder.SetSchema(connection);
            return connection;
        }

        public List<T> GetWhere(params object[] whereExpression)
        {
            return GetWhere(0, whereExpression);
        }

        public List<T> GetWhere(long limit, params object[] whereExpression)
        {
            using (var connection = CreateConnection())
            {
                var objs = new List<T>();
                var command = connection.CreateCommand();
                try{command.CommandTimeout = FastDataOptions.Instance.CommandTimeout;}catch (Exception) { Debug.WriteLine("Timeout nao aceito."); }
                command.CommandText = FastDataOptions.Instance.SelectQueryPreamble + SelectCommandText + " WHERE ";
                CreateWhere(whereExpression, command);

                using (var dataReader = command.ExecuteReader())
                {
                    var i = 0;
                    while (dataReader.Read() && (limit == 0 || i < limit))
                    {
                        objs.Add(CreateObject(dataReader));
                        i++;
                    }
                    dataReader.Close();
                }
                return objs;
            }
        }

        private static void CreateWhere(object[] whereExpression, DbCommand command)
        {
            for (var i = 0; i <= whereExpression.Length - 3; i = i + 4)
            {
                var columnName = (string) whereExpression[i];
                var theOperator = (string) whereExpression[i + 1];
                var value = whereExpression[i + 2];
                if (value.ToString().Trim() == "NULL")
                {
                    if (theOperator == "<>")
                    {
                        command.CommandText += "NOT ";
                    }
                    command.CommandText += columnName + FastDataOptions.Instance.IsNullExpression;
                }                
                else
                {
                    if (theOperator.ToUpper().Trim() == "NOT LIKE")
                    {
                        command.CommandText += "NOT ";
                        theOperator = "LIKE";
                    }
                    command.CommandText += CreateParameter(columnName, value, ref command, theOperator, "_" + i);
                }
                if(i + 3 < whereExpression.Length)
                {
                    command.CommandText += " " + whereExpression[i + 3] + " ";
                }
            }
        }

        public bool Delete(long primaryKeyValue)
        {
            using (var connection = CreateConnection())
            {
                var command = connection.CreateCommand();
                try{command.CommandTimeout = FastDataOptions.Instance.CommandTimeout;}catch (Exception) { Debug.WriteLine("Timeout nao aceito."); }
                command.CommandText = DeleteCommandText + CreateParameter(PrimaryKeyName, primaryKeyValue, ref command);
                return command.ExecuteNonQuery() > 0;
            }
        }

        public static TableMap<T> Create()
        {
            const string namespaceName = "FastData.DynamicMappings";
            var type = typeof(T);
            var className = type.Name.RemoveSpecialChars().LimitLength(32) + new Random(Guid.NewGuid().GetHashCode()).Next();
            var template = new TableMapTemplate(type, namespaceName, className);
            var text = template.TransformText();
            var path = DirectoryUtils.GetTypeAssemblyPath(typeof(T));
            var provider = new CSharpCodeProvider();
            var parameters = new CompilerParameters { GenerateInMemory = true };
            parameters.ReferencedAssemblies.Add("System.dll");
            parameters.ReferencedAssemblies.Add("System.Data.dll");
            parameters.ReferencedAssemblies.Add("System.Core.dll");
            parameters.ReferencedAssemblies.Add(Path.Combine(path, "FastData.dll"));
            var callingAssembly = Assembly.GetCallingAssembly().Location;
            parameters.ReferencedAssemblies.Add(callingAssembly);
            if (FastDataOptions.Instance.CustomReferencedAssemblies != null)
            {
                foreach (var item in FastDataOptions.Instance.CustomReferencedAssemblies)
                {
                    var include = true;
                    var newFileName = (Path.GetFileName(item) ?? "").ToLower();
                    foreach (var t in parameters.ReferencedAssemblies)
                    {
                        if (!t.ToLower().Contains(newFileName))
                        {
                            continue;
                        }
                        include = false;
                        break;
                    }
                    if(include)
                    {
                        parameters.ReferencedAssemblies.Add(item);
                    }
                }
            }

            if (FastDataOptions.Instance.IncludeDebugInformation)
            {
                //Inclui PDB e gera em arquivo, para permitir profile e debug
                parameters.GenerateInMemory = false;
                parameters.TempFiles = new TempFileCollection(path, true);
                parameters.IncludeDebugInformation = true;
                parameters.TempFiles.KeepFiles = true;
            }
            var results = provider.CompileAssemblyFromSource(parameters, text);
            if (results.Errors.HasErrors)
            {
                throw new Exception("MapBuilder.Create error in " + className + " - " + results.Errors[0].ErrorText + " - Line " + results.Errors[0].Line + ", Column " + results.Errors[0].Column);
            }
            type = results.CompiledAssembly.GetTypes()[0];
            Instance = (TableMap<T>)Activator.CreateInstance(type);
            Instance.AppendPrivatePath(path);            
            return Instance;
        }

        protected static string CreateParameter(string columnName, object value, ref DbCommand command)
        {
            command.Parameters.Add(CreateParameter(columnName, value, command));
            return columnName + "=" + FastDataOptions.Instance.ParameterSqlPrefix + columnName;
        }

        protected static string CreateParameter(string columnName, object value, ref DbCommand command, string compOperator, string parameterSuffix)
        {
            var columnNameWithSuffix = columnName + parameterSuffix;
            command.Parameters.Add(CreateParameter(columnNameWithSuffix, value, command));
            return columnName + " " + compOperator + " " + FastDataOptions.Instance.ParameterSqlPrefix + columnNameWithSuffix;
        }

        protected static IEnumerable<DbParameter> CreateParameter(string[] columnName, object[] value, DbCommand command)
        {
            for (var i = 0; i < columnName.Length; i++)
            {
                yield return CreateParameter(columnName[i], value[i], command);
            }            
        }

        protected static DbParameter CreateParameter(string columnName, object value, DbCommand command)
        {
            var parameter = command.CreateParameter();
            parameter.ParameterName = FastDataOptions.Instance.ParameterNamePrefix + columnName;
            SetParameterValue(value, parameter);
            return parameter;
        }

        

        protected static DbParameter CreateParameterWithFullName(string fullName, object value, DbCommand command)
        {
            var parameter = command.CreateParameter();
            parameter.ParameterName = fullName;
            SetParameterValue(value, parameter);
            return parameter;
        }

        private static void SetParameterValue(object value, DbParameter parameter)
        {
            if (value == null)
            {
                parameter.IsNullable = true;
                if (FastDataOptions.Instance.SaveNullStringValuesAsEmpty && parameter.DbType == DbType.String)
                {
                    parameter.Value = "";
                    #if RELEASE                
                    parameter.DbType = DbType.AnsiString;                
                    #endif
                }
                else if (FastDataOptions.Instance.SaveNullNumericValuesAsZero && (parameter.DbType == DbType.Decimal ||
                                                                                  parameter.DbType == DbType.Int64 ||
                                                                                  parameter.DbType == DbType.Int32 ||
                                                                                  parameter.DbType == DbType.Int16 ||
                                                                                  parameter.DbType == DbType.Double ||
                                                                                  parameter.DbType == DbType.UInt64 ||
                                                                                  parameter.DbType == DbType.UInt32 ||
                                                                                  parameter.DbType == DbType.UInt16 ||
                                                                                  parameter.DbType == DbType.VarNumeric))
                {
                    parameter.Value = 0;
                }
                else
                {
                    parameter.Value = DBNull.Value;
                }
            }
            else
            {
                parameter.Value = value;
                #if RELEASE
                if (parameter.DbType == DbType.String)
                {
                    parameter.DbType = DbType.AnsiString;
                }
                #endif
            }
        }

        public T RunCachedQuerySingle(string name, object[] parameterValues, DbConnection connection)
        {
            var query = _cachedCustomQueries[name];
            return RunCachedQuerySingle(query, parameterValues, connection);
        }

        internal T RunCachedQuerySingle(CachedCustomQuery query, object[] parameterValues, DbConnection connection)
        {
            T obj = null;            
            var command = CreateDbCommandFromCachedCustomQuery(query, parameterValues, connection);
            using (var dataReader = command.ExecuteReader())
            {
                if (dataReader.Read())
                {
                    obj = CreateObject(dataReader);
                }
                dataReader.Close();
            }
            return obj;
        }

        public T[] RunCachedQuery(string name, object[] parameterValues, DbConnection connection)
        {
            var query = _cachedCustomQueries[name];
            return RunCachedQuery(query, parameterValues, connection);
        }

        internal T[] RunCachedQuery(CachedCustomQuery query, object[] parameterValues, DbConnection connection)
        {
            var command = CreateDbCommandFromCachedCustomQuery(query, parameterValues, connection);
            var objs = new List<T>();
            using (var dataReader = command.ExecuteReader())
            {
                while (dataReader.Read())
                {
                    objs.Add(CreateObject(dataReader));
                }
                dataReader.Close();
            }
            return objs.ToArray();
        }

        public void AddCachedCustomQuery(string name, string[] columnNames)
        {
            if (_cachedCustomQueries == null)
            {
                _cachedCustomQueries = new Dictionary<string, CachedCustomQuery>();
            }
            _cachedCustomQueries.Add(name, CreateCachedCustomQuery(name, columnNames));
        }

        private static DbCommand CreateDbCommandFromCachedCustomQuery(CachedCustomQuery query, object[] parameterValues, DbConnection connection)
        {
            var command = connection.CreateCommand();
            try{command.CommandTimeout = FastDataOptions.Instance.CommandTimeout;}catch (Exception) { Debug.WriteLine("Timeout nao aceito."); }
            command.CommandText = query.CommandText;            
            for (var i = 0; i < query.ParameterNames.Length; i++)
            {
                var parameterName = query.ParameterNames[i];
                var parameter = command.CreateParameter();
                parameter.ParameterName = parameterName;
                SetParameterValue(parameterValues[i], parameter);                
                command.Parameters.Add(parameter);
            }
            return command;
        }

        internal CachedCustomQuery CreateCachedCustomQuery(string name, string[] columnNames)
        {
            var commandText = new StringBuilder(256);
            var parameters = new List<string>();
            commandText.Append(FastDataOptions.Instance.SelectQueryPreamble + SelectCommandText + " WHERE ");
            for (var i = 0; i < columnNames.Length; i++)
            {
                if (i > 0)
                {
                    commandText.Append(" AND ");
                }
                var parameterName = FastDataOptions.Instance.ParameterNamePrefix + columnNames[i];
                parameters.Add(parameterName);
                commandText.Append(columnNames[i] + "=" + FastDataOptions.Instance.ParameterSqlPrefix + columnNames[i]);
            }
            return new CachedCustomQuery
            {
                Name = name,
                ColumnNames = columnNames,
                CommandText = commandText.ToString(),
                ParameterNames = parameters.ToArray()
            };
        }
    }
}
