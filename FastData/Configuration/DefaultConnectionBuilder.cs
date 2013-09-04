using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;

namespace FastData.Configuration
{
    public class DefaultConnectionBuilder : IConnectionBuilder
    {
        private string _connectionStringName;
        public string ConnectionStringName
        {
            get
            {
                return _connectionStringName ?? "DataContext";
            }
            set
            {
                _connectionStringName = value;
            }
        }

        public long ExecuteScalar(string commandText)
        {
            using (var connection = CreateConnection())
            {
                var command = connection.CreateCommand();
                command.CommandTimeout = FastDataOptions.Instance.CommandTimeout;
                command.CommandText = commandText;
                connection.Open();
                SetSchema(connection);
                var affected = command.ExecuteNonQuery();
                connection.Close();
                return affected;
            }
        }

        public DataTable ExecuteQuery(string commandText)
        {
            using (var connection = CreateConnection())
            {
                var command = connection.CreateCommand();
                command.CommandText = commandText;
                connection.Open();
                SetSchema(connection);
                var reader = command.ExecuteReader(CommandBehavior.CloseConnection);
                var dataTable = new DataTable();
                dataTable.Load(reader);
                reader.Close();
                return dataTable;
            }
        }

        public void SetSchema(DbConnection connection)
        {
            
        }

        public string CurrentSchema { get; set; }

        public DbConnection CreateConnection()
        {            
            var connectionString = ConfigurationManager.ConnectionStrings[ConnectionStringName];
            return new SqlConnection(connectionString.ConnectionString);
        }
    }
}
