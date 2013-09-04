using System.Data;
using System.Data.Common;

namespace FastData.Configuration
{
    public interface IConnectionBuilder
    {
        string CurrentSchema { get; set; }

        DbConnection CreateConnection();

        long ExecuteScalar(string commandText);

        DataTable ExecuteQuery(string commandText);

        void SetSchema(DbConnection connection);
    }
}
