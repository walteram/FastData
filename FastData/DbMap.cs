using System;

namespace FastData
{
    public class DbMap : IDisposable
    {      
        public TableMap<T> GetTableMap<T>() where T : class, new()
        {
            return TableMap<T>.Instance ?? TableMap<T>.Create();
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposing)
            {
                return;
            }
            foreach (var field in GetType().GetFields())
            {
                if (!field.FieldType.IsGenericType || field.FieldType.GetGenericTypeDefinition() != typeof (TableMap<>))
                {
                    continue;
                }
                var map = field.GetValue(this) as IDisposable;
                if (map != null)
                {
                    map.Dispose();
                }
            }
            foreach (var property in GetType().GetProperties())
            {
                if (!property.PropertyType.IsGenericType || property.PropertyType.GetGenericTypeDefinition() != typeof (TableMap<>))
                {
                    continue;
                }
                var map = property.GetValue(this, null) as IDisposable;
                if (map != null)
                {
                    map.Dispose();
                }
            }
        }
    }
}
