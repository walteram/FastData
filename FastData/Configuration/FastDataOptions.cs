using System.Collections.Generic;

namespace FastData.Configuration
{
    public class FastDataOptions
    {
        public bool UseSystemConvert { get; set; }

        public int CommandTimeout { get; set; }

        public string SelectQueryPreamble { get; set; }
        
        public string BeginTryExpression { get; set; }

        public string EndTryExpression { get; set; }

        public string TopExpression { get; set; }

        public string LimitExpression { get; set; }

        public string IsNullExpression { get; set; }

        public string ParameterNamePrefix { get; set; }
        
        public string ParameterSqlPrefix { get; set; }

        public bool AutoLoadIdOnInsert { get; set; }

        public ITypeInfoLoader TypeInfoLoader { get; set; }

        public IConnectionBuilder ConnectionBuilder { get; set; }

        public List<string> CustomReferencedAssemblies { get; set; }

        public bool IncludeDebugInformation { get; set; }

        public bool SaveNullStringValuesAsEmpty { get; set; }

        public bool SaveNullNumericValuesAsZero { get; set; }

        public static FastDataOptions Instance;

        static FastDataOptions()
        {
            Instance = new FastDataOptions();
        }
        
        private FastDataOptions()
        {
            CommandTimeout = 600;
            TypeInfoLoader = new DefaultTypeInfoLoader();
            ConnectionBuilder = new DefaultConnectionBuilder();
            IsNullExpression = " IS NULL ";
            ParameterNamePrefix = "@";
            ParameterSqlPrefix = "@";
            TopExpression = "TOP";
        }

        public string GetTop(int num)
        {
            if (string.IsNullOrEmpty(TopExpression))
            {
                return "";
            }
            return TopExpression + " " + num;
        }

        public string GetLimit(int num)
        {
            if (string.IsNullOrEmpty(LimitExpression))
            {
                return "";
            }
            return LimitExpression + " " + num;
        }
    }
}
