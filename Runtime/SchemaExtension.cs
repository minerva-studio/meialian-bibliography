using System;

namespace Amlos.Container
{
    public static class SchemaExtension
    {
        public static Schema Variate(this Schema schema, Action<SchemaBuilder> edit)
        {
            var builder = SchemaBuilder.FromSchema(schema, true);
            edit(builder);
            return builder.Build();
        }
    }
}
