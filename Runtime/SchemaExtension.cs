using System;

namespace Amlos.Container
{
    public static class SchemaExtension
    {
        public static Schema_Old Variate(this Schema_Old schema, Action<SchemaBuilder> edit)
        {
            var builder = SchemaBuilder.FromSchema(schema, true);
            edit(builder);
            return builder.Build();
        }
    }
}
