namespace Parquet.Schema
{
    /// <summary>
    /// Schema element for <see cref="TimeSpan"/> which allows to specify precision
    /// </summary>
    public class EnumField : DataField
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="TimeSpanDataField"/> class.
        /// </summary>
        /// <param name="name">The name.</param>
        /// <param name="format">The format.</param>
        /// <param name="isNullable"></param>
        public EnumField(string name, bool isNullable = false)
            : base(name, typeof(int))
        {
            IsNullable = isNullable;
        }
    }
}
