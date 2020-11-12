namespace MonetDb.Mapi
{

    using MonetDb.Mapi.Enums;

    /// <summary>
    /// Represents a local transaction.
    /// </summary>
    public static class MonetDbEnviroments
    {
        public static CommandCloseStrategy CommandCloseStrategy { get; set; } = CommandCloseStrategy.None;
    }
}