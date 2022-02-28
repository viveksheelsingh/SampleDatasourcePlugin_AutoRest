using System.Text;

namespace SamplePlugin
{
    /// <summary>
    /// Barebone mock source dataplane.
    /// </summary>
    internal static class MockSourceDataplane
    {
        private static string backupContent = "Just a small random backup content";
        public static int maxReadSize = backupContent.Length;
        public static string bkpContentFile = @"backupContent";

        public static Stream DoBackup(ILogger logger)
        {
            logger.LogInformation($"SrcDataplane--> Returning backup content: {backupContent}", backupContent);
            return new MemoryStream(Encoding.ASCII.GetBytes(backupContent));
        }

        public static async Task DoRestore(Stream stream, ILogger logger)
        {
            byte[] buffer = new byte[maxReadSize];
            await stream.ReadAsync(buffer, 0, maxReadSize);
            string restoredContent = Encoding.ASCII.GetString(buffer);
            logger.LogInformation("SrcDataplane--> Read: {0} from: {1}", restoredContent, Path.Combine(".", bkpContentFile));
            if (!restoredContent.Equals(backupContent))
            {
                throw new Exception("Restore content != Backup Content");
            }
        }
        
    }
}
