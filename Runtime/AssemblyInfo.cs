using Minerva.DataStorage;
using System.Reflection;
using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("Minerva.DataStorage.Editor")]
[assembly: InternalsVisibleTo("Minerva.DataStorage.Tests")]
[assembly: AssemblyVersion(AssemblyInfo.Version)]
[assembly: AssemblyFileVersion(AssemblyInfo.FileVersion)]

namespace Minerva.DataStorage
{
    public class AssemblyInfo
    {
        public const string VisionRevision = "0";
        public const string Version = "0.3.1";
        public const string FileVersion = Version + "." + VisionRevision;
    }
}
