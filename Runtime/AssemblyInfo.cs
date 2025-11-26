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
        /// <summary>
        /// The game revision <br/>
        /// Only need to change when want to publish a same version name but with changes on save/load
        /// </summary> 
        public const string VisionRevision = "0";
        public const string Version = "0.3.0";
        public const string FileVersion = Version + "." + VisionRevision;
    }
}
