using System.Threading.Tasks;
using Nest;

namespace ElasticSearchSample.Console
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var manage = new EmployeeManage();
            await manage.CreateIndexAsync();
            await manage.PutMappingAsync();
            await manage.UpdateSettingsAsync();

            //await manage.BatchCreateDoumentAsync();

            await manage.SearchMatchAllAsync();
            await manage.SearchMatchAsync();
            System.Console.ReadLine();
        }
    }
}
