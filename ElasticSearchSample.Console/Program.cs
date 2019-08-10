using System.Threading.Tasks;
using Nest;

namespace ElasticSearchSample.Console
{
    class Program
    {
        static string _indexName = "employees";

        static async Task Main(string[] args)
        {
            var es = new EsProvider(_indexName);
            await CreateIndexAsync(es);
            await UpdateSettingsAsync(es);
            //await CreateDocumentAsync(es);

            System.Console.ReadLine();

        }

        /// <summary>
        /// 创建索引mapping
        /// </summary>
        /// <param name="es"></param>
        /// <returns></returns>
        static async Task CreateIndexAsync(EsProvider es)
        {
            var existResponse = await es.HightClient.Indices.ExistsAsync(Indices.Index(_indexName));
            if(existResponse.Exists)
            {
                System.Console.WriteLine($"已经存在名称为{_indexName}的索引");
                return;
            }

            // fluent mapping
            //var createResponse = await es.HightClient.Indices.CreateAsync(index, descriptor =>
            // {
            //     descriptor.Map<Employee>(m =>
            //     {
            //         return m.Properties(p =>
            //         {
            //             return p.Number(f => f.Name(e => e.Id).Type(NumberType.Long))
            //             .Number(f => f.Name(e => e.Salary).Type(NumberType.Float))
            //             .Text(f => f.Name(e => e.Name))
            //             .Text(f => f.Name(e => e.Mobile))
            //             .Text(f => f.Name(e => e.Gender))
            //             .Date(f => f.Name(e => e.BirthDay))
            //             .Date(f => f.Name(e => e.JoinDate))
            //             .Object<Address>(o => o.Name(e => e.Home)
            //             .Properties(hp =>
            //             {
            //                 return hp.Text(hpp => hpp.Name(e => e.Province))
            //              .Text(hpp => hpp.Name(e => e.City));
            //             }));
            //         });
            //     });
            //     return descriptor;
            // });

            // auto mapping
            var createResponse = await es.HightClient.Indices.CreateAsync(Indices.Index(_indexName), descriptor =>
             {
                 return descriptor.Map<Employee>(m =>
                 {
                     return m.AutoMap()
                     .AutoMap<Address>();
                 });
             });

            if (createResponse.Acknowledged)
            {
                System.Console.WriteLine($"创建{_indexName}索引成功");
            }
            else
            {
                System.Console.WriteLine(createResponse.DebugInformation);
            }
        }

        /// <summary>
        /// 更新索引的setting
        /// </summary>
        /// <param name="es"></param>
        /// <returns></returns>
        static async Task UpdateSettingsAsync(EsProvider es)
        {
            var request = new UpdateIndexSettingsRequest(Indices.Index(_indexName))
            {
                IndexSettings = new IndexSettings
                {
                    NumberOfReplicas = 0
                }
            };
            var response = await es.HightClient.Indices.UpdateSettingsAsync(request);
            if(response.Acknowledged)
            {
                System.Console.WriteLine($"更新{_indexName}索引设置属性成功");
            }
            else
            {
                System.Console.WriteLine(response.DebugInformation);
            }
        }

        /// <summary>
        /// 创建Document
        /// </summary>
        /// <param name="es"></param>
        /// <returns></returns>
        static async Task CreateDocumentAsync(EsProvider es)
        {
            var response = await es.HightClient.CreateDocumentAsync<Employee>(new Employee
            {
                Id = 1,
                Name = "shz",
                BirthDay = System.DateTime.Now.AddYears(-30),
                Gender = "男",
                Home = new Address { Province = "江西省", City = "九江市" },
                Mobile = "12345678901",
                JoinDate = System.DateTime.Now,
                Salary = 10000
            });
            System.Console.WriteLine(response.DebugInformation);
            System.Console.WriteLine("----------------------------------");

            response = await es.HightClient.CreateDocumentAsync<Employee>(new Employee
            {
                Id = 2,
                Name = "shao hua",
                BirthDay = System.DateTime.Now.AddYears(-12),
                Gender = "男",
                Home = new Address { Province = "江西省", City = "南昌市" },
                Mobile = "12345678903",
                JoinDate = System.DateTime.Now,
                Salary = 3456
            });
            System.Console.WriteLine(response.DebugInformation);
            System.Console.WriteLine("----------------------------------");

            response = await es.HightClient.CreateDocumentAsync<Employee>(new Employee
            {
                Id = 3,
                Name = "li hua",
                BirthDay = System.DateTime.Now.AddYears(-23),
                Gender = "男",
                Home = new Address { Province = "广东省", City = "深圳市" },
                Mobile = "345242344645",
                JoinDate = System.DateTime.Now,
                Salary = 12334
            });
            System.Console.WriteLine(response.DebugInformation);
            System.Console.WriteLine("----------------------------------");

            response = await es.HightClient.CreateDocumentAsync<Employee>(new Employee
            {
                Id = 4,
                Name = "mary hou",
                BirthDay = System.DateTime.Now.AddYears(-15),
                Gender = "女",
                Home = new Address { Province = "上海市", City = "上海市" },
                Mobile = "12423423435645",
                JoinDate = System.DateTime.Now,
                Salary = 23000
            });
            System.Console.WriteLine(response.DebugInformation);
            System.Console.WriteLine("----------------------------------");

            response = await es.HightClient.CreateDocumentAsync<Employee>(new Employee
            {
                Id = 5,
                Name = "lucy liu",
                BirthDay = System.DateTime.Now.AddYears(-15),
                Gender = "女",
                Home = new Address { Province = "浙江省", City = "杭州市" },
                Mobile = "76896765",
                JoinDate = System.DateTime.Now,
                Salary = 34567
            });
            System.Console.WriteLine(response.DebugInformation);
            System.Console.WriteLine("----------------------------------");
        }



        static async Task CreateMappingAsync(EsProvider es)
        {
            var request = new PutMappingRequest<Employee>();
            request.DateDetection = true;
            request.Dynamic = new Union<bool, DynamicMapping>(DynamicMapping.Strict);

            var response = await es.HightClient.MapAsync(request);
            if(response.Acknowledged)
            {
                System.Console.WriteLine("创建Employee mapping成功");
            }
        }
    }
}
