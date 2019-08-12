using Nest;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ElasticSearchSample.Console
{
    public class EmployeeManage
    {
        private readonly string _indexName = "employees";
        private readonly EsProvider _es;

        public EmployeeManage()
        {
            _es = new EsProvider(_indexName);
        }

        /// <summary>
        /// 创建索引mapping
        /// </summary>
        /// <param name="es"></param>
        /// <returns></returns>
        public async Task CreateIndexAsync()
        {
            var existResponse = await _es.HightClient.Indices.ExistsAsync(Indices.Index(_indexName));
            if (existResponse.Exists)
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
            var createResponse = await _es.HightClient.Indices.CreateAsync(Indices.Index(_indexName), descriptor =>
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
        /// 更新索引mapping
        /// </summary>
        /// <returns></returns>
        public async Task PutMappingAsync()
        {
            var request = new PutMappingRequest<Employee>();
            request.DateDetection = true;
            request.Dynamic = new Union<bool, DynamicMapping>(DynamicMapping.Strict);

            var container = new Dictionary<PropertyName, IProperty>();
            // TextProperty实例对象可以设置分词器
            container.Add(new PropertyName(nameof(Employee.NewField)), new TextProperty() { Fielddata = true});
            request.Properties = new Properties(container);

            var response = await _es.HightClient.MapAsync(request);
            if (response.Acknowledged)
            {
                System.Console.WriteLine($"更新{_indexName} mapping成功");
            }
            else
            {
                System.Console.WriteLine(response.DebugInformation);
            }
        }

        /// <summary>
        /// 更新索引的setting
        /// </summary>
        /// <param name="es"></param>
        /// <returns></returns>
        public async Task UpdateSettingsAsync()
        {
            var request = new UpdateIndexSettingsRequest(Indices.Index(_indexName))
            {
                IndexSettings = new IndexSettings
                {
                    NumberOfReplicas = 0
                }
            };
            var response = await _es.HightClient.Indices.UpdateSettingsAsync(request);
            if (response.Acknowledged)
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
        public async Task CreateDocumentAsync()
        {
            var response = await _es.HightClient.CreateDocumentAsync(new Employee
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

            response = await _es.HightClient.CreateDocumentAsync(new Employee
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

            response = await _es.HightClient.CreateDocumentAsync(new Employee
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

            response = await _es.HightClient.CreateDocumentAsync(new Employee
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

            response = await _es.HightClient.CreateDocumentAsync(new Employee
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

        /// <summary>
        /// 批量创建Document
        /// </summary>
        /// <returns></returns>
        public async Task BatchCreateDoumentAsync()
        {
            var employees = new List<Employee>();
            var names = new string[10] { "lucy li", "jack sun", "jim shao", "han shao", "han mei", "tom zhang", "chen long", "li lian jie", "fuck ri", "shz" };
            var provinces = new List<string> { "广东", "广西", "江西", "江苏", "湖南", "湖北", "山东", "北京", "上海" };
            for (int i = 0; i < names.Length; i++)
            {
                var employee = new Employee
                {
                    Id = i,
                    Name = names[i],
                    BirthDay = System.DateTime.Now.Date.AddYears(0 - new Random().Next(20, 40)),
                    Gender = i % 2 == 0 ? "女" : "男",
                    Home = new Address { Province = provinces[new Random().Next(0, provinces.Count + 1)], City = "" },
                    Mobile = "10123456789",
                    JoinDate = System.DateTime.Now.AddDays(0 - new Random().Next(0, 1000)),
                    Salary = new Random().Next(10000,100000)
                };
                employees.Add(employee);
            }

            //var response = await _es.HightClient.IndexManyAsync(employees);
            var response = await _es.HightClient.BulkAsync(b => b.IndexMany(employees));
            if (response.Errors)
            {
                foreach (var item in response.ItemsWithErrors)
                {
                    System.Console.WriteLine($"创建Id为{item.Id}的doc失败：{item.Error}");
                }
            }
            else
            {
                System.Console.WriteLine("批量创建Employee信息成功");
            }
        }

        /// <summary>
        /// 查询所有雇员
        /// </summary>
        /// <returns></returns>
        public async Task SearchMatchAllAsync()
        {
            //var response = await _es.HightClient.SearchAsync<Employee>(selector =>
            //{
            //    var descriptor = new SearchDescriptor<Employee>();
            //    descriptor.MatchAll();
            //    return descriptor;
            //});

            var response = await _es.HightClient.SearchAsync<Employee>(s => s.Query
            (
                q => q.MatchAll()
                ));

            if (response.Documents != null && response.Documents.Count > 0)
            {
                foreach (var employee in response.Documents)
                {
                    System.Console.WriteLine(employee.ToString());
                }
            }
        }

        /// <summary>
        /// 根据条件查询
        /// </summary>
        /// <returns></returns>
        public async Task SearchMatchAsync()
        {
            var response = await _es.HightClient.SearchAsync<Employee>(s => s.Query(
                q => q.DateRange(dr => 
                dr.Field(f => f.JoinDate)
                .GreaterThanOrEquals(new DateTime(2018, 1, 1))
                .LessThan(DateTime.Now.Date))));

            System.Console.WriteLine("查询入职日期在2018年以后的所有员工");
            if (response.Documents != null && response.Documents.Count > 0)
            {
                foreach (var employee in response.Documents)
                {
                    System.Console.WriteLine(employee.ToString());
                }
            }

            System.Console.WriteLine("------------------------------------------------");

            // 全文检索
            response = await _es.HightClient.SearchAsync<Employee>(s => s.Query
            (q => q.Match(m => m.Field(f => f.Name).Query("jim shao"))));
            System.Console.WriteLine("查询姓名中含有 jim 或者 shao 的所有员工");
            if (response.Documents != null && response.Documents.Count > 0)
            {
                foreach (var employee in response.Documents)
                {
                    System.Console.WriteLine(employee.ToString());
                }
            }

            System.Console.WriteLine("------------------------------------------------");

            // 短语搜索，精确匹配
            response = await _es.HightClient.SearchAsync<Employee>(s => s
                .Query(q => q
                    .MatchPhrase(p=>p.Field(f=>f.Name).Query("jim shao"))
                )
            );
            System.Console.WriteLine("查询姓名为jim shao的所有员工");
            if (response.Documents != null && response.Documents.Count > 0)
            {
                foreach (var employee in response.Documents)
                {
                    System.Console.WriteLine(employee.ToString());
                }
            }

            System.Console.WriteLine("------------------------------------------------");

            response = await _es.HightClient.SearchAsync<Employee>(s => s.Query
            (q => q.MatchPhrase(m => m.Field(
                  f => f.Home.Province)
              .Query("广西"))));
            System.Console.WriteLine("查询广西籍所有员工");
            if (response.Documents != null && response.Documents.Count > 0)
            {
                foreach (var employee in response.Documents)
                {
                    System.Console.WriteLine(employee.ToString());
                }
            }

            System.Console.WriteLine("------------------------------------------------");
        }
    }
}
