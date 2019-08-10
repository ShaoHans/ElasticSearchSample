using Elasticsearch.Net;
using Nest;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ElasticSearchSample.Console
{
    public class EsProvider
    {
        public EsProvider(string indexName)
        {
            var config = new ElasticSearchConfig()
            {
                Hosts = new string[] { "http://localhost:9200" },
                TimeOut = 30
            };

            var pool = new StaticConnectionPool(config.Hosts.Select(h => new Uri(h)));
            var settings = new ConnectionSettings(pool)
                .RequestTimeout(TimeSpan.FromSeconds(config.TimeOut))
                .DefaultMappingFor<Employee>((descriptor =>
                {
                    return descriptor.IndexName("employees");
                }))
                .DefaultIndex(indexName);
            HightClient = new ElasticClient(settings);
            LowClient = new ElasticLowLevelClient(settings);
        }

        public ElasticClient HightClient { get; }
        public ElasticLowLevelClient LowClient { get; }
    }

    public class ElasticSearchConfig
    {
        public string[] Hosts { get; set; }

        public int TimeOut { get; set; }
    }
}
