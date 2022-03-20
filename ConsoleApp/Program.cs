using CsvHelper;
using Gremlin.Net.Driver;
using Gremlin.Net.Structure.IO.GraphSON;
using System.Globalization;
using System.Net.WebSockets;

var Host = "xxx.gremlin.cosmos.azure.com";
var PrimaryKey = "xxx";
var Database = "sample-database";
var Container = "recommend-graph";
var containerLink = "/dbs/" + Database + "/colls/" + Container;
var gremlinServer = new GremlinServer(Host, 443, enableSsl: true, username: containerLink, password: PrimaryKey);
var connectionPoolSettings = new ConnectionPoolSettings()
{
    MaxInProcessPerConnection = 10,
    PoolSize = 30,
    ReconnectionAttempts = 3,
    ReconnectionBaseDelay = TimeSpan.FromMilliseconds(500)
};
var webSocketConfiguration =
    new Action<ClientWebSocketOptions>(options =>
    {
        options.KeepAliveInterval = TimeSpan.FromSeconds(10);
    });

Console.WriteLine("Start.");
using (var gremlinClient = new GremlinClient(gremlinServer, new GraphSON2Reader(), new GraphSON2Writer(),
                                    "application/vnd.gremlin-v2.0+json", connectionPoolSettings, webSocketConfiguration))
{
    Console.WriteLine("Adding Vetex of anime.");
    using (var reader = new StreamReader("anime-demo.csv"))
    using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
    {
        while (csv.Read())
        {
            var record = csv.GetRecord<Anime>();
            var query = $"g.addV('anime').property('id', '{record.anime_id}').property('title', '{record.name}').property('pk', '{record.anime_id}')";
            await gremlinClient.SubmitAsync<dynamic>(query);
        }
    }

    Console.WriteLine("Adding Vetex of user.");
    using (var reader = new StreamReader("user-demo.csv"))
    using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
    {
        while (csv.Read())
        {
            var record = csv.GetRecord<User>();
            var query = $"g.addV('user').property('id', 'user{record.user_id}').property('name', '{record.user_name}').property('pk', 'user{record.user_id}')";
            await gremlinClient.SubmitAsync<dynamic>(query);
        }
    }

    Console.WriteLine("Adding Edge of rating.");
    using (var reader = new StreamReader("rating-demo.csv"))
    using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
    {
        while (csv.Read())
        {
            var record = csv.GetRecord<Rating>();

            try
            {
               var query = $"g.V().hasLabel('user').has('id', 'user{record.user_id}').addE('rates').property('weight', {record.rating}).to(g.V().has('id', '{record.anime_id}'))";
               await gremlinClient.SubmitAsync<dynamic>(query);
            }
            catch (ArgumentOutOfRangeException ex) when (ex.ParamName == "ValueKind")
            {
               // ↓ の例外が発生するが登録自体は成功しているため、正常終了とする
               // JSON type not supported. (Parameter 'ValueKind')
               // Actual value was Number.
            }
        }
    }
}
Console.WriteLine("Finish.");

class Anime
{
    public string anime_id { get; set; } = null!;

    public string name { get; set; } = null!;
}

class Rating
{
    public string user_id { get; set; } = null!;

    public string anime_id { get; set; } = null!;

    public int rating { get; set; }
}

class User
{
    public string user_id { get; set; } = null!;

    public string user_name { get; set; } = null!;
}
