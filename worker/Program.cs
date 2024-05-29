using System;
using System.Threading;
using StackExchange.Redis;
using Npgsql;

namespace Worker
{
    public class Program
    {
        private static ConnectionMultiplexer _redisConn;
        private static IDatabase _redisDb;

        public static int Main(string[] args)
        {
            try
            {
                _redisConn = OpenRedisConnection();
                _redisDb = _redisConn.GetDatabase();

                // Temporizador de 20s
                var timer = new Timer(state => RefreshMovies(), null, TimeSpan.Zero, TimeSpan.FromSeconds(20));

                while (true)
                {
                    Thread.Sleep(100);

                    if (_redisConn == null || !_redisConn.IsConnected)
                    {
                        Console.WriteLine("Reconnecting Redis");
                        _redisConn = OpenRedisConnection();
                        _redisDb = _redisConn.GetDatabase();
                    }
                    RedisValue value = _redisDb.ListLeftPop("movies");
                    if (!value.IsNull)
                    {
                        string movieName = value.ToString();
                        string movieList = _redisDb.StringGet(movieName);
                        Console.WriteLine($"Processing movie '{movieName}'");
                        SaveToPostgres(movieName, movieList);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine(ex.ToString());
                return 1;
            }
        }

        private static void RefreshMovies()
        {
            Console.WriteLine("Refrescando películas...");

            // Conexión a PostgreSQL
            var pgConnectionString = "Host=mi_postgres_container;Username=postgres;Password=password;Database=recomendaciones_peliculas";

            using (var pgConn = new NpgsqlConnection(pgConnectionString))
            {
                try
                {
                    pgConn.Open();
                    Console.WriteLine("Connected to PostgreSQL");

                    using (var cmdDelete = new NpgsqlCommand())
                    {
                        cmdDelete.Connection = pgConn;
                        cmdDelete.CommandText = "DELETE FROM movies";
                        cmdDelete.ExecuteNonQuery();
                        Console.WriteLine("Deleted all existing records from the movies table");
                    }
                }
                catch (Exception ex)
                {
                    Console.Error.WriteLine($"Error connecting to PostgreSQL: {ex.Message}");
                    return;
                }
            }

            var redisKeys = _redisConn.GetServer("my-redis-container", 6379).Keys(pattern: "*");

            foreach (var redisKey in redisKeys)
            {
                string movieName = redisKey.ToString();
                string movieList = _redisDb.StringGet(redisKey);
                Console.WriteLine($"Processing movie '{movieName}'");
                SaveToPostgres(movieName, movieList);
            }
        }

        private static void SaveToPostgres(string movieName, string movieList)
        {
            var connectionString = "Host=mi_postgres_container;Username=postgres;Password=password;Database=recomendaciones_peliculas";
            Console.WriteLine($"Connecting to PostgreSQL at {connectionString}");

            using (var conn = new NpgsqlConnection(connectionString))
            {
                try
                {
                    conn.Open();
                    Console.WriteLine("Connected to PostgreSQL");

                    using (var cmd = new NpgsqlCommand())
                    {
                        cmd.Connection = conn;
                        cmd.CommandText = "INSERT INTO movies (movie_name, movie_list) VALUES (@movieName, @movieList)";
                        // Utiliza el título de la película como parámetro
                        cmd.Parameters.AddWithValue("@movieName", movieName);
                        cmd.Parameters.AddWithValue("@movieList", movieList);
                        cmd.ExecuteNonQuery();
                        Console.WriteLine($"Data inserted successfully for movie '{movieName}'");
                    }
                }
                catch (Exception ex)
                {
                    Console.Error.WriteLine($"Error connecting to PostgreSQL: {ex.Message}");
                }
            }
        }


        private static ConnectionMultiplexer OpenRedisConnection()
        {
            var options = ConfigurationOptions.Parse("my-redis-container");
            options.ConnectTimeout = 5000;

            while (true)
            {
                try
                {
                    Console.WriteLine("Connecting to Redis");
                    var redisConn = ConnectionMultiplexer.Connect(options);
                    Console.WriteLine("Connected to Redis");
                    return redisConn;
                }
                catch (RedisConnectionException)
                {
                    Console.WriteLine("Waiting for Redis");
                    Thread.Sleep(1000);
                }
            }
        }
    }
}
