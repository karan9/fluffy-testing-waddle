using System;
using System.Collections.Generic;
using MongoDB.Bson;
using MongoDB.Driver;


namespace MongoTestHarness.Services
{
    public class MainService
    {
        private readonly IMongoCollection<BsonDocument> baseCollection;
        private readonly IMongoCollection<BsonDocument> airbnbCollection;
        private readonly IMongoCollection<BsonDocument> moviesCollection;

//        private readonly string CONNECTION_STRING = "mongodb+srv://ubisoft:ubisoft@cluster1.afhfm.mongodb.net/myFirstDatabase?retryWrites=true&w=majority&minPoolSize=20&maxPoolSize=1000&waitQueueTimeoutMS=60000&maxIdleTimeMS=1500000";
        private readonly string CONNECTION_STRING = "mongodb+srv://ubisoft:ubisoft@cluster1.afhfm.mongodb.net/myFirstDatabase?retryWrites=true&w=majority";
        private readonly string DB_NAME = "ubisoft";
        private readonly string DB_AIRBNB = "sample_airbnb";
        private readonly string DB_MOVIES = "sample_mflix";

        private readonly string threadName;

        public MainService()
        {

            var client = new MongoClient(CONNECTION_STRING);

            Console.WriteLine(" =================  Verify Connection Settings ================");
            Console.WriteLine("Client Max Pool Size " + client.Settings.MaxConnectionPoolSize.ToString());
            Console.WriteLine("Client Min Pool Size " + client.Settings.MinConnectionPoolSize.ToString());
            Console.WriteLine("Client MaxIdleTimeMS " + client.Settings.MaxConnectionIdleTime.ToString());
            Console.WriteLine("Client WaitQueueTimeOutMS " + client.Settings.WaitQueueTimeout.ToString());
            Console.WriteLine("Client Wait Queue Size" + client.Settings.WaitQueueSize.ToString());
            Console.WriteLine(" =================  Verify Connection Settings ================");

            // Mark databases
            var databaseUbisoft = client.GetDatabase(DB_NAME);
            var databaseSampleAirBNB = client.GetDatabase(DB_AIRBNB);
            var databaseSampleMFLIX = client.GetDatabase(DB_MOVIES);


            // Gather all the collections
            baseCollection = databaseUbisoft.GetCollection<BsonDocument>("testdata");
            airbnbCollection = databaseSampleAirBNB.GetCollection<BsonDocument>("listingsAndReviews");
            moviesCollection = databaseSampleMFLIX.GetCollection<BsonDocument>("movies");
        }

        public MainService(string tname)
        {

            var client = new MongoClient(CONNECTION_STRING);

            Console.WriteLine(" =================  Verify Connection Settings ================");
            Console.WriteLine("Client Max Pool Size " + client.Settings.MaxConnectionPoolSize.ToString());
            Console.WriteLine("Client Min Pool Size " + client.Settings.MinConnectionPoolSize.ToString());
            Console.WriteLine("Client MaxIdleTimeMS " + client.Settings.MaxConnectionIdleTime.ToString());
            Console.WriteLine("Client WaitQueueTimeOutMS " + client.Settings.WaitQueueTimeout.ToString());
            Console.WriteLine("Client Wait Queue Size" + client.Settings.WaitQueueSize.ToString());
            Console.WriteLine(" =================  Verify Connection Settings ================");

            // Mark databases
            var databaseUbisoft = client.GetDatabase(DB_NAME);
            var databaseSampleAirBNB = client.GetDatabase(DB_AIRBNB);
            var databaseSampleMFLIX = client.GetDatabase(DB_MOVIES);


            // Gather all the collections
            baseCollection = databaseUbisoft.GetCollection<BsonDocument>("testdata");
            airbnbCollection = databaseSampleAirBNB.GetCollection<BsonDocument>("listingsAndReviews");
            moviesCollection = databaseSampleMFLIX.GetCollection<BsonDocument>("movies");

            threadName = tname;
        }

        public void findRandomData()
        {
            // Search 1 00 000 Times
            for(var i=0; i < 100000; i++)
            {
                var filter = new BsonDocument
                {
                    { "room_type", "Entire home/apt" }
                };

                var data = airbnbCollection.Find<BsonDocument>(filter).ToList();
                Console.WriteLine("Found " + data.Count + " Documents in the randomData on thread " + threadName);
            }
        }

        public void aggregateRandomDataForTests()
        {
            //var data = moviesCollection.Aggregate<BsonDocument>(    );
        }

        public void CreateData()
        {
            // Run the Inserts 100 times
            for(var counter = 0; counter < 100; counter++)
            {
                List<BsonDocument> inserts = new List<BsonDocument>();

                // insert it way too many times;
                for (var i = 0; i < 100000; i++)
                {
                    var document = new BsonDocument
                    {
                        { "student_id", i },
                        { "scores", new BsonArray
                            {
                                new BsonDocument{ {"type", "exam"}, {"score", 88.12334193287023 } },
                                new BsonDocument{ {"type", "quiz"}, {"score", 74.92381029342834 } },
                                new BsonDocument{ {"type", "homework"}, {"score", 89.97929384290324 } },
                                new BsonDocument{ {"type", "homework"}, {"score", 82.12931030513218 } }
                            }
                        },
                        { "class_id", 480},
                        { "_id", ObjectId.GenerateNewId() }
                    };

                    inserts.Add(document);
                }


                baseCollection.InsertMany(inserts);

                // Log Current Iteration
                Console.WriteLine("Current Iteration " + counter + " Inserted 100000 Documents from thread " + threadName);
            }            
        }


        public void Unleash()
        {
            CreateData();
            findRandomData();
        }
    }

   
}






