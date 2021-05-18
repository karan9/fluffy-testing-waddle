using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using Microsoft.AspNetCore.Mvc;
using MongoTestHarness.Services;

namespace MongoTestHarness.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ValuesController : ControllerBase
    {


        private readonly MainService service;


        public ValuesController(MainService s)
        {
            service = s;
        }

        // GET api/values
        [HttpGet]
        public ActionResult<IEnumerable<string>> Get()
        {
            return new string[] { "value1", "value2" };
        }

        // GET api/values/5
        [HttpGet("{id}")]
        public ActionResult<string> Get(int id)
        {
            return "value";
        }

        public static void startThread()
        {
            MainService main = new MainService(Thread.CurrentThread.Name);
            Console.WriteLine("Thread Started ", Thread.CurrentThread.Name);
            main.Unleash();
        }


        public static void startIngestion()
        {
            MainService main = new MainService(Thread.CurrentThread.Name);
            main.CreateData();
        }

        public static void startQuerying()
        {
            MainService main = new MainService(Thread.CurrentThread.Name);
            main.findRandomData();
        }

        // POST api/values
        [HttpPost]
        public void Post()
        {
            try
            {
                ThreadStart ingestionRef = new ThreadStart(startIngestion);
                ThreadStart ingestionRef2 = new ThreadStart(startIngestion);
                ThreadStart ingestionRef3 = new ThreadStart(startIngestion);
                ThreadStart queryRef = new ThreadStart(startQuerying);
                ThreadStart queryRef2 = new ThreadStart(startQuerying);
                ThreadStart queryRef3 = new ThreadStart(startQuerying);

                Thread ingestionThread = new Thread(ingestionRef);
                Thread ingestionThread2 = new Thread(ingestionRef2);
                Thread ingestionThread3 = new Thread(ingestionRef3);

                Thread queryThread = new Thread(queryRef);
                Thread queryThread2 = new Thread(queryRef2);
                Thread queryThread3 = new Thread(queryRef3);

                ingestionThread.Name = "Ingestion Thread";
                ingestionThread2.Name = "Ingestion Thread 2";
                ingestionThread3.Name = "Ingestion Thread 3";

                queryThread.Name = "Query Thread";
                queryThread2.Name = "Query Thread 2";
                queryThread3.Name = "Query Thread 3";

                ingestionThread.Start();
                ingestionThread2.Start();
                ingestionThread3.Start();

                queryThread.Start();
                queryThread2.Start();
                queryThread3.Start();
            } catch (Exception e)
            {
                Console.Write(e);
            } finally
            {
                Console.Write("Finally Reached");
            }
        }

        // PUT api/values/5
        [HttpPut("{id}")]
        public void Put(int id, [FromBody] string value)
        {
        }

        // DELETE api/values/5
        [HttpDelete("{id}")]
        public void Delete(int id)
        {
        }
    }
}
