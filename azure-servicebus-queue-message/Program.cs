using Microsoft.Azure;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace azure_servicebus_queue_message
{
    class Program
    {
        public static string queueName;
        public static string connectionString;

        static void Main(string[] args)
        {
            queueName = CloudConfigurationManager.GetSetting("Microsoft.ServiceBus.QueueName");
            connectionString = CloudConfigurationManager.GetSetting("Microsoft.ServiceBus.ConnectionString");

            var basicScneario = Activator.CreateInstance(typeof(Basic));

            if (args.Length > 0)
            {
                if (args[0]== "1")
                {
                    ((IBasicQueueConnectionStringSample)basicScneario).Run(queueName, connectionString, WorkMode.SendOnly).GetAwaiter().GetResult();
                }
                else if (args[0]== "2")
                {
                    ((IBasicQueueConnectionStringSample)basicScneario).Run(queueName, connectionString, WorkMode.ReceiveOnly).GetAwaiter().GetResult();
                }
            }
            else
            {
                ((IBasicQueueConnectionStringSample)basicScneario).Run(queueName, connectionString, WorkMode.SendAndReceive).GetAwaiter().GetResult();
            }
            
        }
    }
}
