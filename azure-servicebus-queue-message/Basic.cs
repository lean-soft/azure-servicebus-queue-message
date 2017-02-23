using Microsoft.ServiceBus.Messaging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace azure_servicebus_queue_message
{
    public enum WorkMode
    {
        SendOnly,
        ReceiveOnly,
        SendAndReceive
    }
    interface IBasicQueueConnectionStringSample
    {
        Task Run(string queueName, string connectionString, WorkMode modeOfWork);
    }

    public class Basic: IBasicQueueConnectionStringSample
    {
        QueueClient sendClient;
        QueueClient receiveClient;
        Task sendTask;

        public async Task Run(string queueName, string connectionString, WorkMode modeOfWork)
        {
            Console.WriteLine("Press any key to exit the scenario");

            if (modeOfWork != WorkMode.SendOnly)
            {
                this.receiveClient = QueueClient.CreateFromConnectionString(connectionString, queueName, ReceiveMode.PeekLock);
                this.InitializeReceiver();
            }
            
            if (modeOfWork != WorkMode.ReceiveOnly)
            {
                this.sendClient = QueueClient.CreateFromConnectionString(connectionString, queueName);
                sendTask = this.SendMessagesAsync();
            }

            Console.ReadKey();

            if (modeOfWork != WorkMode.SendOnly)
            {
                // shut down the receiver, which will stop the OnMessageAsync loop
                await this.receiveClient.CloseAsync();
            }

            if (modeOfWork != WorkMode.ReceiveOnly)
            {
                // wait for send work to complete if required
                await sendTask;
                await this.sendClient.CloseAsync();
            }
        }

        async Task SendMessagesAsync()
        {
            dynamic data = new[]
            {
                new {name = "Einstein", firstName = "Albert"},
                new {name = "Heisenberg", firstName = "Werner"},
                new {name = "Curie", firstName = "Marie"},
                new {name = "Hawking", firstName = "Steven"},
                new {name = "Newton", firstName = "Isaac"},
                new {name = "Bohr", firstName = "Niels"},
                new {name = "Faraday", firstName = "Michael"},
                new {name = "Galilei", firstName = "Galileo"},
                new {name = "Kepler", firstName = "Johannes"},
                new {name = "Kopernikus", firstName = "Nikolaus"}
            };


            for (int i = 0; i < data.Length; i++)
            {
                var message = new BrokeredMessage(new MemoryStream(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(data[i]))))
                {
                    ContentType = "application/json",
                    Label = "Scientist",
                    MessageId = i.ToString(),
                    TimeToLive = TimeSpan.FromMinutes(2)
                };

                await this.sendClient.SendAsync(message);
                lock (Console.Out)
                {
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Console.WriteLine("Message sent: Id = {0}", message.MessageId);
                    Console.ResetColor();
                }
            }
        }

        void InitializeReceiver()
        {
            // register the OnMessageAsync callback
            this.receiveClient.OnMessageAsync(
                async message =>
                {
                    if (message.Label != null &&
                        message.ContentType != null &&
                        message.Label.Equals("Scientist", StringComparison.InvariantCultureIgnoreCase) &&
                        message.ContentType.Equals("application/json", StringComparison.InvariantCultureIgnoreCase))
                    {
                        var body = message.GetBody<Stream>();

                        dynamic scientist = JsonConvert.DeserializeObject(new StreamReader(body, true).ReadToEnd());

                        lock (Console.Out)
                        {
                            Console.ForegroundColor = ConsoleColor.Cyan;
                            Console.WriteLine(
                                "\t\t\t\tMessage received: \n\t\t\t\t\t\tMessageId = {0}, \n\t\t\t\t\t\tSequenceNumber = {1}, \n\t\t\t\t\t\tEnqueuedTimeUtc = {2}," +
                                "\n\t\t\t\t\t\tExpiresAtUtc = {5}, \n\t\t\t\t\t\tContentType = \"{3}\", \n\t\t\t\t\t\tSize = {4},  \n\t\t\t\t\t\tContent: [ firstName = {6}, name = {7} ]",
                                message.MessageId,
                                message.SequenceNumber,
                                message.EnqueuedTimeUtc,
                                message.ContentType,
                                message.Size,
                                message.ExpiresAtUtc,
                                scientist.firstName,
                                scientist.name);
                            Console.ResetColor();
                        }
                    }
                    await message.CompleteAsync();
                },
                new OnMessageOptions { AutoComplete = false, MaxConcurrentCalls = 1 });
        }

    }
}
