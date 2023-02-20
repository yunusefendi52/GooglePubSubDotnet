using Google.Api.Gax;
using Google.Cloud.PubSub.V1;
using static Google.Cloud.PubSub.V1.PublisherClient;

Console.WriteLine("Starting subscriber...");

var projectId = "project-app-d1471";
// var topicId = "logging-api";
var subscriptionId = "logging-api-sub";

// var subscriberService = await new SubscriberServiceApiClientBuilder
// {
//     EmulatorDetection = EmulatorDetection.EmulatorOrProduction
// }.BuildAsync();

var subscriptionName = new SubscriptionName(projectId, subscriptionId);
// var topicName = new TopicName(projectId, topicId);

// if (subscriberService.GetSubscription(subscriptionName) == null)
// {
//     await subscriberService.CreateSubscriptionAsync(subscriptionName,
//         topicName, pushConfig: null, ackDeadlineSeconds: 60);
// }

var pullMode = false;
if (pullMode)
{
    var subscriber = await SubscriberServiceApiClient.CreateAsync();
    var response = await subscriber.PullAsync(subscriptionName, 2);
    foreach (ReceivedMessage received in response.ReceivedMessages)
    {
        PubsubMessage msg = received.Message;
        Console.WriteLine($"Received message {msg.MessageId} published at {msg.PublishTime.ToDateTime()}");
        Console.WriteLine($"Text: '{msg.Data.ToStringUtf8()}'");
    }

    await subscriber.AcknowledgeAsync(subscriptionName, response.ReceivedMessages.Select(m => m.AckId));
}
else
{
    await SetupSubscriber();      
}

async Task SetupSubscriber()
{
    while (true)
    {
        try
        {
            var subscriber = await new SubscriberClientBuilder
            {
                SubscriptionName = subscriptionName,
                Settings = new SubscriberClient.Settings
                {
                    // 10 = limit concurrenctly
                    FlowControlSettings = new FlowControlSettings(10, null)
                },
            }.BuildAsync();

            Console.WriteLine("Waiting some messages...");

            // This will throw error, use while for simpler case
            await subscriber.StartAsync((msg, token) =>
            {
                Console.WriteLine($"Received message {msg.MessageId} published at {msg.PublishTime.ToDateTime()}");
                Console.WriteLine($"Text: '{msg.Data.ToStringUtf8()}'");

                return Task.FromResult(SubscriberClient.Reply.Ack);
            });
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error waiting messages {ex}, will retry in 1s");
            await Task.Delay(1000);
        }
    }
}
